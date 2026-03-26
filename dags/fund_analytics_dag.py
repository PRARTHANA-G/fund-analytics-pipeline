# dags/fund_analytics_dag.py
"""
Fund Analytics Pipeline DAG
Runs daily on weekdays at 8:00 AM UTC:
  1. Extract fund prices from yfinance
  2. Extract exchange rates from ECB
  3. Load both to BigQuery raw layer
  4. Run dbt models (staging + marts)
  5. Run dbt tests
"""
 
import sys
sys.path.insert(0, "/opt/airflow")
 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
 
default_args = {
    "owner": "prarthana",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
 
# Import your ingestion functions
from ingestion.extract_fund_data import (
    extract_fund_prices, 
    load_to_bigquery as load_prices
)
from ingestion.extract_exchange_rates import (
    extract_exchange_rates,
    load_to_bigquery as load_rates
)
 
 
def run_fund_extraction(**kwargs):
    df = extract_fund_prices(lookback_days=7)
    load_prices(
        df, 
        "fund-analytics-pipeline.raw.fund_prices"
    )
 
 
def run_rate_extraction(**kwargs):
    df = extract_exchange_rates()
    load_rates(
        df, 
        "fund-analytics-pipeline.raw.exchange_rates"
    )
 
 
with DAG(
    dag_id="fund_analytics_pipeline",
    default_args=default_args,
    description="Daily ETL: APIs > BigQuery > dbt",
    schedule_interval="0 8 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["funds", "analytics", "etl"],
) as dag:
 
    extract_prices = PythonOperator(
        task_id="extract_fund_prices",
        python_callable=run_fund_extraction,
    )
 
    extract_rates = PythonOperator(
        task_id="extract_exchange_rates",
        python_callable=run_rate_extraction,
    )
dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/dbt/fund_analytics && "
            "pip install dbt-bigquery && "
            "dbt run --profiles-dir ."
        ),
    )
 
dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/dbt/fund_analytics && "
            "dbt test --profiles-dir ."
        ),
    )
 
    # Dependencies: extract in parallel, then dbt
    [extract_prices, extract_rates] >> dbt_run >> dbt_test