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