# Fund Analytics Pipeline
 
End-to-end data pipeline that ingests investment fund 
data, loads it into BigQuery, transforms it with dbt, 
and orchestrates everything with Airflow in Docker.
 
## Architecture
[Insert architecture diagram screenshot]
 
## Tech Stack
- **Ingestion**: Python, yfinance, ECB API
- **Warehouse**: Google BigQuery (raw > staging > marts)
- **Transformation**: dbt (data build tool)
- **Orchestration**: Apache Airflow 2.8
- **Containerization**: Docker & Docker Compose
- **Dashboard**: Streamlit + Plotly
- **Version Control**: Git
 
## Data Sources
- **yfinance**: Daily NAV/price data for 5 ETFs 
  (SPY, IVV, EEM, AGG, VGK)
- **ECB Data Portal**: EUR exchange rates 
  (USD, GBP, JPY, CHF)
 
## Warehouse Architecture
| Layer   | Purpose                        |
|---------|--------------------------------|
| raw     | Exact copy of source, append-only |
| staging | Cleaned, typed, deduplicated   |
| marts   | Business-ready aggregations    |
 
## dbt Model Lineage
[Insert screenshot from dbt docs serve]
 
## Airflow DAG
[Insert screenshot of DAG graph view]
Schedule: Weekdays at 8:00 AM UTC
 
## Quick Start
```bash
git clone https://github.com/PRARTHANA-G/fund-analytics-pipeline.git
cd fund-analytics-pipeline
cp .env.example .env
# Add your GCP credentials to credentials/gcp_key.json
docker-compose up airflow-init
docker-compose up -d
# Airflow UI: http://localhost:8080 (admin/admin)
# Dashboard:  http://localhost:8501
```
 
## Project Structure
[Copy the folder tree from Phase 4]
