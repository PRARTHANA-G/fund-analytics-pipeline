# ingestion/extract_exchange_rates.py
"""Extract ECB exchange rate data and load to BigQuery."""
 
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
 
ECB_URL = (
    "https://data-api.ecb.europa.eu/service/data/"
    "EXR/D.USD+GBP+JPY+CHF.EUR.SP00.A"
    "?format=csvdata"
)
 
def extract_exchange_rates():
    """Pull exchange rates from ECB API."""
    print("Extracting ECB exchange rates...")
    
    resp = requests.get(ECB_URL)
    resp.raise_for_status()
    
    # Write to temp file and read as CSV
    with open("/tmp/ecb_rates.csv", "w") as f:
        f.write(resp.text)
    
    df = pd.read_csv("/tmp/ecb_rates.csv")
    
    # Clean up: keep only relevant columns
    df = df[["TIME_PERIOD", "CURRENCY", "OBS_VALUE"]].copy()
    df.columns = ["date", "currency", "rate"]
    df["date"] = pd.to_datetime(df["date"])
    df["ingested_at"] = datetime.utcnow()
    
    print(f"Extracted {len(df)} exchange rate records")
    return df
 
def load_to_bigquery(df, table_id):
    """Append to BigQuery raw table."""
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()
    print(f"Loaded {len(df)} rows to {table_id}")
 
 
if __name__ == "__main__":
    df = extract_exchange_rates()
    load_to_bigquery(
        df,
        "fund-analytics-pipeline.raw.exchange_rates"
    )
    print("Done!")
