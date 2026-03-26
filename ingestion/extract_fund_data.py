import yfinance as yf
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta

FUNDS = ["SPY", "IVV", "EEM", "AGG", "VGK"]

def extract_fund_prices(lookback_days=90):
    end = datetime.today()
    start = end - timedelta(days=lookback_days)
    
    all_data = []
    for ticker in FUNDS:
        print(f"Extracting {ticker}...")
        df = yf.download(ticker, start=start, end=end)
        df.columns = df.columns.get_level_values(0)
        df = df.reset_index()
        df["ticker"] = ticker
        df["ingested_at"] = datetime.utcnow()
        all_data.append(df)
    
    combined = pd.concat(all_data, ignore_index=True)
    print(f"Extracted {len(combined)} total rows")
    return combined

def load_to_bigquery(df, table_id):
    client = bigquery.Client(project="fund-analytics-pipeline")
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ],
    )
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()
    table = client.get_table(table_id)
    print(f"Loaded {len(df)} rows to {table_id}")
    print(f"Table now has {table.num_rows} total rows")

if __name__ == "__main__":
    df = extract_fund_prices(lookback_days=90)
    load_to_bigquery(df, "fund-analytics-pipeline.raw.fund_prices")
    print("Done!")
