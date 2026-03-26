from google.cloud import bigquery

client = bigquery.Client(project="fund-analytics-pipeline")

datasets = ["raw", "staging", "marts"]
for ds_name in datasets:
    dataset_id = f"fund-analytics-pipeline.{ds_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "EU"
    client.create_dataset(dataset, exists_ok=True)
    print(f"Created dataset: {ds_name}")

print("Done! All 3 datasets created.")
