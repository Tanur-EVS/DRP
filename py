import functions_framework
from google.cloud import storage, bigquery
import pandas as pd
import io, re

PROJECT_ID = "your-project-id"   # <-- update
DATASET    = "training_data"

@functions_framework.cloud_event
def process_excel(cloud_event):
    data        = cloud_event.data
    bucket_name = data["bucket"]
    file_name   = data["name"]

    # Only process Excel files
    if not file_name.endswith(".xlsx"):
        print(f"Skipping non-Excel file: {file_name}")
        return

    # Extract date from filename e.g. training_2025-03-04.xlsx
    match = re.search(r"(\d{4}-\d{2}-\d{2})", file_name)
    if not match:
        print(f"No YYYY-MM-DD date found in filename: {file_name}. Aborting.")
        return
    week_date = match.group(1)
    print(f"Processing file: {file_name} | Week date: {week_date}")

    # Download Excel from GCS
    gcs_client  = storage.Client()
    bq_client   = bigquery.Client(project=PROJECT_ID)
    bucket      = gcs_client.bucket(bucket_name)
    blob        = bucket.blob(file_name)
    excel_bytes = blob.download_as_bytes()
    xl          = pd.ExcelFile(io.BytesIO(excel_bytes))

    SKIP_TABS = {"Total Points", "Leaderboard", "WoW Change", "Last Week Snapshot"}

    all_raw = []
    for sheet in xl.sheet_names:
        if sheet in SKIP_TABS:
            continue

        df = xl.parse(sheet)
        df.columns = (df.columns
                        .str.strip()
                        .str.lower()
                        .str.replace(" ", "_", regex=False))

        id_cols       = ["employee_name", "drp_id"]
        training_cols = [c for c in df.columns if c not in id_cols]

        melted = df.melt(
            id_vars=id_cols,
            value_vars=training_cols,
            var_name="training_column",
            value_name="points"
        )
        melted["service_name"] = sheet
        melted["week_date"]    = week_date
        melted["points"]       = pd.to_numeric(
                                     melted["points"], errors="coerce"
                                 ).fillna(0)
        all_raw.append(melted)

    raw_df = pd.concat(all_raw, ignore_index=True)
    print(f"Total rows parsed: {len(raw_df)}")

    # Load to BigQuery — APPEND so history is preserved
    table_ref  = f"{PROJECT_ID}.{DATASET}.raw_training_data"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )
    job = bq_client.load_table_from_dataframe(
        raw_df, table_ref, job_config=job_config
    )
    job.result()
    print(f"Successfully loaded {len(raw_df)} rows into {table_ref}")
```

**`requirements.txt`**
```
functions-framework==3.*
google-cloud-storage==2.*
google-cloud-bigquery==3.*
google-cloud-bigquery-storage==2.*
pandas==2.*
openpyxl==3.*
pyarrow==14.*
db-dtypes==1.*




{
httpRequest: {9}
insertId: "69a7d57500056e7058186f5a"
logName: "projects/neat-striker-447409-t5/logs/run.googleapis.com%2Frequests"
receiveTimestamp: "2026-03-04T06:47:17.361454313Z"
resource: {2}
severity: "WARNING"
spanId: "b6b12e10702da0ad"
textPayload: "The request was not authenticated. Either allow unauthenticated invocations or set the proper Authorization header. Read more at https://cloud.google.com/run/docs/securing/authenticating Additional troubleshooting documentation can be found at: https://cloud.google.com/run/docs/troubleshooting#unauthorized-client"
timestamp: "2026-03-04T06:47:17.339168Z"
trace: "projects/neat-striker-447409-t5/traces/6de1b146dc32bbff91577d869aaa7cee"
traceSampled: true
}
