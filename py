import functions_framework
from google.cloud import storage, bigquery
import pandas as pd
import io, re, json

PROJECT_ID = "neat-striker-447409-t5"
DATASET    = "Training_points_tracking"

@functions_framework.cloud_event
def process_excel(cloud_event):
    data        = cloud_event.data
    bucket_name = data["bucket"]
    file_name   = data["name"]

    if not file_name.endswith(".xlsx"):
        print(f"Skipping non-Excel file: {file_name}")
        return

    # Extract date from filename
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

    if not blob.exists():
        print(f"File no longer exists in GCS, skipping: {file_name}")
        return

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

        id_cols       = ["name", "drp_id", "email", "total_dri"]
        id_cols       = [c for c in id_cols if c in df.columns]
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
    print(f"Total rows to load: {len(raw_df)}")

    # Load to BigQuery via JSON — no pyarrow needed
    table_ref = f"{PROJECT_ID}.{DATASET}.raw_training_data"
    records   = raw_df.where(pd.notnull(raw_df), None).to_dict(orient="records")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    job = bq_client.load_table_from_json(
        records, table_ref, job_config=job_config
    )
    job.result()
    print(f"Successfully loaded {len(records)} rows into {table_ref}")
