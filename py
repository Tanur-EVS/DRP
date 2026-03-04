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


ERROR 2026-03-04T06:57:39.066059Z [severity: ERROR] Traceback (most recent call last): File "/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py", line 1511, in wsgi_app response = self.full_dispatch_request() ^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py", line 919, in full_dispatch_request rv = self.handle_user_exception(e) ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py", line 917, in full_dispatch_request rv = self.dispatch_request() ^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py", line 902, in dispatch_request return self.ensure_sync(self.view_functions[rule.endpoint])(**view_args) # type: ignore[no-any-return] ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/execution_id.py", line 157, in wrapper result = view_function(*args, **kwargs) ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/__init__.py", line 188, in view_func function(event) File "/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/__init__.py", line 81, in wrapper return func(*args, **kwargs) ^^^^^^^^^^^^^^^^^^^^^ File "/workspace/main.py", line 52, in process_excel melted = df.melt( ^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/pandas/core/frame.py", line 9969, in melt return melt( ^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/pandas/core/reshape/melt.py", line 74, in melt raise KeyError( KeyError: "The following id_vars or value_vars are not present in the DataFrame: ['employee_name']"
  {
    "textPayload": "Traceback (most recent call last):\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py\", line 1511, in wsgi_app\n    response = self.full_dispatch_request()\n               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py\", line 919, in full_dispatch_request\n    rv = self.handle_user_exception(e)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py\", line 917, in full_dispatch_request\n    rv = self.dispatch_request()\n         ^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py\", line 902, in dispatch_request\n    return self.ensure_sync(self.view_functions[rule.endpoint])(**view_args)  # type: ignore[no-any-return]\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/execution_id.py\", line 157, in wrapper\n    result = view_function(*args, **kwargs)\n             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/__init__.py\", line 188, in view_func\n    function(event)\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/__init__.py\", line 81, in wrapper\n    return func(*args, **kwargs)\n           ^^^^^^^^^^^^^^^^^^^^^\n  File \"/workspace/main.py\", line 52, in process_excel\n    melted = df.melt(\n             ^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/pandas/core/frame.py\", line 9969, in melt\n    return melt(\n           ^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/pandas/core/reshape/melt.py\", line 74, in melt\n    raise KeyError(\nKeyError: \"The following id_vars or value_vars are not present in the DataFrame: ['employee_name']\"",
    "insertId": "69a7d7e30001020baa323ab9",
    "resource": {
      "type": "cloud_run_revision",
      "labels": {
        "revision_name": "drp-score-testing-00005-n8g",
        "service_name": "drp-score-testing",
        "configuration_name": "drp-score-testing",
        "location": "us-central1",
        "project_id": "neat-striker-447409-t5"
      }
    },
    "timestamp": "2026-03-04T06:57:39.066059Z",
    "severity": "ERROR",
    "labels": {
      "instanceId": "00da6cd2c422ccbb8c7a0f0f698e8ee2de6e3f408a960718188d9a13aaf577a84c4d56b13709ab2670d5937fe61f9568496503e92568ad0793ad90512cba27c20fa1879a186579ac9fa057ddaa7b9b",
      "run.googleapis.com/base_image_versions": "us-docker.pkg.dev/serverless-runtimes/google-22/runtimes/python311:python311_20260215_3_11_14_RC00"
    },
    "logName": "projects/neat-striker-447409-t5/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-03-04T06:57:39.072932434Z",
    "errorGroups": [
      {
        "id": "CMe7zb-144Xn8QE"
      }
    ]
  }


httpRequest: {10}
insertId: "69a7d80600011960d05704e6"
labels: {3}
logName: "projects/neat-striker-447409-t5/logs/run.googleapis.com%2Frequests"
receiveTimestamp: "2026-03-04T06:58:14.079237257Z"
resource: {2}
severity: "ERROR"
spanId: "8b545b938b53b8d5"
timestamp: "2026-03-04T06:58:13.802038Z"
trace: "projects/neat-striker-447409-t5/traces/aea416811fa3419addf949ee97a380a6"
traceSampled: true
}

