import functions_framework
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime
import io
import os

PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET = "drp_leaderboard"

TOTALS_TABLE = f"{PROJECT_ID}.{DATASET}.weekly_totals"
WOW_TABLE = f"{PROJECT_ID}.{DATASET}.leaderboard_wow"

ARCHIVE_BUCKET = "drp-archive"


@functions_framework.cloud_event
def hello_gcs(cloud_event):

    bucket_name = cloud_event.data["bucket"]
    file_name = cloud_event.data["name"]

    storage_client = storage.Client()
    bq_client = bigquery.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_bytes = blob.download_as_bytes()

    # -----------------------------
    # STEP 1 — READ ALL SHEETS
    # -----------------------------
    excel_file = pd.ExcelFile(io.BytesIO(file_bytes))

    all_data = []

    for sheet in excel_file.sheet_names:
        df = excel_file.parse(sheet)
        df.columns = df.columns.str.strip()

        required_cols = ["Name", "DRP ID", "Total DRI"]

        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"{col} missing in sheet {sheet}")

        temp = df[["Name", "DRP ID", "Total DRI"]].copy()
        temp.columns = ["employee_name", "drp_id", "points"]

        all_data.append(temp)

    # Combine all services
    combined_df = pd.concat(all_data)

    # -----------------------------
    # STEP 2 — SUM ACROSS SERVICES
    # -----------------------------
    final_df = (
        combined_df
        .groupby(["drp_id", "employee_name"], as_index=False)
        .sum()
    )

    final_df.rename(columns={"points": "total_points"}, inplace=True)

    # Assign upload date as week_date
    week_date = datetime.utcnow().date()

    final_df["week_date"] = week_date
    final_df["processed_timestamp"] = datetime.utcnow()

    # -----------------------------
    # STEP 3 — LOAD TO BIGQUERY
    # -----------------------------
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    bq_client.load_table_from_dataframe(
        final_df,
        TOTALS_TABLE,
        job_config=job_config
    ).result()

    # -----------------------------
    # STEP 4 — BUILD WoW TABLE
    # -----------------------------
    wow_query = f"""
    CREATE OR REPLACE TABLE `{WOW_TABLE}` AS
    WITH current_week AS (
      SELECT *
      FROM `{TOTALS_TABLE}`
      WHERE week_date = (
          SELECT MAX(week_date) FROM `{TOTALS_TABLE}`
      )
    ),

    previous_week AS (
      SELECT *
      FROM `{TOTALS_TABLE}`
      WHERE week_date = (
        SELECT MAX(week_date)
        FROM `{TOTALS_TABLE}`
        WHERE week_date < (
          SELECT MAX(week_date)
          FROM `{TOTALS_TABLE}`
        )
      )
    )

    SELECT
      c.drp_id,
      c.employee_name,
      c.week_date,
      c.total_points,
      IFNULL(p.total_points, 0) AS previous_week_points,
      c.total_points - IFNULL(p.total_points, 0) AS wow_change,
      RANK() OVER (ORDER BY c.total_points DESC) AS rank
    FROM current_week c
    LEFT JOIN previous_week p
    ON c.drp_id = p.drp_id
    """

    bq_client.query(wow_query).result()

    # -----------------------------
    # STEP 5 — ARCHIVE FILE
    # -----------------------------
    archive_bucket = storage_client.bucket(ARCHIVE_BUCKET)
    bucket.copy_blob(blob, archive_bucket, file_name)
    blob.delete()

    print("Processing completed successfully.")
