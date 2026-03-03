import functions_framework
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime
import io
import os

PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET = "drp_leaderboard"

RAW_TABLE = f"{PROJECT_ID}.{DATASET}.raw_drp_data"
TOTALS_TABLE = f"{PROJECT_ID}.{DATASET}.weekly_totals"
WOW_TABLE = f"{PROJECT_ID}.{DATASET}.leaderboard_wow"

ARCHIVE_BUCKET = "drp-archive"


@functions_framework.cloud_event
def hello_gcs(cloud_event):

    bucket_name = cloud_event.data["bucket"]
    file_name = cloud_event.data["name"]

    print(f"Processing file: {file_name}")

    storage_client = storage.Client()
    bq_client = bigquery.Client()

    # -----------------------------
    # STEP 1 — DOWNLOAD FILE
    # -----------------------------
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    file_bytes = blob.download_as_bytes()

    # -----------------------------
    # STEP 2 — READ EXCEL
    # -----------------------------
    df = pd.read_excel(io.BytesIO(file_bytes))

    df.columns = df.columns.str.strip()

    expected_columns = [
        "DRP ID",
        "Employee Name",
        "Service",
        "Points",
        "Date"
    ]

    for col in expected_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df = df.rename(columns={
        "DRP ID": "drp_id",
        "Employee Name": "employee_name",
        "Service": "service",
        "Points": "points",
        "Date": "activity_date"
    })

    df["activity_date"] = pd.to_datetime(df["activity_date"]).dt.date
    df["points"] = df["points"].astype(int)

    # One file per week guaranteed
    week_start = df["activity_date"].min()

    df["week_start_date"] = week_start
    df["processed_timestamp"] = datetime.utcnow()

    # -----------------------------
    # STEP 3 — LOAD RAW TO BQ
    # -----------------------------
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    load_job = bq_client.load_table_from_dataframe(
        df,
        RAW_TABLE,
        job_config=job_config
    )

    load_job.result()
    print("Raw data loaded.")

    # -----------------------------
    # STEP 4 — UPDATE WEEKLY TOTALS
    # -----------------------------
    totals_query = f"""
    INSERT INTO `{TOTALS_TABLE}`
    SELECT
      drp_id,
      employee_name,
      week_start_date,
      SUM(points) AS total_points
    FROM `{RAW_TABLE}`
    WHERE week_start_date = (
        SELECT MAX(week_start_date) FROM `{RAW_TABLE}`
    )
    GROUP BY drp_id, employee_name, week_start_date
    """

    bq_client.query(totals_query).result()
    print("Weekly totals updated.")

    # -----------------------------
    # STEP 5 — WoW + RANKING
    # -----------------------------
    wow_query = f"""
    CREATE OR REPLACE TABLE `{WOW_TABLE}` AS
    WITH current_week AS (
      SELECT *
      FROM `{TOTALS_TABLE}`
      WHERE week_start_date = (
          SELECT MAX(week_start_date)
          FROM `{TOTALS_TABLE}`
      )
    ),
    previous_week AS (
      SELECT *
      FROM `{TOTALS_TABLE}`
      WHERE week_start_date = (
        SELECT MAX(week_start_date)
        FROM `{TOTALS_TABLE}`
        WHERE week_start_date < (
          SELECT MAX(week_start_date)
          FROM `{TOTALS_TABLE}`
        )
      )
    )

    SELECT
      c.drp_id,
      c.employee_name,
      c.week_start_date,
      c.total_points,
      IFNULL(p.total_points, 0) AS previous_week_points,
      c.total_points - IFNULL(p.total_points, 0) AS wow_change,
      RANK() OVER (ORDER BY c.total_points DESC) AS rank
    FROM current_week c
    LEFT JOIN previous_week p
    ON c.drp_id = p.drp_id
    """

    bq_client.query(wow_query).result()
    print("WoW table rebuilt.")

    # -----------------------------
    # STEP 6 — ARCHIVE FILE
    # -----------------------------
    archive_bucket = storage_client.bucket(ARCHIVE_BUCKET)

    bucket.copy_blob(blob, archive_bucket, file_name)
    blob.delete()

    print("File archived successfully.")
