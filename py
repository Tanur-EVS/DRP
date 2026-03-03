import functions_framework
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import io
from datetime import datetime

@functions_framework.cloud_event
def drp_transform(cloud_event):
    
    bucket_name = cloud_event.data["bucket"]
    file_name = cloud_event.data["name"]

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    file_content = blob.download_as_bytes()
    
    # Read Excel
    xls = pd.ExcelFile(io.BytesIO(file_content))
    
    # Combine all service sheets
    df_list = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet)
        df_list.append(df)
    
    full_df = pd.concat(df_list)
    
    # Calculate total points per employee
    grouped = (
        full_df
        .groupby(["DRP_ID", "Employee_Name"])
        ["Points"]
        .sum()
        .reset_index()
    )

    grouped.rename(columns={
        "DRP_ID": "employee_id",
        "Employee_Name": "employee_name",
        "Points": "total_points"
    }, inplace=True)

    # Add week date (hardcoded example - improve later)
    week_date = datetime.today().date()
    grouped["week_start_date"] = week_date
    grouped["processed_timestamp"] = datetime.utcnow()
    grouped["wow_change"] = 0  # placeholder

    client = bigquery.Client()
    table_id = "your-project-id.drp_leaderboard.weekly_totals"

    job = client.load_table_from_dataframe(grouped, table_id)
    job.result()

    print("Upload complete.")
