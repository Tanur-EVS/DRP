df = pd.read_excel(xls, sheet_name=sheet)
        df["Service"] = sheet
        all_data.append(df)
    combined = pd.concat(all_data)

    # Employee totals
    employee_totals = combined.groupby(["Employee Name","DRP ID"])["Points"].sum().reset_index()

    # Leaderboard
    leaderboard = employee_totals.sort_values("Points", ascending=False).head(10)

    # WoW Change
    client = bigquery.Client()
    last_week = client.query("SELECT * FROM employee_training.last_week_totals").to_dataframe()
    wow = employee_totals.merge(last_week, on="DRP ID", suffixes=("", "_last"))
    wow["WoW Change"] = wow["Points"] - wow["Points_last"]

    # Write to BigQuery
    client.load_table_from_dataframe(employee_totals, "employee_training.employee_totals").result()
    client.load_table_from_dataframe(leaderboard, "employee_training.leaderboard").result()

Failed. Details: The user-provided container failed to start and listen on the port defined provided by the PORT=8080 environment variable within the allocated timeout. This can happen when the container port is misconfigured or if the timeout is too short. The health check timeout can be extended. Logs for this revision might contain more information. Logs URL: Open Cloud Logging  For more troubleshooting guidance, see https://cloud.google.com/run/docs/troubleshooting#container-failed-to-start 
    client.load_table_from_dataframe(wow, "employee_training.wow_change").result()

    # Update last_week_totals
    client.load_table_from_dataframe(employee_totals, "employee_training.last_week_totals", job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
