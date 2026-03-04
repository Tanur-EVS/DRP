DEFAULT 2026-03-04T07:23:35.698873Z Processing file: Summary of DRP scores of profiles 2026-02-24.xlsx | Week date: 2026-02-24
DEFAULT 2026-03-04T07:23:36.477998Z Total rows parsed: 27200
DEFAULT 2026-03-04T07:23:36.479790Z [2026-03-04 07:23:36,477] ERROR in app: Exception on / [POST]
ERROR 2026-03-04T07:23:36.479847Z [severity: ERROR] Traceback (most recent call last): File "/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py", line 1511, in wsgi_app response = self.full_dispatch_request() ^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py", line 919, in full_dispatch_request rv = self.handle_user_exception(e) ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py", line 917, in full_dispatch_request rv = self.dispatch_request() ^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py", line 902, in dispatch_request return self.ensure_sync(self.view_functions[rule.endpoint])(**view_args) # type: ignore[no-any-return] ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/execution_id.py", line 157, in wrapper result = view_function(*args, **kwargs) ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/__init__.py", line 188, in view_func function(event) File "/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/__init__.py", line 81, in wrapper return func(*args, **kwargs) ^^^^^^^^^^^^^^^^^^^^^ File "/workspace/main.py", line 74, in process_excel job = bq_client.load_table_from_dataframe( ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/google/cloud/bigquery/client.py", line 2831, in load_table_from_dataframe raise ValueError("This method requires pyarrow to be installed") ValueError: This method requires pyarrow to be installed
  {
    "textPayload": "Traceback (most recent call last):\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py\", line 1511, in wsgi_app\n    response = self.full_dispatch_request()\n               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py\", line 919, in full_dispatch_request\n    rv = self.handle_user_exception(e)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py\", line 917, in full_dispatch_request\n    rv = self.dispatch_request()\n         ^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py\", line 902, in dispatch_request\n    return self.ensure_sync(self.view_functions[rule.endpoint])(**view_args)  # type: ignore[no-any-return]\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/execution_id.py\", line 157, in wrapper\n    result = view_function(*args, **kwargs)\n             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/__init__.py\", line 188, in view_func\n    function(event)\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/__init__.py\", line 81, in wrapper\n    return func(*args, **kwargs)\n           ^^^^^^^^^^^^^^^^^^^^^\n  File \"/workspace/main.py\", line 74, in process_excel\n    job = bq_client.load_table_from_dataframe(\n          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/google/cloud/bigquery/client.py\", line 2831, in load_table_from_dataframe\n    raise ValueError(\"This method requires pyarrow to be installed\")\nValueError: This method requires pyarrow to be installed",
    "insertId": "69a7ddf800075267e08038de",
    "resource": {
      "type": "cloud_run_revision",
      "labels": {
        "service_name": "drp-score-testing",
        "location": "us-central1",
        "configuration_name": "drp-score-testing",
        "project_id": "neat-striker-447409-t5",
        "revision_name": "drp-score-testing-00007-46g"
      }
    },
    "timestamp": "2026-03-04T07:23:36.479847Z",
    "severity": "ERROR",
    "labels": {
      "instanceId": "00da6cd2c469b2f64375e1f9ad72a0c08beec12466f0977dd672cedb80d0f0a44a74f22f3d03853f8341cf3b84ca61f4ec9464d8e592963a3b0456062f10bd050d4217db4d0b6ed78dd489a24b51e0",
      "run.googleapis.com/base_image_versions": "us-docker.pkg.dev/serverless-runtimes/google-22/runtimes/python311:python311_20260215_3_11_14_RC00"
    },
    "logName": "projects/neat-striker-447409-t5/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-03-04T07:23:36.485474640Z",
    "errorGroups": [
      {
        "id": "COTsjYOl-9T7Lw"
      }
    ]
  }
ERROR 2026-03-04T07:24:33.504715Z [severity: ERROR] [httpRequest.requestMethod: POST] [httpRequest.status: 500] [httpRequest.responseSize: 331 B] [httpRequest.latency: 763 ms] [httpRequest.userAgent: APIs-Google; (+https://developers.google.com/webmasters/APIs-Google.html)] https://drp-score-testing-5otrx5s65a-uc.a.run.app/?__GCP_CloudEventsMode=GCS_NOTIFICATION
  {
    "insertId": "69a7de3200045aac1894e73f",
    "httpRequest": {
      "requestMethod": "POST",
      "requestUrl": "https://drp-score-testing-5otrx5s65a-uc.a.run.app/?__GCP_CloudEventsMode=GCS_NOTIFICATION",
      "requestSize": "3296",
      "status": 500,
      "responseSize": "331",
      "userAgent": "APIs-Google; (+https://developers.google.com/webmasters/APIs-Google.html)",
      "remoteIp": "74.125.212.3",
      "serverIp": "34.143.77.2",
      "latency": "0.763984036s",
      "protocol": "HTTP/1.1"
    },
    "resource": {
      "type": "cloud_run_revision",
      "labels": {
        "configuration_name": "drp-score-testing",
        "revision_name": "drp-score-testing-00007-46g",
        "project_id": "neat-striker-447409-t5",
        "location": "us-central1",
        "service_name": "drp-score-testing"
      }
    },
    "timestamp": "2026-03-04T07:24:33.504715Z",
    "severity": "ERROR",
    "labels": {
      "instanceId": "00da6cd2c469b2f64375e1f9ad72a0c08beec12466f0977dd672cedb80d0f0a44a74f22f3d03853f8341cf3b84ca61f4ec9464d8e592963a3b0456062f10bd050d4217db4d0b6ed78dd489a24b51e0",
      "run.googleapis.com/cloud_event_id": "18552255291253944",
      "run.googleapis.com/cloud_event_source": "//storage.googleapis.com/projects/_/buckets/drp_weekly_score"
    },
    "logName": "projects/neat-striker-447409-t5/logs/run.googleapis.com%2Frequests",
    "trace": "projects/neat-striker-447409-t5/traces/28b3d1d00ee745e5db477b32a938818e",
    "receiveTimestamp": "2026-03-04T07:24:34.290682200Z",
    "spanId": "543656dec8df97eb",
    "traceSampled": true
  }
