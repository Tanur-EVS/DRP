ERROR 2026-03-04T07:17:34.871003Z [severity: ERROR] Traceback (most recent call last): File "/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py", line 1511, in wsgi_app response = self.full_dispatch_request() ^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py", line 919, in full_dispatch_request rv = self.handle_user_exception(e) ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py", line 917, in full_dispatch_request rv = self.dispatch_request() ^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py", line 902, in dispatch_request return self.ensure_sync(self.view_functions[rule.endpoint])(**view_args) # type: ignore[no-any-return] ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/execution_id.py", line 157, in wrapper result = view_function(*args, **kwargs) ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/__init__.py", line 188, in view_func function(event) File "/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/__init__.py", line 81, in wrapper return func(*args, **kwargs) ^^^^^^^^^^^^^^^^^^^^^ File "/workspace/main.py", line 52, in process_excel melted = df.melt( ^^^^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/pandas/core/frame.py", line 9969, in melt return melt( ^^^^^ File "/layers/google.python.pip/pip/lib/python3.11/site-packages/pandas/core/reshape/melt.py", line 74, in melt raise KeyError( KeyError: "The following id_vars or value_vars are not present in the DataFrame: ['Name', 'DRP ID']"
  {
    "textPayload": "Traceback (most recent call last):\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py\", line 1511, in wsgi_app\n    response = self.full_dispatch_request()\n               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py\", line 919, in full_dispatch_request\n    rv = self.handle_user_exception(e)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py\", line 917, in full_dispatch_request\n    rv = self.dispatch_request()\n         ^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/flask/app.py\", line 902, in dispatch_request\n    return self.ensure_sync(self.view_functions[rule.endpoint])(**view_args)  # type: ignore[no-any-return]\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/execution_id.py\", line 157, in wrapper\n    result = view_function(*args, **kwargs)\n             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/__init__.py\", line 188, in view_func\n    function(event)\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/functions_framework/__init__.py\", line 81, in wrapper\n    return func(*args, **kwargs)\n           ^^^^^^^^^^^^^^^^^^^^^\n  File \"/workspace/main.py\", line 52, in process_excel\n    melted = df.melt(\n             ^^^^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/pandas/core/frame.py\", line 9969, in melt\n    return melt(\n           ^^^^^\n  File \"/layers/google.python.pip/pip/lib/python3.11/site-packages/pandas/core/reshape/melt.py\", line 74, in melt\n    raise KeyError(\nKeyError: \"The following id_vars or value_vars are not present in the DataFrame: ['Name', 'DRP ID']\"",
    "insertId": "69a7dc8e000d4a5b94d72fb4",
    "resource": {
      "type": "cloud_run_revision",
      "labels": {
        "location": "us-central1",
        "service_name": "drp-score-testing",
        "configuration_name": "drp-score-testing",
        "project_id": "neat-striker-447409-t5",
        "revision_name": "drp-score-testing-00006-c9s"
      }
    },
    "timestamp": "2026-03-04T07:17:34.871003Z",
    "severity": "ERROR",
    "labels": {
      "instanceId": "00da6cd2c41173b0b620fc249a3b11685e3776824ad14056a7702f688e0ffc0e071e2e95dcc06a278c45472bbf382766f916d52809be74078e3d05d69a33ccf97b3ff089ca08ec97962a7b42f1ae4d",
      "run.googleapis.com/base_image_versions": "us-docker.pkg.dev/serverless-runtimes/google-22/runtimes/python311:python311_20260215_3_11_14_RC00"
    },
    "logName": "projects/neat-striker-447409-t5/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-03-04T07:17:34.875639827Z",
    "errorGroups": [
      {
        "id": "CMe7zb-144Xn8QE"
      }
    ]
  }
