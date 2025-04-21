# proj_data_final/sensors.py
from dagster import RunRequest, sensor
from proj_data_final.jobs import fetch_and_store_job

@sensor(job=fetch_and_store_job)
def manual_trigger_sensor(context):
    return RunRequest(run_key=None)
