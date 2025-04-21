from dagster import ScheduleDefinition, DefaultScheduleStatus
from proj_data_final.jobs import fetch_and_store_job, fetch_top_tracks_job

daily_schedule = ScheduleDefinition(
    job=fetch_and_store_job,
    cron_schedule="0 7 * * *",
    default_status=DefaultScheduleStatus.RUNNING
)

fetch_top_tracks_schedule = ScheduleDefinition(
    job=fetch_top_tracks_job,
    cron_schedule="0 8 * * *",
    default_status=DefaultScheduleStatus.RUNNING
)
