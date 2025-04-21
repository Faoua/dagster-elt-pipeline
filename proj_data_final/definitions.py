from dagster import Definitions, load_assets_from_modules
from proj_data_final import assets  
from proj_data_final.jobs import fetch_and_store_job, fetch_top_tracks_job, fetch_top_tracks_full_job
from proj_data_final.schedules import daily_schedule, fetch_top_tracks_schedule
from proj_data_final.sensors import manual_trigger_sensor

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[fetch_and_store_job, fetch_top_tracks_job, fetch_top_tracks_full_job],
    schedules=[daily_schedule, fetch_top_tracks_schedule],
    sensors=[manual_trigger_sensor]
)
