# proj_data_final/jobs.py
'''
from dagster import define_asset_job

fetch_and_store_job = define_asset_job(
    name="fetch_and_store_job",
    selection="*"
)

fetch_top_tracks_job = define_asset_job(
    name="fetch_top_tracks_job",
    selection=["fetch_top_tracks"]
)
'''

# proj_data_final/jobs.py
from dagster import define_asset_job, DailyPartitionsDefinition

# Définition sans partition (tout exécuter)
fetch_and_store_job = define_asset_job(
    name="fetch_and_store_job",
    selection="*"
)

# Définition AVEC partitions pour fetch_top_tracks
daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

fetch_top_tracks_job = define_asset_job(
    name="fetch_top_tracks_job",
    selection=["fetch_top_tracks", "save_tracks_to_duckdb"],
    partitions_def=daily_partitions
)




# ✅ AJOUTE BIEN CECI 👇
fetch_top_tracks_full_job = define_asset_job(
    name="fetch_top_tracks_full_job",
    selection=["fetch_top_tracks", "save_tracks_to_duckdb"],
    partitions_def=daily_partitions
)