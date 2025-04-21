# tests/test_assets.py

from proj_data_final.assets import fetch_top_tracks
from dagster import build_op_context
import pandas as pd

def test_fetch_top_tracks_returns_dataframe():
    context = build_op_context(partition_key="2024-04-21")
    df = fetch_top_tracks(context)
    
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "title" in df.columns
