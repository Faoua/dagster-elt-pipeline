import requests
import pandas as pd
import duckdb
from dagster import asset, DailyPartitionsDefinition
from dagster import AssetIn, asset
from dagster import asset


daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

# ------------------ FETCH ------------------

@asset
def fetch_top_tracks() -> pd.DataFrame:
    response = requests.get("https://api.deezer.com/chart")
    tracks = response.json().get("tracks", {}).get("data", [])
    return pd.DataFrame([{
        "rank": t.get("position"),
        "title": t.get("title"),
        "artist": t.get("artist", {}).get("name"),
        "album": t.get("album", {}).get("title"),
        "duration": t.get("duration"),
        "explicit": t.get("explicit_lyrics"),
        "link": t.get("link")
    } for t in tracks])


@asset
def fetch_top_albums() -> pd.DataFrame:
    response = requests.get("https://api.deezer.com/chart")
    albums = response.json().get("albums", {}).get("data", [])
    return pd.DataFrame([{
        "rank": a.get("position"),
        "title": a.get("title"),
        "artist": a.get("artist", {}).get("name"),
        "nb_tracks": a.get("nb_tracks"),
        "release_date": a.get("release_date"),
        "link": a.get("link")
    } for a in albums])


@asset
def fetch_top_artists() -> pd.DataFrame:
    response = requests.get("https://api.deezer.com/chart")
    artists = response.json().get("artists", {}).get("data", [])
    return pd.DataFrame([{
        "rank": a.get("position"),
        "name": a.get("name"),
        "nb_fans": a.get("nb_fan"),
        "link": a.get("link")
    } for a in artists])


@asset
def fetch_top_playlists() -> pd.DataFrame:
    response = requests.get("https://api.deezer.com/chart")
    playlists = response.json().get("playlists", {}).get("data", [])
    return pd.DataFrame([{
        "rank": p.get("position"),
        "title": p.get("title"),
        "nb_tracks": p.get("nb_tracks"),
        "user": p.get("user", {}).get("name"),
        "link": p.get("link")
    } for p in playlists])


# ------------------ SAVE ------------------

@asset(
    deps=["fetch_top_tracks"],
    partitions_def=daily_partitions
)
def save_tracks_to_duckdb(context, fetch_top_tracks: pd.DataFrame):
    date = context.partition_key  # Important pour debug
    context.log.info(f"Inserting partition for {date}")

    with duckdb.connect("deezer_top.db") as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS top_tracks_partitioned (
                partition_date DATE,
                rank INTEGER,
                title TEXT,
                artist TEXT,
                album TEXT,
                duration INTEGER,
                explicit BOOLEAN,
                link TEXT
            )
        """)
        con.register("df", fetch_top_tracks)
        con.execute("INSERT INTO top_tracks_partitioned SELECT * FROM df")

@asset(deps=[fetch_top_albums])
def save_albums_to_duckdb(fetch_top_albums: pd.DataFrame):
    con = duckdb.connect("deezer_top.db")

    # Crée une table vide si elle n'existe pas (structure identique à la DataFrame)
    con.register("df_temp", fetch_top_albums)
    con.execute("""
        CREATE TABLE IF NOT EXISTS top_albums AS 
        SELECT * FROM df_temp LIMIT 0
    """)

    # Vide la table si elle existe déjà
    con.execute("DELETE FROM top_albums")

    # Réinsère les nouvelles données
    con.register("df", fetch_top_albums)
    con.execute("INSERT INTO top_albums SELECT * FROM df")

    con.close()



@asset(deps=[fetch_top_artists])
def save_artists_to_duckdb(fetch_top_artists: pd.DataFrame):
    con = duckdb.connect("deezer_top.db")
    con.execute("CREATE TABLE IF NOT EXISTS top_artists AS SELECT * FROM fetch_top_artists")
    con.execute("DELETE FROM top_artists")
    con.register("df", fetch_top_artists)
    con.execute("INSERT INTO top_artists SELECT * FROM df")
    con.close()


@asset(deps=[fetch_top_playlists])
def save_playlists_to_duckdb(fetch_top_playlists: pd.DataFrame):
    con = duckdb.connect("deezer_top.db")
    con.execute("CREATE TABLE IF NOT EXISTS top_playlists AS SELECT * FROM fetch_top_playlists")
    con.execute("DELETE FROM top_playlists")
    con.register("df", fetch_top_playlists)
    con.execute("INSERT INTO top_playlists SELECT * FROM df")
    con.close()


daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

@asset(partitions_def=daily_partitions)
def fetch_top_tracks(context) -> pd.DataFrame:
    date = context.partition_key
    context.log.info(f"Partition: {date}")

    url = "https://api.deezer.com/chart"
    response = requests.get(url)
    data = response.json()

    rows = []
    for track in data["tracks"]["data"]:
        rows.append({
            "partition_date": date,
            "rank": track["position"],
            "title": track["title"],
            "artist": track["artist"]["name"],
            "album": track["album"]["title"],
            "duration": track["duration"],
            "explicit": track["explicit_lyrics"],
            "link": track["link"]
        })

    return pd.DataFrame(rows)


@asset(partitions_def=daily_partitions)
def fetch_top_tracks(context) -> pd.DataFrame:
    date = context.partition_key
    context.log.info(f"🔍 Fetching top tracks for partition date: {date}")

    url = "https://api.deezer.com/chart"
    response = requests.get(url)

    if response.status_code != 200:
        context.log.error(f"❌ API call failed with status {response.status_code}")
        raise Exception("API fetch failed")

    data = response.json()
    context.log.info("✅ API fetch succeeded. Now parsing data...")

    rows = []
    for track in data["tracks"]["data"]:
        rows.append({
            "partition_date": date,
            "rank": track["position"],
            "title": track["title"],
            "artist": track["artist"]["name"],
            "album": track["album"]["title"],
            "duration": track["duration"],
            "explicit": track["explicit_lyrics"],
            "link": track["link"]
        })

    context.log.info(f"📦 Parsed {len(rows)} top tracks.")
    return pd.DataFrame(rows)

