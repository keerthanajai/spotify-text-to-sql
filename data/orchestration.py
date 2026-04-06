import os
import json
import glob
import re
import snowflake.connector
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta

load_dotenv("../backend/.env")

def get_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

@task(name="extract_json_slices", cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=24))
def extract(data_dir: str):
    logger = get_run_logger()
    files = sorted(glob.glob(f"{data_dir}/mpd.slice.*.json"))
    logger.info(f"Found {len(files)} JSON slice files")

    playlists_rows, tracks_dict, artists_dict, fact_rows = [], {}, {}, []

    for i, filepath in enumerate(files):
        with open(filepath, "r") as f:
            data = json.load(f)

        for playlist in data["playlists"]:
            pid = playlist["pid"]
            playlists_rows.append((
                pid,
                playlist.get("name", ""),
                playlist.get("description", ""),
                playlist.get("num_tracks", 0),
                playlist.get("num_albums", 0),
                playlist.get("num_artists", 0),
                playlist.get("num_followers", 0),
                playlist.get("num_edits", 0),
                playlist.get("duration_ms", 0),
                bool(playlist.get("collaborative", False)),
                playlist.get("modified_at", 0)
            ))
            for track in playlist.get("tracks", []):
                track_uri = track["track_uri"]
                artist_uri = track["artist_uri"]
                if track_uri not in tracks_dict:
                    tracks_dict[track_uri] = (
                        track_uri,
                        track.get("track_name", ""),
                        track.get("album_name", ""),
                        track.get("album_uri", ""),
                        artist_uri,
                        track.get("duration_ms", 0)
                    )
                if artist_uri not in artists_dict:
                    artists_dict[artist_uri] = (artist_uri, track.get("artist_name", ""))
                fact_rows.append((pid, track_uri, artist_uri, track.get("pos", 0)))

        if (i + 1) % 10 == 0:
            logger.info(f"Processed {i+1}/{len(files)} files")

    logger.info(f"Extracted: {len(playlists_rows):,} playlists, {len(tracks_dict):,} tracks, {len(fact_rows):,} fact rows")
    return playlists_rows, list(tracks_dict.values()), list(artists_dict.values()), fact_rows


@task(name="validate_data")
def validate(playlists_rows, tracks_rows, artists_rows, fact_rows):
    logger = get_run_logger()
    errors = []

    if len(playlists_rows) < 1000:
        errors.append(f"Too few playlists: {len(playlists_rows)}")
    if len(tracks_rows) < 10000:
        errors.append(f"Too few tracks: {len(tracks_rows)}")
    if len(fact_rows) < 50000:
        errors.append(f"Too few fact rows: {len(fact_rows)}")
    null_tracks = [t for t in tracks_rows if not t[1]]
    if null_tracks:
        errors.append(f"{len(null_tracks)} tracks with empty names")
    bad_duration = [t for t in tracks_rows if t[5] <= 0]
    if bad_duration:
        logger.warning(f"Found {len(bad_duration)} tracks with invalid duration — will be filtered")
        tracks_rows = [t for t in tracks_rows if t[5] > 0]

    if errors:
        for e in errors:
            logger.error(f"Validation error: {e}")
        raise ValueError(f"Data validation failed: {errors}")

    logger.info(f"Validation passed — {len(playlists_rows):,} playlists, {len(tracks_rows):,} tracks")
    return playlists_rows, tracks_rows, artists_rows, fact_rows


@task(name="load_to_snowflake")
def load(playlists_rows, tracks_rows, artists_rows, fact_rows):
    logger = get_run_logger()
    conn = get_conn()
    cur = conn.cursor()

    def batch_insert(table, rows, columns, batch_size=5000):
        placeholders = ",".join(["%s"] * len(columns))
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            cur.executemany(
                f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})",
                batch
            )
        logger.info(f"Loaded {len(rows):,} rows into {table}")

    cur.execute("CREATE OR REPLACE TABLE SPOTIFY_DW.PUBLIC.DIM_ARTISTS (ARTIST_URI STRING, ARTIST_NAME STRING)")
    cur.execute("CREATE OR REPLACE TABLE SPOTIFY_DW.PUBLIC.DIM_TRACKS (TRACK_URI STRING, TRACK_NAME STRING, ALBUM_NAME STRING, ALBUM_URI STRING, ARTIST_URI STRING, DURATION_MS INT)")
    cur.execute("CREATE OR REPLACE TABLE SPOTIFY_DW.PUBLIC.DIM_PLAYLISTS (PID INT, NAME STRING, DESCRIPTION STRING, NUM_TRACKS INT, NUM_ALBUMS INT, NUM_ARTISTS INT, NUM_FOLLOWERS INT, NUM_EDITS INT, DURATION_MS INT, COLLABORATIVE BOOLEAN, MODIFIED_AT INT)")
    cur.execute("CREATE OR REPLACE TABLE SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS (PID INT, TRACK_URI STRING, ARTIST_URI STRING, POSITION INT)")

    batch_insert("SPOTIFY_DW.PUBLIC.DIM_ARTISTS", artists_rows, ["ARTIST_URI", "ARTIST_NAME"])
    batch_insert("SPOTIFY_DW.PUBLIC.DIM_TRACKS", tracks_rows, ["TRACK_URI", "TRACK_NAME", "ALBUM_NAME", "ALBUM_URI", "ARTIST_URI", "DURATION_MS"])
    batch_insert("SPOTIFY_DW.PUBLIC.DIM_PLAYLISTS", playlists_rows, ["PID", "NAME", "DESCRIPTION", "NUM_TRACKS", "NUM_ALBUMS", "NUM_ARTISTS", "NUM_FOLLOWERS", "NUM_EDITS", "DURATION_MS", "COLLABORATIVE", "MODIFIED_AT"])
    batch_insert("SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS", fact_rows, ["PID", "TRACK_URI", "ARTIST_URI", "POSITION"])
    cur.execute("""
        DELETE FROM SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS
        WHERE TRACK_URI NOT IN (
            SELECT TRACK_URI FROM SPOTIFY_DW.PUBLIC.DIM_TRACKS
        )
    """)
    logger.info("🧹 Cleaned orphan fact rows")
    conn.commit()
    conn.close()
    logger.info("All data loaded to Snowflake successfully")


@task(name="quality_checks")
def quality_checks():
    logger = get_run_logger()
    conn = get_conn()
    cur = conn.cursor()
    failures = []

    checks = [
        ("DIM_ARTISTS row count",        "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_ARTISTS",                                              lambda x: x > 50000),
        ("DIM_TRACKS row count",         "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_TRACKS",                                               lambda x: x > 200000),
        ("DIM_PLAYLISTS row count",      "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_PLAYLISTS",                                            lambda x: x >= 50000),
        ("FACT row count",               "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS",                                     lambda x: x > 1000000),
        ("No null artist names",         "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_ARTISTS WHERE ARTIST_NAME IS NULL OR ARTIST_NAME=''",  lambda x: x == 0),
        ("No null track names",          "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_TRACKS WHERE TRACK_NAME IS NULL OR TRACK_NAME=''",     lambda x: x == 0),
        ("No duplicate tracks",          "SELECT COUNT(*)-COUNT(DISTINCT TRACK_URI) FROM SPOTIFY_DW.PUBLIC.DIM_TRACKS",                     lambda x: x == 0),
        ("No duplicate playlists",       "SELECT COUNT(*)-COUNT(DISTINCT PID) FROM SPOTIFY_DW.PUBLIC.DIM_PLAYLISTS",                        lambda x: x == 0),
        ("Valid track durations",        "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_TRACKS WHERE DURATION_MS <= 0",                        lambda x: x == 0),
        ("No orphan fact tracks",        "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS f LEFT JOIN SPOTIFY_DW.PUBLIC.DIM_TRACKS t ON f.TRACK_URI=t.TRACK_URI WHERE t.TRACK_URI IS NULL", lambda x: x == 0),
    ]

    for name, query, condition in checks:
        cur.execute(query)
        val = cur.fetchone()[0]
        if condition(val):
            logger.info(f"{name}: {val}")
        else:
            logger.error(f"{name}: {val}")
            failures.append(name)

    conn.close()
    if failures:
        raise ValueError(f"Quality checks failed: {failures}")
    logger.info("All quality checks passed!")


@flow(name="spotify_etl_pipeline", log_prints=True)
def spotify_pipeline(data_dir: str = "."):
    logger = get_run_logger()
    logger.info("🚀 Starting Spotify ETL pipeline")

    playlists_rows, tracks_rows, artists_rows, fact_rows = extract(data_dir)
    playlists_rows, tracks_rows, artists_rows, fact_rows = validate(
        playlists_rows, tracks_rows, artists_rows, fact_rows
    )
    load(playlists_rows, tracks_rows, artists_rows, fact_rows)
    quality_checks()

    logger.info("Pipeline complete!")


if __name__ == "__main__":
    spotify_pipeline(data_dir=".")