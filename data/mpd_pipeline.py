import json
import os
import glob
import snowflake.connector
from dotenv import load_dotenv

load_dotenv("../backend/.env")

# ── 1. PARSE ALL 50 JSON SLICES ────────────────────────────────────
print("📂 Reading JSON slices...")

playlists_rows = []
tracks_dict = {}
artists_dict = {}
fact_rows = []

files = sorted(glob.glob("mpd.slice.*.json"))
print(f"  Found {len(files)} slice files")

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
            playlist.get("collaborative", "false").lower() == "true",
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
                artists_dict[artist_uri] = (
                    artist_uri,
                    track.get("artist_name", "")
                )

            # Fact row
            fact_rows.append((
                pid,
                track_uri,
                artist_uri,
                track.get("pos", 0)
            ))

    if (i + 1) % 10 == 0:
        print(f"  Processed {i + 1}/{len(files)} files...")

print(f"\nParsed:")
print(f"  Playlists:       {len(playlists_rows):,}")
print(f"  Unique tracks:   {len(tracks_dict):,}")
print(f"  Unique artists:  {len(artists_dict):,}")
print(f"  Fact rows:       {len(fact_rows):,}")

# ── 2. CONNECT TO SNOWFLAKE ────────────────────────────────────────
print("\n Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
)
cur = conn.cursor()

# ── 3. CREATE DATABASE & TABLES ────────────────────────────────────
print("Creating schema...")
cur.execute("CREATE DATABASE IF NOT EXISTS SPOTIFY_DW")
cur.execute("USE DATABASE SPOTIFY_DW")
cur.execute("USE SCHEMA PUBLIC")

cur.execute("""
CREATE OR REPLACE TABLE DIM_ARTISTS (
    ARTIST_URI   STRING PRIMARY KEY,
    ARTIST_NAME  STRING
)""")

cur.execute("""
CREATE OR REPLACE TABLE DIM_TRACKS (
    TRACK_URI    STRING PRIMARY KEY,
    TRACK_NAME   STRING,
    ALBUM_NAME   STRING,
    ALBUM_URI    STRING,
    ARTIST_URI   STRING,
    DURATION_MS  INT
)""")

cur.execute("""
CREATE OR REPLACE TABLE DIM_PLAYLISTS (
    PID              INT PRIMARY KEY,
    NAME             STRING,
    DESCRIPTION      STRING,
    NUM_TRACKS       INT,
    NUM_ALBUMS       INT,
    NUM_ARTISTS      INT,
    NUM_FOLLOWERS    INT,
    NUM_EDITS        INT,
    DURATION_MS      INT,
    COLLABORATIVE    BOOLEAN,
    MODIFIED_AT      INT
)""")

cur.execute("""
CREATE OR REPLACE TABLE FACT_PLAYLIST_TRACKS (
    PID         INT,
    TRACK_URI   STRING,
    ARTIST_URI  STRING,
    POSITION    INT
)""")

# ── 4. LOAD DATA IN BATCHES ────────────────────────────────────────
def batch_insert(cur, table, rows, columns, batch_size=5000):
    placeholders = ",".join(["%s"] * len(columns))
    total = len(rows)
    for i in range(0, total, batch_size):
        batch = rows[i:i+batch_size]
        cur.executemany(
            f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})",
            batch
        )
        print(f"  {table}: {min(i+batch_size, total):,}/{total:,} rows")

print("\n Loading DIM_ARTISTS...")
batch_insert(cur, "DIM_ARTISTS", list(artists_dict.values()),
    ["ARTIST_URI", "ARTIST_NAME"])

print("\n Loading DIM_TRACKS...")
batch_insert(cur, "DIM_TRACKS", list(tracks_dict.values()),
    ["TRACK_URI", "TRACK_NAME", "ALBUM_NAME", "ALBUM_URI", "ARTIST_URI", "DURATION_MS"])

print("\n Loading DIM_PLAYLISTS...")
batch_insert(cur, "DIM_PLAYLISTS", playlists_rows,
    ["PID", "NAME", "DESCRIPTION", "NUM_TRACKS", "NUM_ALBUMS",
     "NUM_ARTISTS", "NUM_FOLLOWERS", "NUM_EDITS", "DURATION_MS",
     "COLLABORATIVE", "MODIFIED_AT"])

print("\n Loading FACT_PLAYLIST_TRACKS...")
batch_insert(cur, "FACT_PLAYLIST_TRACKS", fact_rows,
    ["PID", "TRACK_URI", "ARTIST_URI", "POSITION"])

conn.commit()
conn.close()

print("\n Pipeline complete! SPOTIFY_DW is ready.")
print(f"   {len(playlists_rows):,} playlists")
print(f"   {len(tracks_dict):,} unique tracks")
print(f"   {len(artists_dict):,} unique artists")
print(f"   {len(fact_rows):,} playlist-track relationships")