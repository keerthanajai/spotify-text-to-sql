import os
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime

load_dotenv("../backend/.env")

CHECKS_PASSED = 0
CHECKS_FAILED = 0
RESULTS = []

def connect():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

def check(name, query, condition_fn, description):
    global CHECKS_PASSED, CHECKS_FAILED
    conn = connect()
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()[0]
    conn.close()
    passed = condition_fn(result)
    status = "PASS" if passed else "FAIL"
    if passed:
        CHECKS_PASSED += 1
    else:
        CHECKS_FAILED += 1
    RESULTS.append((status, name, result, description))
    print(f"{status} | {name} | value={result} | {description}")

print(f"\n{'='*60}")
print(f"  Spotify DW — Data Quality Report")
print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*60}\n")

#ROW COUNTS
check("dim_artists row count",
    "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_ARTISTS",
    lambda x: x > 50000,
    "Expect 50k+ unique artists")

check("dim_tracks row count",
    "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_TRACKS",
    lambda x: x > 200000,
    "Expect 200k+ unique tracks")

check("dim_playlists row count",
    "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_PLAYLISTS",
    lambda x: x >= 50000,
    "Expect exactly 50k playlists")

check("fact_playlist_tracks row count",
    "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS",
    lambda x: x > 1000000,
    "Expect 1M+ playlist-track rows")

#NULL CHECKS
check("dim_artists no null names",
    "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_ARTISTS WHERE ARTIST_NAME IS NULL OR ARTIST_NAME = ''",
    lambda x: x == 0,
    "No null or empty artist names")

check("dim_tracks no null names",
    "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_TRACKS WHERE TRACK_NAME IS NULL OR TRACK_NAME = ''",
    lambda x: x == 0,
    "No null or empty track names")

check("dim_playlists no null names",
    "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_PLAYLISTS WHERE NAME IS NULL OR NAME = ''",
    lambda x: x == 0,
    "No null or empty playlist names")

check("fact no null pids",
    "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS WHERE PID IS NULL",
    lambda x: x == 0,
    "No null PIDs in fact table")

check("fact no null track uris",
    "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS WHERE TRACK_URI IS NULL",
    lambda x: x == 0,
    "No null track URIs in fact table")

#DUPLICATE CHECKS
check("dim_artists no duplicates",
    "SELECT COUNT(*) - COUNT(DISTINCT ARTIST_URI) FROM SPOTIFY_DW.PUBLIC.DIM_ARTISTS",
    lambda x: x == 0,
    "No duplicate artist URIs")

check("dim_tracks no duplicates",
    "SELECT COUNT(*) - COUNT(DISTINCT TRACK_URI) FROM SPOTIFY_DW.PUBLIC.DIM_TRACKS",
    lambda x: x == 0,
    "No duplicate track URIs")

check("dim_playlists no duplicates",
    "SELECT COUNT(*) - COUNT(DISTINCT PID) FROM SPOTIFY_DW.PUBLIC.DIM_PLAYLISTS",
    lambda x: x == 0,
    "No duplicate playlist IDs")

#REFERENTIAL INTEGRITY
check("fact tracks exist in dim_tracks",
    """SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS f
       LEFT JOIN SPOTIFY_DW.PUBLIC.DIM_TRACKS t ON f.TRACK_URI = t.TRACK_URI
       WHERE t.TRACK_URI IS NULL""",
    lambda x: x == 0,
    "All fact track URIs exist in DIM_TRACKS")

check("fact playlists exist in dim_playlists",
    """SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS f
       LEFT JOIN SPOTIFY_DW.PUBLIC.DIM_PLAYLISTS p ON f.PID = p.PID
       WHERE p.PID IS NULL""",
    lambda x: x == 0,
    "All fact PIDs exist in DIM_PLAYLISTS")

check("fact artists exist in dim_artists",
    """SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS f
       LEFT JOIN SPOTIFY_DW.PUBLIC.DIM_ARTISTS a ON f.ARTIST_URI = a.ARTIST_URI
       WHERE a.ARTIST_URI IS NULL""",
    lambda x: x == 0,
    "All fact artist URIs exist in DIM_ARTISTS")

#BUSINESS LOGIC CHECKS
check("playlists have at least 5 tracks",
    "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_PLAYLISTS WHERE NUM_TRACKS < 5",
    lambda x: x == 0,
    "All playlists have 5+ tracks per Spotify rules")

check("no negative followers",
    "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_PLAYLISTS WHERE NUM_FOLLOWERS < 0",
    lambda x: x == 0,
    "No negative follower counts")

check("track duration is positive",
    "SELECT COUNT(*) FROM SPOTIFY_DW.PUBLIC.DIM_TRACKS WHERE DURATION_MS <= 0",
    lambda x: x == 0,
    "All tracks have positive duration")

check("positions start at 0",
    "SELECT MIN(POSITION) FROM SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS",
    lambda x: x == 0,
    "Track positions are zero-based")

#SUMMARY
total = CHECKS_PASSED + CHECKS_FAILED
print(f"\n{'='*60}")
print(f"  Results: {CHECKS_PASSED}/{total} checks passed")
if CHECKS_FAILED == 0:
    print(f"All checks passed — data quality is solid!")
else:
    print(f"{CHECKS_FAILED} check(s) failed — review above")
print(f"{'='*60}\n")