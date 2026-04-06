import os
import re
import time
import anthropic
import snowflake.connector
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from typing import List, Optional

load_dotenv()

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SCHEMA_CONTEXT = """
Table: SPOTIFY_DW.PUBLIC.DIM_ARTISTS
Columns:
  - ARTIST_URI (STRING) - unique artist id
  - ARTIST_NAME (STRING) - artist name

Table: SPOTIFY_DW.PUBLIC.DIM_TRACKS
Columns:
  - TRACK_URI (STRING) - unique track id
  - TRACK_NAME (STRING) - song name
  - ALBUM_NAME (STRING) - album name
  - ALBUM_URI (STRING) - album id
  - ARTIST_URI (STRING) - links to DIM_ARTISTS
  - DURATION_MS (INT) - duration in milliseconds

Table: SPOTIFY_DW.PUBLIC.DIM_PLAYLISTS
Columns:
  - PID (INT) - unique playlist id
  - NAME (STRING) - playlist name
  - DESCRIPTION (STRING) - playlist description
  - NUM_TRACKS (INT) - number of tracks
  - NUM_ALBUMS (INT) - number of albums
  - NUM_ARTISTS (INT) - number of unique artists
  - NUM_FOLLOWERS (INT) - follower count
  - NUM_EDITS (INT) - number of edits
  - DURATION_MS (INT) - total duration
  - COLLABORATIVE (BOOLEAN) - is it collaborative
  - MODIFIED_AT (INT) - last modified timestamp

Table: SPOTIFY_DW.PUBLIC.FACT_PLAYLIST_TRACKS
Columns:
  - PID (INT) - links to DIM_PLAYLISTS
  - TRACK_URI (STRING) - links to DIM_TRACKS
  - ARTIST_URI (STRING) - links to DIM_ARTISTS
  - POSITION (INT) - track position in playlist
"""

UNSAFE_PATTERNS = [
    r'\bDROP\b', r'\bDELETE\b', r'\bTRUNCATE\b',
    r'\bINSERT\b', r'\bUPDATE\b', r'\bALTER\b',
    r'\bCREATE\b', r'\bGRANT\b', r'\bREVOKE\b'
]

class Message(BaseModel):
    question: str
    history: Optional[List[dict]] = []

def clean_sql(raw: str) -> str:
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("sql"):
            raw = raw[3:]
    raw = raw.strip()
    match = re.search(r'\b(SELECT|WITH)\b', raw, re.IGNORECASE)
    if match:
        raw = raw[match.start():]
    return raw.strip()

def validate_sql(sql: str) -> tuple[bool, str]:
    if not re.match(r'^\s*(SELECT|WITH)', sql, re.IGNORECASE):
        return False, "Query must be a SELECT statement"

    for pattern in UNSAFE_PATTERNS:
        if re.search(pattern, sql, re.IGNORECASE):
            return False, f"Unsafe SQL operation detected"

    known_tables = ["DIM_ARTISTS", "DIM_TRACKS", "DIM_PLAYLISTS", "FACT_PLAYLIST_TRACKS",
                    "STG_ARTISTS", "STG_TRACKS", "STG_PLAYLISTS", "STG_PLAYLIST_TRACKS",
                    "MART_TOP_ARTISTS", "MART_TOP_TRACKS", "MART_PLAYLIST_STATS"]
    if not any(t in sql.upper() for t in known_tables):
        return False, "Query must reference a known Spotify table"

    return True, ""

def run_query(sql: str):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        columns = [col[0] for col in cur.description]
        rows = cur.fetchall()
        return columns, rows
    finally:
        conn.close()

def generate_sql(client, question: str, history_text: str, attempt: int) -> str:
    # On retry, add extra emphasis on SQL only
    retry_note = ""
    if attempt > 1:
        retry_note = f"\nIMPORTANT: Previous attempt failed. Be extra careful to return ONLY valid Snowflake SQL."

    sql_prompt = f"""You are a SQL expert. Given this schema:
{SCHEMA_CONTEXT}
{history_text}
Write a single valid Snowflake SQL query to answer: "{question}"
Return ONLY the raw SQL query. No explanation. No markdown. No backticks. No intro text.
Start your response directly with SELECT or WITH.{retry_note}"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        messages=[{"role": "user", "content": sql_prompt}]
    )
    return clean_sql(response.content[0].text)

@app.post("/ask")
def ask(msg: Message):
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    history_text = ""
    if msg.history:
        history_text = "\n\nPrevious questions and SQL for context:\n"
        for h in msg.history[-3:]:
            history_text += f"Q: {h['question']}\nSQL: {h['sql']}\n\n"

    MAX_RETRIES = 3
    sql = None
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            sql = generate_sql(client, msg.question, history_text, attempt)
            valid, reason = validate_sql(sql)
            if not valid:
                last_error = f"Invalid SQL generated: {reason}"
                time.sleep(0.5)
                continue

            columns, rows = run_query(sql)
            rows = [list(r) for r in rows[:500]]

            results_preview = f"Columns: {columns}\nFirst 5 rows: {rows[:5]}\nTotal rows: {len(rows)}"
            summary_prompt = f"""The user asked: "{msg.question}"
The SQL query returned these results:
{results_preview}

Write a 1-2 sentence plain English summary of what the data shows.
Be specific with numbers. Be conversational and insightful."""

            summary_response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=200,
                messages=[{"role": "user", "content": summary_prompt}]
            )
            summary = summary_response.content[0].text.strip()

            return {
                "sql": sql,
                "columns": columns,
                "rows": rows,
                "summary": summary,
                "attempts": attempt
            }

        except snowflake.connector.errors.ProgrammingError as e:
            last_error = f"SQL error: {str(e)[:200]}"
            time.sleep(0.5)
            continue

        except Exception as e:
            last_error = str(e)[:200]
            break

    raise HTTPException(
        status_code=422,
        detail=f"Could not generate a valid query after {MAX_RETRIES} attempts. Last error: {last_error}. Try rephrasing your question."
    )

@app.get("/health")
def health():
    return {"status": "ok", "database": "SPOTIFY_DW"}