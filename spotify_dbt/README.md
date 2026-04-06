# Spotify Text-to-SQL

A full-stack data engineering and AI project that lets you query 3.3 million real Spotify playlist relationships using plain English.

Ask "Who are the most playlisted artists?" and get back a Claude-generated SQL query, executed against a Snowflake data warehouse, with results and a plain English summary — all in under 5 seconds.

![Top Artists](assets/insight2_top_artists.png)

---

## What I Built

I wanted to combine two things I was learning — large-scale data engineering and LLM-powered applications — into something that actually works end to end. The result is a Text-to-SQL dashboard backed by a production-grade data pipeline built on real Spotify data.

---

## The Data

**Source:** Spotify Million Playlist Dataset (RecSys Challenge 2018)  
50,000 real user-generated playlists, created between 2010 and 2017.

| Table | Rows | Description |
|---|---|---|
| DIM_ARTISTS | 80,483 | Unique artists |
| DIM_TRACKS | 461,871 | Unique tracks |
| DIM_PLAYLISTS | 50,000 | Playlist metadata |
| FACT_PLAYLIST_TRACKS | 3,344,328 | Track-playlist relationships |

---

## Architecture

```
Spotify MPD (JSON slices)
        ↓
Python ingestion pipeline (mpd_pipeline.py)
        ↓
Data validation (19 quality checks)
        ↓
Snowflake Data Warehouse (SPOTIFY_DW)
        ↓
dbt transformations (staging → marts)
        ↓
Prefect orchestration (scheduled + monitored)
        ↓
FastAPI backend + Claude API
        ↓
Plain English → SQL → Results → Summary
```

---

## Data Engineering Stack

### Ingestion
Raw JSON slices are parsed, deduplicated, and validated before loading into Snowflake. The pipeline automatically filters invalid records (e.g. tracks with zero duration) and removes orphaned fact rows.

### dbt Models
Transformations are written as pure SQL dbt models — not pandas scripts — making them testable, documented, and version controlled.

```
staging/
├── stg_tracks.sql          — cleaned tracks with duration in minutes
├── stg_artists.sql         — deduplicated artists
├── stg_playlists.sql       — playlists with parsed timestamps
└── stg_playlist_tracks.sql — fact relationships

marts/
├── mart_top_tracks.sql     — tracks ranked by playlist appearances
├── mart_top_artists.sql    — artists ranked by playlist reach
└── mart_playlist_stats.sql — playlist-level aggregations
```

![dbt Lineage](assets/dbt_lineage.png)

### Data Quality
19 automated checks run after every pipeline execution covering:
- Row count assertions
- Null checks on critical columns
- Duplicate detection
- Referential integrity across all 4 tables
- Business logic validation (e.g. all playlists have 5+ tracks per Spotify's own rules)

### Orchestration
The full pipeline runs as a Prefect flow with structured task logging, automatic retry on failure, and orphan row cleanup built in.

---

## The App

**Backend:** FastAPI + Anthropic Claude API + Snowflake connector

The LLM layer is hardened with:
- SQL validation before execution (blocks DROP, DELETE, INSERT etc.)
- Up to 3 automatic retries with smarter prompting on each attempt
- Results capped at 500 rows to keep the UI responsive
- Friendly error messages when a question can't be answered

**Frontend:** Vanilla HTML/CSS/JS with a query history sidebar and plain English summaries generated for every result.

---

## What I Found

I analyzed the data to understand what separates playlists that grow followers from ones that stay personal.

**Drake appears in nearly 1 in 5 playlists.** 9,859 out of 50,000 — that's not just popularity, that's cultural dominance. The top 20 artists follow a classic power-law distribution.

**The 11-100 follower tier is where the real curators live.** These playlists have 67% more tracks, double the edits, and 48% more unique artists than low-follower playlists. Active curation drives early growth.

**HUMBLE. by Kendrick Lamar is the most playlisted track** with 2,183 appearances — more than any other song in the dataset.

**More tracks ≠ more followers.** The scatter plot is almost flat. What you put in matters more than how much.

![Top Artists](assets/insight2_top_artists.png)
![Top Tracks](assets/insight5_top_tracks.png)
![Playlist Sweet Spot](assets/insight4_sweet_spot.png)
![Follower Buckets](assets/insight3_follower_buckets.png)

---

## Running Locally

**1. Clone the repo:**
```bash
git clone https://github.com/YOUR_USERNAME/spotify-text-to-sql.git
cd spotify-text-to-sql
```

**2. Set up environment:**
```bash
cd backend
pip install -r requirements.txt
cp .env.example .env
# Fill in your Snowflake and Anthropic credentials
```

**3. Run the pipeline:**
```bash
cd data
python3 mpd_pipeline.py
python3 quality_checks.py
```

**4. Run dbt:**
```bash
cd spotify_dbt
dbt run
dbt test
```

**5. Start the backend:**
```bash
cd backend
uvicorn main:app --reload
```

**6. Open the frontend:**
```bash
open frontend/index.html
```

---

## Project Structure

```
spotify-text-to-sql/
├── backend/
│   ├── main.py              ← FastAPI app with LLM hardening
│   ├── requirements.txt
│   └── .env.example
├── data/
│   ├── mpd_pipeline.py      ← ingestion pipeline
│   ├── orchestration.py     ← Prefect flow
│   └── quality_checks.py    ← 19 data quality checks
├── spotify_dbt/
│   ├── models/
│   │   ├── staging/         ← cleaned source models
│   │   └── marts/           ← analytical models
│   └── dbt_project.yml
├── frontend/
│   └── index.html           ← chat UI
├── assets/                  ← charts and diagrams
└── spotify_analysis.ipynb   ← exploratory analysis
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data warehouse | Snowflake |
| Transformations | dbt |
| Orchestration | Prefect |
| Backend | FastAPI (Python) |
| LLM | Claude (Anthropic) |
| Frontend | HTML / CSS / JS |
| Analysis | Jupyter + pandas + matplotlib |