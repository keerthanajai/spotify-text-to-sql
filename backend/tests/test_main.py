import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import app, clean_sql, validate_sql

client = TestClient(app)

def test_clean_sql_removes_markdown_backticks():
    raw = "```sql\nSELECT * FROM DIM_TRACKS\n```"
    result = clean_sql(raw)
    assert result.startswith("SELECT")
    assert "```" not in result

def test_clean_sql_removes_intro_text():
    raw = "Here is the SQL query:\nSELECT * FROM DIM_TRACKS"
    result = clean_sql(raw)
    assert result.startswith("SELECT")

def test_clean_sql_handles_clean_input():
    raw = "SELECT * FROM DIM_TRACKS LIMIT 10"
    result = clean_sql(raw)
    assert result == "SELECT * FROM DIM_TRACKS LIMIT 10"

def test_clean_sql_handles_with_clause():
    raw = "WITH cte AS (SELECT 1) SELECT * FROM cte"
    result = clean_sql(raw)
    assert result.startswith("WITH")

def test_validate_sql_accepts_valid_select():
    sql = "SELECT * FROM SPOTIFY_DW.PUBLIC.DIM_TRACKS LIMIT 10"
    valid, reason = validate_sql(sql)
    assert valid is True
    assert reason == ""

def test_validate_sql_blocks_drop():
    sql = "DROP TABLE DIM_TRACKS"
    valid, reason = validate_sql(sql)
    assert valid is False

def test_validate_sql_blocks_delete():
    sql = "DELETE FROM DIM_TRACKS WHERE 1=1"
    valid, reason = validate_sql(sql)
    assert valid is False

def test_validate_sql_blocks_insert():
    sql = "INSERT INTO DIM_TRACKS VALUES ('test', 'test')"
    valid, reason = validate_sql(sql)
    assert valid is False

def test_validate_sql_rejects_unknown_table():
    sql = "SELECT * FROM SOME_RANDOM_TABLE"
    valid, reason = validate_sql(sql)
    assert valid is False
    assert "known" in reason

def test_validate_sql_accepts_mart_tables():
    sql = "SELECT * FROM SPOTIFY_DW.PUBLIC.MART_TOP_ARTISTS LIMIT 5"
    valid, reason = validate_sql(sql)
    assert valid is True

def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

def test_ask_endpoint_missing_question():
    response = client.post("/ask", json={})
    assert response.status_code == 422

def test_ask_endpoint_with_mock():
    mock_sql = "SELECT ARTIST_NAME FROM SPOTIFY_DW.PUBLIC.DIM_ARTISTS LIMIT 5"
    mock_columns = ["ARTIST_NAME"]
    mock_rows = [["Drake"], ["Rihanna"], ["Kanye West"]]
    mock_summary = "The top artists in the dataset are Drake, Rihanna, and Kanye West."

    with patch("main.generate_sql", return_value=mock_sql), \
         patch("main.run_query", return_value=(mock_columns, mock_rows)), \
         patch("main.anthropic.Anthropic") as mock_anthropic:

        mock_client = MagicMock()
        mock_anthropic.return_value = mock_client
        mock_client.messages.create.return_value.content[0].text = mock_summary

        response = client.post("/ask", json={"question": "Who are the top artists?"})

        assert response.status_code == 200
        data = response.json()
        assert "sql" in data
        assert "columns" in data
        assert "rows" in data
        assert "summary" in data
