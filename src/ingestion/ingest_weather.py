"""
Ingests hourly NYC weather data from Open-Meteo historical archive API.
Stores raw JSON responses in raw.weather_raw (bronze layer).

Features:
- Chunked pagination: fetches one week at a time to stay within API limits
- Retry with exponential backoff: handles transient API failures
- Incremental sync: skips date ranges already loaded
- Idempotent: safe to re-run without duplicating data

Usage:
    python3 src/ingestion/ingest_weather.py
"""

import json
import time
import logging
from datetime import date, timedelta

import requests
from sqlalchemy import create_engine, text

# --- Config ---
DB_URL = "postgresql://taxi_user:taxi_pass@localhost:5433/taxi_db"
API_BASE = "https://archive-api.open-meteo.com/v1/archive"
NYC_LAT = 40.7128
NYC_LON = -74.0060
TIMEZONE = "America/New_York"
START_DATE = date(2023, 1, 1)
END_DATE = date(2023, 1, 31)
CHUNK_DAYS = 7       # fetch one week per API request
MAX_RETRIES = 3
BACKOFF_BASE = 2     # seconds — doubles on each retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CREATE_RAW_TABLE = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.weather_raw (
    id              serial primary key,
    fetch_date      date not null,
    start_date      date not null,
    end_date        date not null,
    response_json   jsonb not null,
    fetched_at      timestamptz default current_timestamp,
    UNIQUE (start_date, end_date)
);
"""


def fetch_with_retry(params: dict) -> dict:
    """Call the API with exponential backoff on failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(API_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("error"):
                raise ValueError(f"API error: {data.get('reason')}")
            return data
        except (requests.RequestException, ValueError) as e:
            if attempt == MAX_RETRIES:
                raise
            wait = BACKOFF_BASE ** attempt
            log.warning(f"Attempt {attempt} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)


def get_loaded_ranges(conn) -> set:
    """Return set of (start_date, end_date) already in the DB — for incremental sync."""
    rows = conn.execute(
        text("SELECT start_date, end_date FROM raw.weather_raw")
    ).fetchall()
    return {(r.start_date, r.end_date) for r in rows}


def date_chunks(start: date, end: date, chunk_days: int):
    """Yield (chunk_start, chunk_end) tuples covering start→end in chunks."""
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def ingest():
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        conn.execute(text(CREATE_RAW_TABLE))

    log.info(f"Ingesting NYC weather {START_DATE} → {END_DATE} in {CHUNK_DAYS}-day chunks")

    with engine.begin() as conn:
        loaded = get_loaded_ranges(conn)

    inserted = 0
    skipped = 0

    for chunk_start, chunk_end in date_chunks(START_DATE, END_DATE, CHUNK_DAYS):
        key = (chunk_start, chunk_end)

        # Incremental: skip already loaded ranges
        if key in loaded:
            log.info(f"  Skipping {chunk_start} → {chunk_end} (already loaded)")
            skipped += 1
            continue

        log.info(f"  Fetching {chunk_start} → {chunk_end}...")
        params = {
            "latitude": NYC_LAT,
            "longitude": NYC_LON,
            "hourly": "temperature_2m,precipitation,windspeed_10m",
            "start_date": chunk_start.isoformat(),
            "end_date": chunk_end.isoformat(),
            "timezone": TIMEZONE,
        }

        data = fetch_with_retry(params)

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO raw.weather_raw
                        (fetch_date, start_date, end_date, response_json)
                    VALUES
                        (:fetch_date, :start_date, :end_date, :response_json)
                    ON CONFLICT (start_date, end_date) DO NOTHING
                """),
                {
                    "fetch_date": date.today(),
                    "start_date": chunk_start,
                    "end_date": chunk_end,
                    "response_json": json.dumps(data),
                }
            )
        inserted += 1
        log.info(f"  Loaded {chunk_start} → {chunk_end} ({len(data['hourly']['time'])} hours)")

        # Be polite to the API — small delay between requests
        time.sleep(0.5)

    log.info(f"Done. Inserted: {inserted} chunks, Skipped: {skipped} chunks.")


if __name__ == "__main__":
    ingest()
