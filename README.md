# NYC Weather API Ingestion Pipeline

Ingests hourly NYC weather data from the Open-Meteo API and joins it with
NYC Yellow Taxi trip data to analyse how weather affects taxi demand.

## What this builds

| Layer | Table | Description |
|-------|-------|-------------|
| Bronze | `raw.weather_raw` | Raw JSON responses from Open-Meteo API |
| Silver | `stg_weather_hourly` | Cleaned hourly weather (temp, precipitation, wind) |
| Gold | `gold_weather_trip_correlation` | Daily weather vs taxi trip volume |

## Skills demonstrated
- API pagination (chunking date ranges into requests)
- Retry logic with exponential backoff
- Incremental sync (only fetch dates not yet loaded)
- Idempotent loading (safe to re-run)
- Bronze→Silver→Gold medallion architecture

## Stack
- Python 3.11, requests, SQLAlchemy, pandas
- PostgreSQL 15 (shared with Project 1)
- dbt 1.11.7

## How to run
```bash
# 1. Ingest weather data
python3 src/ingestion/ingest_weather.py

# 2. Transform with dbt
cd dbt_weather && dbt run && dbt test
```
