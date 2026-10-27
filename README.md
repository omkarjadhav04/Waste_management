# Smart Waste Management Analytics (Apache Spark + Streamlit)

Batch analytics dashboard: PySpark reads and cleans waste-collection CSV data, computes
area-wise totals, waste-type split, recycling % and pickup delays, and Streamlit shows them
as a web dashboard with filters, CSV upload and report download.

**Data:** `waste_data.csv` is generated SAMPLE data (`generate_sample_data.py`), not real
municipal records. It contains a few intentionally bad rows to demonstrate data cleaning.

## Architecture
CSV -> PySpark (clean, group, aggregate) -> small summary tables -> Streamlit dashboard

## Files
- `app.py` - Streamlit dashboard
- `spark_engine.py` - PySpark processing (the core)
- `pandas_engine.py` - fallback used only if Spark cannot start on the host
- `generate_sample_data.py` - creates `waste_data.csv`
- `requirements.txt`, `packages.txt` (Java for hosting), `Dockerfile` (optional, Render)

## CSV columns
`date, ward, waste_type, kg_collected, recyclable_kg, scheduled_pickup, actual_pickup`
(dates as `YYYY-MM-DD`, times as `YYYY-MM-DD HH:MM`)

## Run locally
    pip install -r requirements.txt   # also needs Java 17+
    streamlit run app.py

The page header shows which engine ran (Spark or fallback).
