# CSV → PostgreSQL Batch ETL Pipeline

## Tech
- Python
- Pandas
- PostgreSQL (Docker)
- SQLAlchemy

## How to Run
1. Start Postgres via Docker
2. Apply schema.sql
3. Run ETL:
   python -m etl.main data/raw/movies.csv

## Features
- Chunked CSV processing
- Normalized schema
- SQL analytics
