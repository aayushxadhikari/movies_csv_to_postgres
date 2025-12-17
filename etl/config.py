import os
from dotenv import load_dotenv

load_dotenv()

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5433"),
    "db": os.getenv("PG_DB", "etl_db"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

CSV_CHUNK_SIZE = 50000
