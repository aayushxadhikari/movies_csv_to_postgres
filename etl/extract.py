import pandas as pd
from etl.config import CSV_CHUNK_SIZE

def extract_csv(csv_path):
    return pd.read_csv(
        csv_path,
        chunksize=CSV_CHUNK_SIZE,
        low_memory=False
    )
