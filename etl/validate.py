import pandas as pd
from etl.utils import logger

REQUIRED_COLUMNS = {"id", "title", "release_date"}

def validate_movies(df: pd.DataFrame) -> pd.DataFrame:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    before = len(df)
    df = df[df["id"].notna()].copy()
    df["id"] = df["id"].astype(int)
    after = len(df)

    logger.info(f"Validated movies: {before} -> {after}")
    return df
