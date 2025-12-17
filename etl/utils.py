import json
import ast
import logging
from pathlib import Path
import hashlib

# -----------------------
# Logging
# -----------------------
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "etl.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("etl")

# -----------------------
# Processed output folder
# -----------------------
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def save_processed(df, name, chunk_id):
    path = PROCESSED_DIR / f"{name}_chunk_{chunk_id}.csv"
    df.to_csv(path, index=False)
    logger.info(f"Saved processed file: {path}")

# -----------------------
# Parsing helpers
# -----------------------
def is_jsonish(s: str) -> bool:
    s = s.strip()
    return s.startswith("[") or s.startswith("{")

def parse_list(value):
    """
    Parse list-like columns that may be:
    - JSON string: [{"id": 28, "name": "Action"}]
    - Python literal string: [{'id': 28, 'name': 'Action'}]
    If not parseable, return [].
    """
    if value is None:
        return []

    s = str(value).strip()
    if s == "" or s.lower() == "nan":
        return []

    if not is_jsonish(s):
        # Not JSON/literal list/dict
        return []

    # Try JSON
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, list) else []
    except Exception:
        pass

    # Try python literal
    try:
        obj = ast.literal_eval(s)
        return obj if isinstance(obj, list) else []
    except Exception:
        logger.warning(f"Failed to parse list column value (sample): {s[:120]}")
        return []

def stable_id_from_name(name: str) -> int:
    """
    Create a stable numeric id for string categories like genre names.
    """
    h = hashlib.md5(name.encode("utf-8")).hexdigest()
    return int(h[:8], 16)  # fits in BIGINT easily
