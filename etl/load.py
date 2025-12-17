import io
from sqlalchemy import text
from etl.db import engine
from etl.utils import logger

def copy_load(df, table):
    if df is None or df.empty:
        logger.info(f"Skipped {table} (empty)")
        return

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    conn = engine.raw_connection()
    cur = conn.cursor()

    try:
        cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV", buffer)
        conn.commit()
        logger.info(f"Loaded {len(df)} rows into {table}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed loading {table}: {e}")
    finally:
        cur.close()
        conn.close()

def truncate_stage_person():
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stg_person;"))

def merge_people():
    sql = """
    INSERT INTO dim_person (person_id, name)
    SELECT DISTINCT person_id, name
    FROM stg_person
    ON CONFLICT (person_id) DO UPDATE
      SET name = EXCLUDED.name;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
        conn.execute(text("TRUNCATE TABLE stg_person;"))
    logger.info("Merged stg_person -> dim_person (upsert)")

def load_all(data):
    # Dimensions
    copy_load(data["movies"], "dim_movie")
    copy_load(data["genres"], "dim_genre")
    copy_load(data["movie_genres"], "bridge_movie_genre")

    # People: stage -> upsert merge (fixes duplicates)
    truncate_stage_person()
    copy_load(data["people"], "stg_person")
    merge_people()

    # Facts
    copy_load(data["cast"], "fact_movie_cast")
    copy_load(data["crew"], "fact_movie_crew")
