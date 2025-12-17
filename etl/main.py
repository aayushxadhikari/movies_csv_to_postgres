import sys
from etl.extract import extract_csv
from etl.validate import validate_movies
from etl.transform import transform_movies
from etl.load import load_all
from etl.utils import logger, save_processed

def run(csv_path):
    for chunk_id, chunk in enumerate(extract_csv(csv_path), start=1):
        logger.info(f"Processing chunk {chunk_id}")

        chunk = validate_movies(chunk)
        data = transform_movies(chunk)

        # Save processed outputs
        save_processed(data["movies"], "movies", chunk_id)
        save_processed(data["genres"], "genres", chunk_id)
        save_processed(data["movie_genres"], "movie_genres", chunk_id)
        save_processed(data["people"], "people", chunk_id)
        save_processed(data["cast"], "cast", chunk_id)
        save_processed(data["crew"], "crew", chunk_id)

        # Load into Postgres
        load_all(data)

    logger.info("ETL completed successfully")

if __name__ == "__main__":
    run(sys.argv[1])
