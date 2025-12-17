CREATE TABLE IF NOT EXISTS dim_movie (
  movie_id BIGINT PRIMARY KEY,
  title TEXT,
  original_language TEXT,
  release_date DATE,
  budget BIGINT,
  revenue BIGINT,
  runtime INT,
  vote_average FLOAT,
  vote_count INT
);

CREATE TABLE IF NOT EXISTS dim_genre (
  genre_id BIGINT PRIMARY KEY,
  name TEXT
);

CREATE TABLE IF NOT EXISTS bridge_movie_genre (
  movie_id BIGINT REFERENCES dim_movie(movie_id),
  genre_id BIGINT REFERENCES dim_genre(genre_id),
  PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE IF NOT EXISTS dim_person (
  person_id BIGINT PRIMARY KEY,
  name TEXT
);

-- staging table for safe COPY then upsert
CREATE TABLE IF NOT EXISTS stg_person (
  person_id BIGINT,
  name TEXT
);

CREATE TABLE IF NOT EXISTS fact_movie_cast (
  movie_id BIGINT REFERENCES dim_movie(movie_id),
  person_id BIGINT REFERENCES dim_person(person_id),
  character TEXT,
  cast_order INT
);

CREATE TABLE IF NOT EXISTS fact_movie_crew (
  movie_id BIGINT REFERENCES dim_movie(movie_id),
  person_id BIGINT REFERENCES dim_person(person_id),
  department TEXT,
  job TEXT
);

-- indexes for analytics speed
CREATE INDEX IF NOT EXISTS idx_movie_release_date ON dim_movie(release_date);
CREATE INDEX IF NOT EXISTS idx_cast_movie ON fact_movie_cast(movie_id);
CREATE INDEX IF NOT EXISTS idx_crew_movie ON fact_movie_crew(movie_id);
CREATE INDEX IF NOT EXISTS idx_crew_job ON fact_movie_crew(job);
