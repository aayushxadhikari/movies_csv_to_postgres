import pandas as pd
from etl.utils import parse_list, logger, stable_id_from_name, is_jsonish

def transform_movies(df):
    # ---------- movies ----------
    movies = df[[
        "id", "title", "original_language", "release_date",
        "budget", "revenue", "runtime", "vote_average", "vote_count"
    ]].copy()

    movies.columns = [
        "movie_id", "title", "original_language", "release_date",
        "budget", "revenue", "runtime", "vote_average", "vote_count"
    ]

    movies["release_date"] = pd.to_datetime(movies["release_date"], errors="coerce").dt.date

    # Convert numeric columns safely 
    movies["runtime"] = pd.to_numeric(movies["runtime"], errors="coerce").fillna(0).astype(int)
    movies["budget"] = pd.to_numeric(movies["budget"], errors="coerce").fillna(0).astype(int)
    movies["revenue"] = pd.to_numeric(movies["revenue"], errors="coerce").fillna(0).astype(int)
    movies["vote_count"] = pd.to_numeric(movies["vote_count"], errors="coerce").fillna(0).astype(int)
    movies["vote_average"] = pd.to_numeric(movies["vote_average"], errors="coerce").fillna(0.0)

    # ---------- genres + bridge ----------
    genres, movie_genres = [], []

    # ---------- people + cast/crew ----------
    people, cast_rows, crew_rows = [], [], []

    for _, row in df.iterrows():
        movie_id = int(row["id"])

        # ---- genres: support both JSON and plain text like "Romance Drama" ---
        genres_val = row.get("genres")
        if isinstance(genres_val, str) and not is_jsonish(genres_val):
            # Plain text genres
            for gname in genres_val.split():
                gid = stable_id_from_name(gname)
                genres.append((gid, gname))
                movie_genres.append((movie_id, gid))
        else:
            # JSON list of dicts
            for g in parse_list(genres_val):
                if isinstance(g, dict) and "id" in g and "name" in g:
                    gid = int(g["id"])
                    genres.append((gid, g["name"]))
                    movie_genres.append((movie_id, gid))

        # ---- cast: only parse if JSON. If plain text names, SKIP (names split badly).
        cast_val = row.get("cast")
        if isinstance(cast_val, str) and not is_jsonish(cast_val):
            pass
        else:
            for c in parse_list(cast_val):
                if isinstance(c, dict) and "id" in c and "name" in c:
                    person_id = int(c["id"])
                    people.append((person_id, c["name"]))
                    cast_rows.append((
                        movie_id,
                        person_id,
                        c.get("character"),
                        c.get("order")
                    ))

        # ---- crew: parse JSON if present; if plain text skip
        crew_val = row.get("crew")
        if isinstance(crew_val, str) and not is_jsonish(crew_val):
            pass
        else:
            for cr in parse_list(crew_val):
                if isinstance(cr, dict) and "id" in cr and "name" in cr:
                    person_id = int(cr["id"])
                    people.append((person_id, cr["name"]))
                    crew_rows.append((
                        movie_id,
                        person_id,
                        cr.get("department"),
                        cr.get("job")
                    ))

    genres_df = pd.DataFrame(genres, columns=["genre_id", "name"]).drop_duplicates()
    movie_genres_df = pd.DataFrame(movie_genres, columns=["movie_id", "genre_id"]).drop_duplicates()

    people_df = pd.DataFrame(people, columns=["person_id", "name"]).drop_duplicates()
    cast_df = pd.DataFrame(cast_rows, columns=["movie_id", "person_id", "character", "cast_order"])
    crew_df = pd.DataFrame(crew_rows, columns=["movie_id", "person_id", "department", "job"])

    movies = movies.drop_duplicates(subset=["movie_id"])

    logger.info(
        f"Transformed: movies={len(movies)}, genres={len(genres_df)}, "
        f"movie_genres={len(movie_genres_df)}, people={len(people_df)}, "
        f"cast={len(cast_df)}, crew={len(crew_df)}"
    )

    return {
        "movies": movies,
        "genres": genres_df,
        "movie_genres": movie_genres_df,
        "people": people_df,
        "cast": cast_df,
        "crew": crew_df,
    }
