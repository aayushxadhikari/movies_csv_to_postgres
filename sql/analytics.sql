-- Top 10 movies by revenue
SELECT title, revenue
FROM dim_movie
ORDER BY revenue DESC
LIMIT 10;

-- Revenue by genre
SELECT g.name, SUM(m.revenue) AS total_revenue
FROM dim_movie m
JOIN bridge_movie_genre mg ON m.movie_id = mg.movie_id
JOIN dim_genre g ON g.genre_id = mg.genre_id
GROUP BY g.name
ORDER BY total_revenue DESC;

-- Top actors by movie count
SELECT p.name, COUNT(DISTINCT c.movie_id) AS movies
FROM fact_movie_cast c
JOIN dim_person p ON p.person_id = c.person_id
GROUP BY p.name
ORDER BY movies DESC
LIMIT 20;

-- Most common crew jobs
SELECT job, COUNT(*) AS total
FROM fact_movie_crew
GROUP BY job
ORDER BY total DESC
LIMIT 20;
