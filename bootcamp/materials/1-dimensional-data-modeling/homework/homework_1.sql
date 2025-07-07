-- ===============================================================
-- Postgres SQL script for Actor Films dimensional modeling (SCD2)
-- Replace [YEAR] with the target year for “one-year” and incremental loads
-- ===============================================================

-- ===============================================================
-- 1. DDL: define composite type and tables
-- ===============================================================

-- 1.1 Composite type for the films array
CREATE TYPE IF NOT EXISTS film_struct AS (
  film    TEXT,
  votes   INT,
  rating  REAL,
  filmid  INT
);

-- 1.2 Actors “dimension” table
CREATE TABLE IF NOT EXISTS actors (
  actor          TEXT       NOT NULL,
  actorid        INT        NOT NULL,
  films          film_struct[]  NOT NULL,
  quality_class  TEXT       NOT NULL,
  is_active      BOOLEAN    NOT NULL,
  PRIMARY KEY(actorid)
);

-- 1.3 Actors history SCD2 table
CREATE TABLE IF NOT EXISTS actors_history_scd (
  actor          TEXT       NOT NULL,
  actorid        INT        NOT NULL,
  quality_class  TEXT       NOT NULL,
  is_active      BOOLEAN    NOT NULL,
  start_date     DATE       NOT NULL,
  end_date       DATE       NOT NULL,
  current_flag   BOOLEAN    NOT NULL,
  PRIMARY KEY(actorid, start_date)
);



-- ===============================================================
-- 2. Cumulative load of actors table for one year at a time
-- ===============================================================

BEGIN;

WITH
  actor_summary AS (
    SELECT
      af.actor,
      af.actorid,
      AVG(af.rating)::NUMERIC(5,2) AS avg_rating,
      CASE
        WHEN AVG(af.rating) >  8 THEN 'star'
        WHEN AVG(af.rating) >  7 THEN 'good'
        WHEN AVG(af.rating) >  6 THEN 'average'
        ELSE 'bad'
      END                                AS quality_class,
      ([YEAR] = EXTRACT(YEAR FROM CURRENT_DATE)) AS is_active
    FROM actor_films af
    WHERE af.year = [YEAR]
    GROUP BY af.actor, af.actorid
  ),
  films_agg AS (
    SELECT
      af.actorid,
      ARRAY_AGG(ROW(af.film, af.votes, af.rating, af.filmid)::film_struct) AS films
    FROM actor_films af
    WHERE af.year = [YEAR]
    GROUP BY af.actorid
  )
-- Insert or update actors dimension
INSERT INTO actors (actor, actorid, films, quality_class, is_active)
SELECT
  s.actor,
  s.actorid,
  f.films,
  s.quality_class,
  s.is_active
FROM actor_summary s
JOIN films_agg     f USING(actorid)
ON CONFLICT (actorid) DO UPDATE
  SET films         = EXCLUDED.films,
      quality_class = EXCLUDED.quality_class,
      is_active     = EXCLUDED.is_active;

COMMIT;


-- ===============================================================
-- 3. Backfill entire actors_history_scd table in one query
-- ===============================================================

BEGIN;

WITH
  actor_year_stats AS (
    SELECT
      actor,
      actorid,
      year,
      CASE
        WHEN AVG(rating) >  8 THEN 'star'
        WHEN AVG(rating) >  7 THEN 'good'
        WHEN AVG(rating) >  6 THEN 'average'
        ELSE 'bad'
      END                              AS quality_class,
      (year = EXTRACT(YEAR FROM CURRENT_DATE)) AS is_active
    FROM actor_films
    GROUP BY actor, actorid, year
  ),
  year_bounds AS (
    SELECT
      MIN(year) AS min_year,
      MAX(year) AS max_year
    FROM actor_year_stats
  )
INSERT INTO actors_history_scd (
  actor,
  actorid,
  quality_class,
  is_active,
  start_date,
  end_date,
  current_flag
)
SELECT
  ays.actor,
  ays.actorid,
  ays.quality_class,
  ays.is_active,
  MAKE_DATE(ays.year,   1, 1)       AS start_date,
  MAKE_DATE(ays.year+1, 1, 1)       AS end_date,
  (ays.year = yb.max_year)          AS current_flag
FROM actor_year_stats ays
CROSS JOIN year_bounds yb;

COMMIT;


-- ===============================================================
-- 4. Incremental SCD2 load for new year [YEAR]
-- ===============================================================

BEGIN;

-- 4.1 Compute new-year stats
WITH new_year_stats AS (
  SELECT
    actor,
    actorid,
    CASE
      WHEN AVG(rating) >  8 THEN 'star'
      WHEN AVG(rating) >  7 THEN 'good'
      WHEN AVG(rating) >  6 THEN 'average'
      ELSE 'bad'
    END                              AS quality_class,
    ([YEAR] = EXTRACT(YEAR FROM CURRENT_DATE)) AS is_active,
    MAKE_DATE([YEAR],   1, 1)       AS start_date,
    MAKE_DATE([YEAR]+1, 1, 1)       AS end_date
  FROM actor_films
  WHERE year = [YEAR]
  GROUP BY actor, actorid
)

-- 4.2 Expire existing records if something changed
UPDATE actors_history_scd ah
SET
  end_date     = n.start_date,
  current_flag = FALSE
FROM new_year_stats n
WHERE ah.actorid     = n.actorid
  AND ah.current_flag = TRUE
  AND (ah.quality_class <> n.quality_class
    OR ah.is_active     <> n.is_active);

-- 4.3 Insert new SCD2 rows
INSERT INTO actors_history_scd (
  actor,
  actorid,
  quality_class,
  is_active,
  start_date,
  end_date,
  current_flag
)
SELECT
  n.actor,
  n.actorid,
  n.quality_class,
  n.is_active,
  n.start_date,
  n.end_date,
  TRUE
FROM new_year_stats n
LEFT JOIN actors_history_scd ah
  ON ah.actorid = n.actorid
 AND ah.start_date = n.start_date
WHERE ah.actorid IS NULL;

COMMIT;
