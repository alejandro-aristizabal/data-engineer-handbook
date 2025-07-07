CREATE TYPE scoring_class AS ENUM (
    'start',
    'good',
    'average',
    'bad'
);

CREATE TABLE players_scd (
    player_name TEXT,
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY(player_name, current_season)
)

WITH with_previous AS (
    SELECT
        player_name,
        scoring_class,
        is_active,
        current_season,
        LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
        LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active,
    FROM players
),
with_indicators AS (
    SELECT
        *,
        CASE
            WHEN scoring_class != previous_scoring_class OR is_active != previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
WITH with_streaks AS (
    SELECT
        player_name,
        is_active,
        scoring_class,
        current_season,
        change_indicator,
        SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
    FROM with_indicators
)

SELECT
    player_name,
    is_active,
    scoring_class,
    MIN(current_season) AS start_season,
    MAX(current_season) AS end_season,
FROM with_streaks
GROUP BY
    player_name,
    is_active,
    scoring_class,
    streak_identifier
ORDER BY
    player_name,
    start_season;