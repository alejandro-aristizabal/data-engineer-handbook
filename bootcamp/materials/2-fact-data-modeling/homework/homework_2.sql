-- ===============================================================
-- Week 2 Fact Data Modeling — Postgres SQL Script
-- Datasets: devices, events, game_details (Day 1)
-- Replace placeholders (:target_date, :target_month) as needed
-- ===============================================================

-- ---------------------------------------------------------------
-- 1. Deduplicate game_details from Day 1
--    Keep only one row per the true key columns (e.g. game_id, user_id, event_time)
-- ---------------------------------------------------------------

-- Option A: Simple DISTINCT (if *all* columns define uniqueness)
CREATE TABLE IF NOT EXISTS game_details_dedup AS
SELECT DISTINCT *
FROM game_details;

-- Option B: Keep the latest row per composite key
-- (replace key_col1, key_col2, and timestamp_col)
CREATE TABLE IF NOT EXISTS game_details_dedup2 AS
WITH ranked AS (
  SELECT
    gd.*,
    ROW_NUMBER() OVER (
      PARTITION BY gd.key_col1, gd.key_col2
      ORDER BY gd.timestamp_col DESC
    ) AS rn
  FROM game_details gd
)
SELECT *
FROM ranked
WHERE rn = 1;


-- ---------------------------------------------------------------
-- 2. DDL for user_devices_cumulated
--    Tracks for each user and browser type the list of active dates
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS user_devices_cumulated (
  user_id                    INT     NOT NULL,
  browser_type               TEXT    NOT NULL,
  device_activity_datelist   DATE[]  NOT NULL,
  PRIMARY KEY (user_id, browser_type)
);


-- ---------------------------------------------------------------
-- 3. Cumulative query to populate user_devices_cumulated from events
-- ---------------------------------------------------------------

INSERT INTO user_devices_cumulated (user_id, browser_type, device_activity_datelist)
SELECT
  e.user_id,
  e.browser_type,
  ARRAY_AGG(DISTINCT e.event_date ORDER BY e.event_date) AS device_activity_datelist
FROM events e
GROUP BY e.user_id, e.browser_type
ON CONFLICT (user_id, browser_type) DO UPDATE
  SET device_activity_datelist = EXCLUDED.device_activity_datelist
;


-- ---------------------------------------------------------------
-- 4. Convert device_activity_datelist → datelist_int
--    (YYYYMMDD as INT)
-- ---------------------------------------------------------------

-- Add new column
ALTER TABLE user_devices_cumulated
  ADD COLUMN IF NOT EXISTS datelist_int INT[];

-- Populate it
UPDATE user_devices_cumulated ud
SET datelist_int = (
  SELECT ARRAY_AGG( (TO_CHAR(d, 'YYYYMMDD'))::INT ORDER BY d )
  FROM UNNEST(ud.device_activity_datelist) AS t(d)
);


-- ---------------------------------------------------------------
-- 5. DDL for hosts_cumulated
--    Tracks for each host the list of active dates
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS hosts_cumulated (
  host                     TEXT    NOT NULL,
  host_activity_datelist   DATE[]  NOT NULL,
  PRIMARY KEY (host)
);


-- ---------------------------------------------------------------
-- 6. Incremental query to update hosts_cumulated for a given day
--    Replace :target_date with the date you’re loading (e.g. '2025-06-14')
-- ---------------------------------------------------------------

WITH new_activity AS (
  SELECT DISTINCT host, event_date
  FROM events
  WHERE event_date = :target_date
)
-- 6.1 Update existing hosts
UPDATE hosts_cumulated hc
SET host_activity_datelist = (
  SELECT ARRAY_AGG(DISTINCT d ORDER BY d)
  FROM (
    SELECT UNNEST(hc.host_activity_datelist) AS d
    UNION ALL
    SELECT na.event_date
    FROM new_activity na
    WHERE na.host = hc.host
  ) AS combined
)
FROM new_activity na
WHERE hc.host = na.host;

-- 6.2 Insert brand-new hosts
INSERT INTO hosts_cumulated (host, host_activity_datelist)
SELECT na.host, ARRAY[na.event_date]
FROM new_activity na
LEFT JOIN hosts_cumulated hc ON hc.host = na.host
WHERE hc.host IS NULL
;


-- ---------------------------------------------------------------
-- 7. DDL for monthly reduced fact table host_activity_reduced
--    - month            (DATE: first day of month)
--    - host             (TEXT)
--    - hit_array        (INT[] of daily hit counts)
--    - unique_visitors  (INT[] of daily distinct-user counts)
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS host_activity_reduced (
  month             DATE    NOT NULL,
  host              TEXT    NOT NULL,
  hit_array         INT[]   NOT NULL,
  unique_visitors   INT[]   NOT NULL,
  PRIMARY KEY (month, host)
);


-- ---------------------------------------------------------------
-- 8. Incremental day-by-day load for host_activity_reduced
--    Replace :target_date and derive :target_month = date_trunc('month', :target_date)
-- ---------------------------------------------------------------

WITH daily_stats AS (
  SELECT
    date_trunc('month', event_date)::DATE AS month,
    host,
    COUNT(*)                   AS hits,
    COUNT(DISTINCT user_id)    AS unique_visitors
  FROM events
  WHERE event_date = :target_date
  GROUP BY 1, 2
)
-- 8.1 Update existing month-host rows
UPDATE host_activity_reduced har
SET
  hit_array        = har.hit_array        || ds.hits,
  unique_visitors  = har.unique_visitors  || ds.unique_visitors
FROM daily_stats ds
WHERE har.month = ds.month
  AND har.host  = ds.host;

-- 8.2 Insert new month-host rows
INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors)
SELECT
  ds.month,
  ds.host,
  ARRAY[ds.hits],
  ARRAY[ds.unique_visitors]
FROM daily_stats ds
LEFT JOIN host_activity_reduced har
  ON har.month = ds.month
 AND har.host  = ds.host
WHERE har.host IS NULL
;
