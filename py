CREATE OR REPLACE TABLE `neat-striker-447409-t5.Training_points_tracking.employee_total_points` AS
SELECT
    name,
    drp_id,
    week_date,
    SUM(points) AS total_points
FROM `neat-striker-447409-t5.Training_points_tracking.raw_training_data`
GROUP BY name, drp_id, week_date
ORDER BY total_points DESC;

------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `neat-striker-447409-t5.Training_points_tracking.leaderboard_top10` AS
SELECT
    rank,
    name,
    drp_id,
    total_points,
    week_date
FROM (
    SELECT
        RANK() OVER (PARTITION BY week_date ORDER BY total_points DESC) AS rank,
        name,
        drp_id,
        total_points,
        week_date
    FROM `neat-striker-447409-t5.Training_points_tracking.employee_total_points`
)
WHERE rank <= 27
ORDER BY week_date DESC, rank;

----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `neat-striker-447409-t5.Training_points_tracking.last_week_snapshot` AS
SELECT
    name,
    drp_id,
    total_points AS last_week_points,
    week_date    AS snapshot_week
FROM `neat-striker-447409-t5.Training_points_tracking.employee_total_points`
WHERE week_date = (
    SELECT MAX(week_date) 
    FROM `neat-striker-447409-t5.Training_points_tracking.employee_total_points`
)
ORDER BY last_week_points DESC;
CREATE OR REPLACE TABLE `neat-striker-447409-t5.Training_points_tracking.wow_change` AS
SELECT
    t.name,
    t.drp_id,
    t.week_date,
    t.total_points                                    AS current_points,
    l.last_week_points,
    t.total_points - COALESCE(l.last_week_points, 0) AS wow_diff
FROM `neat-striker-447409-t5.Training_points_tracking.employee_total_points` t
LEFT JOIN `neat-striker-447409-t5.Training_points_tracking.last_week_snapshot` l
    ON LOWER(TRIM(t.name)) = LOWER(TRIM(l.name))
WHERE t.week_date = (
    SELECT MAX(week_date)
    FROM `neat-striker-447409-t5.Training_points_tracking.employee_total_points`
)
ORDER BY wow_diff DESC;
```

> Added `WHERE t.week_date = MAX(week_date)` so WoW only compares the **latest week vs snapshot** — not all historical weeks.

