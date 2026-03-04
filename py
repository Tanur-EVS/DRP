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
CREATE OR REPLACE TABLE `neat-striker-447409-t5.Training_points_tracking.wow_change` AS
WITH weekly AS (
    SELECT
        name,
        drp_id,
        week_date,
        total_points,
        LAG(total_points) OVER (
            PARTITION BY name, drp_id
            ORDER BY week_date
        ) AS last_week_points
    FROM `neat-striker-447409-t5.Training_points_tracking.employee_total_points`
)
SELECT
    name,
    drp_id,
    week_date,
    total_points                                    AS current_points,
    last_week_points,
    total_points - COALESCE(last_week_points, 0)   AS wow_diff
FROM weekly
ORDER BY week_date DESC, wow_diff DESC;
```

---

