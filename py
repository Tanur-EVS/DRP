CREATE OR REPLACE TABLE `neat-striker-447409-t5.Training_points_tracking.employee_total_points` AS
SELECT
    name,
    drp_id,
    week_date,
    SUM(points) AS total_points
FROM `neat-striker-447409-t5.Training_points_tracking.raw_training_data`
GROUP BY name, drp_id, week_date
ORDER BY total_points DESC;

----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `neat-striker-447409-t5.Training_points_tracking.wow_change` AS
SELECT
    t.name,
    t.drp_id,
    t.week_date                                               AS current_week,
    t.total_points                                            AS current_points,
    l.last_week_points,
    t.total_points - COALESCE(l.last_week_points, 0)         AS wow_diff
FROM `neat-striker-447409-t5.Training_points_tracking.employee_total_points` t
LEFT JOIN `neat-striker-447409-t5.Training_points_tracking.last_week_snapshot` l
    ON LOWER(TRIM(t.name)) = LOWER(TRIM(l.name))
WHERE t.week_date = (
    SELECT MAX(week_date)
    FROM `neat-striker-447409-t5.Training_points_tracking.employee_total_points`
)
ORDER BY wow_diff DESC;
-------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `neat-striker-447409-t5.Training_points_tracking.last_week_snapshot` AS
SELECT
    name,
    drp_id,
    total_points AS last_week_points
FROM `neat-striker-447409-t5.Training_points_tracking.employee_total_points`
WHERE week_date = (
    SELECT MAX(week_date)
    FROM `neat-striker-447409-t5.Training_points_tracking.employee_total_points`
)
ORDER BY last_week_points DESC;
```

> Run this **after** `wow_change` — so this week becomes next week's baseline automatically.

---

## Step 3 — Looker Studio Dashboard Setup

### Page 1 — Total Points / Leaderboard

**Data source:** `employee_total_points`

**Chart 1 — Table (main view)**
- Chart type: **Table**
- Dimensions: `name`, `drp_id`
- Metric: `total_points`
- Sort: `total_points` Descending
- Enable: **Row numbers** ON → acts as rank automatically

**Chart 2 — Scorecard (top employee)**
- Chart type: **Scorecard**
- Metric: `name` → show value of employee with MAX `total_points`
- Label: "Current Leader"

**Filter — Week selector**
- Control type: **Drop-down**
- Field: `week_date`

---

### Page 2 — Week Over Week Change

**Data source:** `wow_change`

**Chart — Table (WoW comparison)**
- Chart type: **Table**
- Dimensions: `name`, `drp_id`
- Metrics: `current_points`, `last_week_points`, `wow_diff`
- Sort: `wow_diff` Descending

**Conditional formatting on `wow_diff`:**
1. Click the table → Style tab
2. Enable **Conditional formatting**
3. Add rules:
   - `wow_diff > 0` → text color **green** 🟢
   - `wow_diff < 0` → text color **red** 🔴
   - `wow_diff = 0` → text color **grey** ⚪

---

## Final Dashboard Structure
```
DRP Training Points Dashboard
    │
    ├── Page 1: Leaderboard
    │       ├── Week date filter
    │       └── Table (name, drp_id, total_points) sorted descending
    │
    └── Page 2: Week Over Week Change
            └── Table (name, drp_id, current_points, last_week_points, wow_diff)
                       green ↑ / red ↓ on wow_diff
```

---

## Your Weekly Process Going Forward
```
1. Upload new Excel → GCS
2. Cloud Function auto-loads → raw_training_data
3. Run Query 1 → employee_total_points refreshes
4. Run Query 2 → wow_change refreshes
5. Run Query 3 → last_week_snapshot updates for next week
6. Looker Studio dashboard reflects new data instantly
