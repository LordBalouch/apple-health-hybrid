import duckdb

con = duckdb.connect('data_private/health.duckdb')

# 1) Workouts by day & type
con.execute("""
CREATE OR REPLACE TABLE ah.Workouts_ByType_Daily AS
SELECT
  DATE(start_ts)               AS date,
  COALESCE(activityType,'')    AS activityType,
  COUNT(*)                     AS workouts_ct,
  SUM(duration_min)            AS minutes_total,
  SUM(distance)                AS distance_total,
  SUM(energy_kcal)             AS energy_total
FROM ah.Workouts
GROUP BY 1,2
ORDER BY 1,2;
""")

# 2) Daily activity summary (steps + workout totals rolled to day)
con.execute("""
CREATE OR REPLACE TABLE ah.Activity_Summary_Daily AS
WITH w AS (
  SELECT
    DATE(start_ts) AS date,
    COUNT(*)       AS workouts_ct,
    SUM(duration_min)  AS workout_minutes,
    SUM(distance)      AS workout_distance,
    SUM(energy_kcal)   AS workout_energy
  FROM ah.Workouts
  GROUP BY 1
)
SELECT
  d.date,
  COALESCE(s.steps,0)                 AS steps,
  COALESCE(w.workouts_ct,0)           AS workouts_ct,
  COALESCE(w.workout_minutes,0)       AS workout_minutes,
  COALESCE(w.workout_distance,0)      AS workout_distance,
  COALESCE(w.workout_energy,0)        AS workout_energy
FROM ah.Dates d
LEFT JOIN ah.Steps_Daily s USING(date)
LEFT JOIN w USING(date)
ORDER BY d.date;
""")

# quick peek
for t in ["Workouts_ByType_Daily","Activity_Summary_Daily"]:
    print(t, "->", con.execute(f"SELECT COUNT(*) FROM ah.{t}").fetchone()[0], "rows")
print(con.execute("SELECT * FROM ah.Activity_Summary_Daily ORDER BY date DESC LIMIT 7").fetchdf())

