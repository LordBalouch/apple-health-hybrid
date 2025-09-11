import os, sys, duckdb

DB = "data_private/health.duckdb"
steps_path = "data_private/steps.csv.gz"
workouts_path = "data_private/workouts.csv.gz"

# Basic checks
if not os.path.exists(steps_path) or not os.path.exists(workouts_path):
    sys.exit("Missing steps.csv.gz or workouts.csv.gz in data_private/. Run the ETL first.")

con = duckdb.connect(DB)
con.execute("PRAGMA threads=4;")

# Use schema 'ah' (Apple Health) to avoid name ambiguity with 'health'
con.execute("CREATE SCHEMA IF NOT EXISTS ah;")

# ---- Load raw CSVs to staging (strings) ----
con.execute("""
CREATE OR REPLACE TABLE ah.Steps_raw AS
SELECT 
  startDate AS startDate_str,
  endDate   AS endDate_str,
  TRY_CAST(value AS BIGINT) AS value,
  unit,
  sourceName
FROM read_csv_auto(?, header=True);
""", [steps_path])

con.execute("""
CREATE OR REPLACE TABLE ah.Workouts_raw AS
SELECT 
  activityType,
  startDate AS startDate_str,
  endDate   AS endDate_str,
  TRY_CAST(duration AS DOUBLE) AS duration_min,
  TRY_CAST(distance AS DOUBLE) AS distance,
  TRY_CAST(energy   AS DOUBLE) AS energy_kcal,
  sourceName, distanceUnit, energyUnit
FROM read_csv_auto(?, header=True);
""", [workouts_path])

# ---- Parse timestamps into proper types ----
fmt = "%Y-%m-%d %H:%M:%S %z"

con.execute(f"""
CREATE OR REPLACE TABLE ah.Steps AS
SELECT 
  TRY_STRPTIME(startDate_str, '{fmt}')::TIMESTAMPTZ AS start_ts,
  TRY_STRPTIME(endDate_str,   '{fmt}')::TIMESTAMPTZ AS end_ts,
  value, unit, sourceName
FROM ah.Steps_raw;
""")

con.execute(f"""
CREATE OR REPLACE TABLE ah.Workouts AS
SELECT 
  activityType,
  TRY_STRPTIME(startDate_str, '{fmt}')::TIMESTAMPTZ AS start_ts,
  TRY_STRPTIME(endDate_str,   '{fmt}')::TIMESTAMPTZ AS end_ts,
  duration_min, distance, energy_kcal,
  sourceName, distanceUnit, energyUnit
FROM ah.Workouts_raw;
""")

# ---- Dates dimension (min..max in Steps) ----
con.execute("""
CREATE OR REPLACE TABLE ah.Dates AS
WITH bounds AS (
  SELECT MIN(DATE(start_ts)) AS dmin,
         MAX(DATE(COALESCE(end_ts, start_ts))) AS dmax
  FROM ah.Steps
),
gen AS (
  SELECT * FROM generate_series((SELECT dmin FROM bounds),
                                (SELECT dmax FROM bounds),
                                INTERVAL 1 DAY) AS g(date)
)
SELECT 
  date,
  EXTRACT(YEAR FROM date)::INT AS year,
  EXTRACT(QUARTER FROM date)::INT AS quarter,
  EXTRACT(MONTH FROM date)::INT AS month,
  EXTRACT(DAY FROM date)::INT AS day_of_month,
  EXTRACT(ISODOW FROM date)::INT AS iso_dow,
  STRFTIME(date, '%Y-%m')     AS year_month,
  STRFTIME(date, '%Y-%m-%d')  AS ymd,
  (EXTRACT(ISODOW FROM date) IN (6,7)) AS is_weekend
FROM gen
ORDER BY date;
""")

# ---- Daily steps aggregate ----
con.execute("""
CREATE OR REPLACE TABLE ah.Steps_Daily AS
SELECT DATE(start_ts) AS date,
       SUM(value)     AS steps
FROM ah.Steps
GROUP BY 1
ORDER BY 1;
""")

# ---- Print quick counts ----
def cnt(tbl): 
    return con.execute(f"SELECT COUNT(*) FROM ah.{tbl};").fetchone()[0]

for t in ["Steps_raw","Workouts_raw","Steps","Workouts","Steps_Daily","Dates"]:
    print(f"{t:13s} -> {cnt(t)} rows")

print(f"\nDuckDB file written: {DB}")
