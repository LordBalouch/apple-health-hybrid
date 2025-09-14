# apple-health-hybrid
Private Apple Health Data
 
**Python ETL • DuckDB SQL • Power BI (Tableau coming next)**

Turned a large Apple Health export (~1.8 GB `export.xml`) into a reproducible analytics project:
- **ETL (Python):** stream-parse XML → tidy **steps** & **workouts** tables
- **SQL (DuckDB):** model daily marts + **monthly**, **weekday**, **streaks**
- **BI (Power BI):** 5 pages with trends, seasonality, workout mix, and streaks  
- **Tableau:** Part 2 (rebuild the same pages to compare workflows)

> **Privacy first:** The raw Apple Health export lives in `data_private/` and is git-ignored. The repo includes only small **sample CSVs** in `data_sample/` so others can run the dashboards.

---

## Repo structure

