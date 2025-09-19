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

## Tableau (Part 2)

Public link: [Apple Health — Tableau Public](https://public.tableau.com/views/AppleHealthcare2025Mac/AppleHealth?:language=en-GB&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)

Screens (from Tableau Public):
![Overview](dashboards/tableau/overview.png)
![Workouts](dashboards/tableau/workouts.png)
![Seasonality](dashboards/tableau/seasonality.png)
![Weekday](dashboards/tableau/weekday.png)
![Streaks](dashboards/tableau/streaks.png)

**Why build both:** same dataset, different workflows — DAX vs Table Calcs (e.g., 7/28-day with `WINDOW_AVG`), star schema vs relationships, formatting & UX differences.

More notes: [docs/TABLEAU_NOTES.md](docs/TABLEAU_NOTES.md)
