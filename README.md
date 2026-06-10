# apple-health-hybrid

Reproducible analytics on a 1.8 GB Apple Health export: Python ETL → DuckDB SQL marts → Power BI & Tableau. The raw data is git-ignored; sample CSVs are included so anyone can run it.

**Python ETL • DuckDB SQL • Power BI • Tableau**

Turned a large Apple Health export (~1.8 GB `export.xml`) into a reproducible analytics project:

- **ETL (Python):** stream-parse XML → tidy **steps** & **workouts** tables
- **SQL (DuckDB):** model daily marts + **monthly**, **weekday**, **streaks**
- **BI (Power BI):** 5 pages with trends, seasonality, workout mix, and streaks
- **Tableau:** the same pages rebuilt to compare workflows and tools

> **Privacy first:** The raw Apple Health export lives in `data_private/` and is git-ignored — it is never committed. The repo includes only small **sample CSVs** in `data_sample/` so others can run the dashboards.

## Key findings


- Steps were about __% lower in winter (Nov–Feb) than in summer.
- Most active weekday: ____ · least active: ____.
- Longest daily activity streak: ___ days.

![Power BI – overview](dashboards/powerbi/Screenshot%202025-09-14%20212038.png)
![Power BI – trends](dashboards/powerbi/Screenshot%202025-09-14%20212048.png)
![Power BI – seasonality](dashboards/powerbi/Screenshot%202025-09-14%20212059.png)
![Power BI – workout mix](dashboards/powerbi/Screenshot%202025-09-14%20212122.png)
![Power BI – streaks](dashboards/powerbi/Screenshot%202025-09-14%20212132.png)

## Repo structure

```
apple-health-hybrid/
├── src/
│   ├── apple_health_etl.py   # stream-parse the 1.8 GB export.xml → tidy tables
│   ├── load_duckdb.py        # load tidy data into DuckDB
│   └── build_marts.py        # build daily / monthly / weekday / streak marts
├── data_sample/              # small sample CSVs so anyone can run it
├── data_private/             # raw export.xml (git-ignored, never committed)
├── dashboards/
│   ├── powerbi/              # Power BI screenshots
│   └── tableau/              # Tableau screenshots / notes
├── requirements.txt
└── README.md

## Tableau

Public link: [Apple Health — Tableau Public](https://public.tableau.com/views/AppleHealthcare2025Mac/AppleHealth)

**Why build both:** same dataset, different workflows — DAX vs Table Calcs (e.g., 7/28-day with `WINDOW_AVG`), star schema vs relationships, formatting & UX differences.

More notes: [docs/TABLEAU_NOTES.md](docs/TABLEAU_NOTES.md)
