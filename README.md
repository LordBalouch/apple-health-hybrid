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
One year of data (Aug 2024 – Aug 2025): **4.2M steps total**, **11.5K steps/day** on average, across **145 workouts**.

- **A strong summer peak.** July 2025 was the standout month at ~560K steps — more than double the slower months (~240–280K). The 28-day rolling average climbed from roughly 8K/day in autumn to ~20K/day at the summer peak.
- **Weekends beat weekdays.** Saturday was the most active day (~15K avg steps), Wednesday the least (~9K). Weekends averaged about 2K more steps/day than weekdays (13K vs 11K).
- **A strength-led routine.** Workouts were built around traditional strength training, with regular yoga, bursts of martial arts, and the occasional walk or run — averaging 19 minutes per session.
- **Consistency:** the longest daily-activity streak was 8 days (late Apr–early May 2025), and the best single day hit 38K steps.


![Power BI – seasonality](dashboards/powerbi/Screenshot%202025-09-14%20212059.png)
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
```

## Tableau

Public link: https://public.tableau.com/app/profile/babak.balouch5382/viz/AppleHealthcare2025Mac/AppleHealth
**Why build both:** same dataset, different workflows — DAX vs Table Calcs (e.g., 7/28-day with `WINDOW_AVG`), star schema vs relationships, formatting & UX differences.


