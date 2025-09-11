#!/usr/bin/env python3
"""
Apple Health ETL (streaming) — export.xml -> steps.csv & workouts.csv

Usage
-----
# Basic (writes to current dir)
python apple_health_etl.py /path/to/export.xml --steps steps.csv --workouts workouts.csv

# With output directory
python apple_health_etl.py /path/to/export.xml --outdir data

# Gzipped CSVs
python apple_health_etl.py /path/to/export.xml --outdir data --steps steps.csv.gz --workouts workouts.csv.gz

Notes
-----
- Streams with xml.etree.ElementTree.iterparse (safe for 1.8 GB+).
- Outputs:
  steps:    startDate, endDate, value, unit, sourceName
  workouts: activityType, startDate, endDate, duration, distance, energy, sourceName, distanceUnit, energyUnit
- Times are kept as Apple local strings; we’ll normalize in SQL.
"""

import argparse
import csv
import gzip
import io
import os
import re
import sys
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Tuple

def _open_maybe_gzip(path: str, mode: str):
    """Open path, using gzip if filename ends with .gz."""
    if path.endswith(".gz"):
        if "t" in mode:
            return io.TextIOWrapper(gzip.open(path, mode.replace("t", "b")), encoding="utf-8", newline="")
        return gzip.open(path, mode)
    if "t" in mode:
        return open(path, mode, encoding="utf-8", newline="")
    return open(path, mode)

def _strip_activity_prefix(activity: Optional[str]) -> Optional[str]:
    """Turn 'HKWorkoutActivityTypeRunning' -> 'Running'."""
    if not activity:
        return activity
    return re.sub(r"^HKWorkoutActivityType", "", activity)

def _safe_get(el: ET.Element, key: str) -> str:
    return el.attrib.get(key, "")

def _write_headers_if_empty(writer: csv.writer, headers_written: Dict[str, bool], key: str, headers: Tuple[str, ...]):
    if not headers_written.get(key):
        writer.writerow(headers)
        headers_written[key] = True

def parse_export(
    xml_path: str,
    steps_path: Optional[str] = None,
    workouts_path: Optional[str] = None,
    verbose_every: int = 250_000,
) -> Dict[str, int]:
    """Stream-parse the Apple Health export and write selected tables."""
    want_steps = steps_path is not None
    want_workouts = workouts_path is not None
    if not (want_steps or want_workouts):
        raise ValueError("Provide --steps and/or --workouts output paths.")

    os.makedirs(os.path.dirname(os.path.abspath(steps_path or workouts_path)), exist_ok=True)
    steps_file = _open_maybe_gzip(steps_path, "wt") if want_steps else None
    workouts_file = _open_maybe_gzip(workouts_path, "wt") if want_workouts else None
    steps_writer = csv.writer(steps_file) if steps_file else None
    workouts_writer = csv.writer(workouts_file) if workouts_file else None

    headers_written: Dict[str, bool] = {}
    counts = {"records": 0, "steps": 0, "workouts": 0}

    steps_headers = ("startDate", "endDate", "value", "unit", "sourceName")
    workout_headers = ("activityType", "startDate", "endDate", "duration", "distance", "energy", "sourceName", "distanceUnit", "energyUnit")

    context = ET.iterparse(xml_path, events=("end",))
    for event, elem in context:
        tag = elem.tag

        if want_steps and tag == "Record":
            rectype = _safe_get(elem, "type")
            if rectype == "HKQuantityTypeIdentifierStepCount":
                _write_headers_if_empty(steps_writer, headers_written, "steps", steps_headers)
                steps_writer.writerow([
                    _safe_get(elem, "startDate"),
                    _safe_get(elem, "endDate"),
                    _safe_get(elem, "value"),
                    _safe_get(elem, "unit"),
                    _safe_get(elem, "sourceName"),
                ])
                counts["steps"] += 1
            counts["records"] += 1

        elif want_workouts and tag == "Workout":
            _write_headers_if_empty(workouts_writer, headers_written, "workouts", workout_headers)
            workouts_writer.writerow([
                _strip_activity_prefix(_safe_get(elem, "workoutActivityType")),
                _safe_get(elem, "startDate"),
                _safe_get(elem, "endDate"),
                _safe_get(elem, "duration"),
                _safe_get(elem, "totalDistance"),
                _safe_get(elem, "totalEnergyBurned"),
                _safe_get(elem, "sourceName"),
                _safe_get(elem, "totalDistanceUnit"),
                _safe_get(elem, "totalEnergyBurnedUnit"),
            ])
            counts["workouts"] += 1

        elem.clear()

        if verbose_every and counts["records"] and (counts["records"] % verbose_every == 0):
            print(f"...parsed {counts['records']:,} <Record> elements; steps so far: {counts['steps']:,}")

    if steps_file: steps_file.close()
    if workouts_file: workouts_file.close()
    return counts

def main():
    ap = argparse.ArgumentParser(description="Stream-parse Apple Health export.xml into clean CSV tables.")
    ap.add_argument("xml_path", help="Path to Apple Health export.xml")
    ap.add_argument("--outdir", default=".", help="Directory to write outputs into (default: current dir)")
    ap.add_argument("--steps", default="steps.csv", help="Filename for steps table (use .gz to gzip; empty to skip)")
    ap.add_argument("--workouts", default="workouts.csv", help="Filename for workouts table (use .gz to gzip; empty to skip)")
    ap.add_argument("--quiet", action="store_true", help="Reduce console logging")
    args = ap.parse_args()

    steps_path = None if args.steps.strip() == "" else os.path.join(args.outdir, args.steps)
    workouts_path = None if args.workouts.strip() == "" else os.path.join(args.outdir, args.workouts)

    counts = parse_export(args.xml_path, steps_path=steps_path, workouts_path=workouts_path)

    if not args.quiet:
        print("Done.")
        for k, v in counts.items():
            print(f"{k}: {v:,}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except BrokenPipeError:
        sys.exit(0)
