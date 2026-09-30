# Databricks notebook source
# src/ingest/weather_rest.py

import datetime as dt
import json
import time
import requests

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("start_date", "2018-01-01")
dbutils.widgets.text("end_date", "2025-12-31")
dbutils.widgets.text("stations", "")

CAT = dbutils.widgets.get("catalog")
ENV = dbutils.widgets.get("env")

OUT = f"/Volumes/{CAT}/{ENV}_landing/raw/weather"
BASE = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"

TOKEN = dbutils.secrets.get(
    scope="a2",
    key="noaa_token"
)


def year_windows(start: str, end: str):
    s = dt.date.fromisoformat(start)
    e = dt.date.fromisoformat(end)

    while s <= e:
        w_end = min(
            e,
            dt.date(s.year, 12, 31)
        )

        yield s, w_end

        s = w_end + dt.timedelta(days=1)


def get_page(params: dict, attempts: int = 5) -> dict:

    for attempt in range(1, attempts + 1):

        r = requests.get(
            BASE,
            headers={"token": TOKEN},
            params=params,
            timeout=60
        )

        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(2 ** attempt)
            continue

        r.raise_for_status()

        return r.json() or {}

    raise RuntimeError(
        f"CDO kept failing for {params}"
    )


dbutils.fs.mkdirs(OUT)

stations = [
    s.strip()
    for s in dbutils.widgets.get("stations").split(",")
    if s.strip()
]

if not stations:

    stations = [
        r.station_id
        for r in spark.table(
            f"{CAT}.{ENV}_lakehouse.ref_airport_station"
        ).collect()
    ]

stamp = dt.datetime.now(
    dt.timezone.utc
).strftime("%Y%m%dT%H%M%SZ")


for station in stations:

    for s, e in year_windows(
        dbutils.widgets.get("start_date"),
        dbutils.widgets.get("end_date")
    ):

        rows = []
        offset = 1

        while True:

            body = get_page({
                "datasetid": "GHCND",
                "stationid": station,
                "datatypeid": [
                    "PRCP",
                    "SNOW",
                    "TMAX",
                    "TMIN"
                ],
                "startdate": s.isoformat(),
                "enddate": e.isoformat(),
                "units": "metric",
                "limit": 1000,
                "offset": offset
            })

            rows += body.get("results", [])

            total = (
                body.get("metadata", {})
                    .get("resultset", {})
                    .get("count", 0)
            )

            offset += 1000

            time.sleep(0.25)

            if offset > total:
                break

        name = (
            f"{station.replace(':', '_')}_"
            f"{s:%Y%m%d}_"
            f"{e:%Y%m%d}_"
            f"{stamp}.json"
        )

        doc = {
            "station": station,
            "start": s.isoformat(),
            "end": e.isoformat(),
            "fetched_at": dt.datetime.now(
                dt.timezone.utc
            ).isoformat(),
            "results": rows
        }

        dbutils.fs.put(
            f"{OUT}/{name}",
            json.dumps(doc),
            overwrite=False
        )

        print(
            f"{name}: {len(rows)} observations"
        )