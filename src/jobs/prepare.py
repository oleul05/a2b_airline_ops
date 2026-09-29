# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# src/jobs/prepare.py — unzip newly landed BTS files and register their expected row counts
import os
import re
import shutil
import zipfile

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
CAT, ENV = dbutils.widgets.get("catalog"), dbutils.widgets.get("env")
RAW = f"/Volumes/{CAT}/{ENV}_landing/raw"
ZIP_DIR, CSV_DIR = f"{RAW}/ontime_zip", f"{RAW}/ontime_csv"
MANIFEST = f"{CAT}.{ENV}_ops.file_manifest"
PATTERN = re.compile(r"_(\d{4})_(\d{1,2})\.zip$")

dbutils.fs.mkdirs(CSV_DIR)
known = {r.landed_file for r in spark.table(MANIFEST).select("landed_file").collect()}
periods = set()
for f in sorted(dbutils.fs.ls(ZIP_DIR), key=lambda f: f.name):
    m = PATTERN.search(f.name)
    if not m or f.name in known:
        continue
    period = f"{m.group(1)}-{int(m.group(2)):02d}"
    local_zip = f"/tmp/{f.name}"
    shutil.copyfile(f"{ZIP_DIR}/{f.name}", local_zip)            # Volumes are readable as local paths
    with zipfile.ZipFile(local_zip) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        for i, member in enumerate(members, 1):
            out_name = f"ontime_{period}_{i}.csv"
            local_csv = f"/tmp/{out_name}"
            with zf.open(member) as src, open(local_csv, "wb") as dst:
                shutil.copyfileobj(src, dst, 16 * 1024 * 1024)
            with open(local_csv, "rb") as fh:
                expected = sum(1 for _ in fh) - 1                     # data rows = lines minus the header
            dbutils.fs.cp(f"file:{local_csv}", f"{CSV_DIR}/{out_name}")
            os.remove(local_csv)
            # TODO: what happens if the run dies between the copy above and the INSERT below? Make it safe.
            spark.sql("INSERT INTO IDENTIFIER(:t) VALUES ('ontime', :z, :c, :p, :n, current_timestamp())",
                      args={"t": MANIFEST, "z": f.name, "c": out_name, "p": period, "n": expected})
            print(f"{f.name} -> {out_name}: {expected:,} rows")
    os.remove(local_zip)
    periods.add(period)

dbutils.jobs.taskValues.set(key="periods", value=sorted(periods))
dbutils.jobs.taskValues.set(key="n_new", value=len(periods))