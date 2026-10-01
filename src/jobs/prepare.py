# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# src/jobs/prepare.py
# Unzip newly landed BTS files directly inside Unity Catalog Volumes
# and register their expected row counts.

import re
import zipfile

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")

CAT = dbutils.widgets.get("catalog")
ENV = dbutils.widgets.get("env")

RAW = f"/Volumes/{CAT}/{ENV}_landing/raw"
ZIP_DIR = f"{RAW}/ontime_zip"
CSV_DIR = f"{RAW}/ontime_csv"
MANIFEST = f"{CAT}.{ENV}_ops.file_manifest"

PATTERN = re.compile(r"_(\d{4})_(\d{1,2})\.zip$")

# Make sure destination exists
dbutils.fs.mkdirs(CSV_DIR)

# Files already registered in the manifest
known = {
    r.landed_file
    for r in spark.table(MANIFEST)
                  .select("landed_file")
                  .collect()
}

periods = set()

for f in sorted(dbutils.fs.ls(ZIP_DIR), key=lambda x: x.name):

    m = PATTERN.search(f.name)

    if not m:
        print(f"Skipping unexpected file: {f.name}")
        continue

    if f.name in known:
        print(f"Already registered, skipping: {f.name}")
        continue

    period = f"{m.group(1)}-{int(m.group(2)):02d}"

    # Unity Catalog Volume can be used directly with Python file APIs
    zip_path = f"{ZIP_DIR}/{f.name}"

    print(f"Processing {f.name}")

    with zipfile.ZipFile(zip_path, "r") as zf:

        members = [
            name
            for name in zf.namelist()
            if name.lower().endswith(".csv")
        ]

        if not members:
            raise ValueError(
                f"No CSV file found inside {f.name}"
            )

        for i, member in enumerate(members, 1):

            out_name = f"ontime_{period}_{i}.csv"
            out_path = f"{CSV_DIR}/{out_name}"

            # Write extracted CSV directly into the Volume
            with zf.open(member, "r") as src, \
                 open(out_path, "wb") as dst:

                while True:
                    chunk = src.read(16 * 1024 * 1024)

                    if not chunk:
                        break

                    dst.write(chunk)

            # Count rows: total lines minus header
            with open(out_path, "rb") as fh:
                expected = sum(1 for _ in fh) - 1

            # Register file in manifest
            # Register file in manifest idempotently
            spark.sql(
                """
                MERGE INTO IDENTIFIER(:t) AS target

                USING (
                    SELECT
                        'ontime' AS dataset,
                        :z AS landed_file,
                        :c AS unpacked_file,
                        :p AS period,
                        :n AS expected_rows
                ) AS source

                ON target.landed_file = source.landed_file
                   AND target.unpacked_file = source.unpacked_file

                WHEN NOT MATCHED THEN
                  INSERT (
                      dataset,
                      landed_file,
                      unpacked_file,
                      period,
                      expected_rows,
                      registered_at
                  )
                  VALUES (
                      source.dataset,
                      source.landed_file,
                      source.unpacked_file,
                      source.period,
                      source.expected_rows,
                      current_timestamp()
                  )
                """,
                args={
                    "t": MANIFEST,
                    "z": f.name,
                    "c": out_name,
                    "p": period,
                    "n": expected,
                },
            )

            print(
                f"{f.name} -> {out_name}: "
                f"{expected:,} rows"
            )

    periods.add(period)


print(f"New periods: {sorted(periods)}")
print(f"Number of new periods: {len(periods)}")


# These values are required later when prepare.py runs as a Lakeflow Job task.
# During today's manual Task 1.3 run, the notebook may not have a Jobs task context.
try:
    dbutils.jobs.taskValues.set(
        key="periods",
        value=sorted(periods)
    )

    dbutils.jobs.taskValues.set(
        key="n_new",
        value=len(periods)
    )

except Exception as exc:
    print(
        "Task values skipped because this is a manual notebook run:",
        exc
    )