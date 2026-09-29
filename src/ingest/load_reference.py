# Databricks notebook source
# src/ingest/load_reference.py
# COPY INTO is idempotent: rerun only loads unseen files

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")

CAT = dbutils.widgets.get("catalog")
ENV = dbutils.widgets.get("env")

LH = f"{CAT}.{ENV}_lakehouse"
REF = f"/Volumes/{CAT}/{ENV}_landing/raw/reference"

try:
    landed = [
        f.name
        for f in dbutils.fs.ls(REF)
    ]
except Exception:
    landed = []

if not landed:
    dbutils.notebook.exit(
        "no reference files have landed yet"
    )

TABLES = {
    "ref_airport_id":
        "L_AIRPORT_ID.csv",

    "ref_unique_carriers":
        "L_UNIQUE_CARRIERS.csv",

    "ref_cancellation":
        "L_CANCELLATION.csv",

    "ref_ourairports":
        "ourairports_airports.csv",
}

for table_name, pattern in TABLES.items():

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
        {LH}.{table_name}
        """
    )

    result = spark.sql(
        f"""
        COPY INTO {LH}.{table_name}

        FROM '{REF}/'

        FILEFORMAT = CSV

        PATTERN = '{pattern}'

        FORMAT_OPTIONS (
            'header' = 'true',
            'inferSchema' = 'true',
            'mergeSchema' = 'true'
        )

        COPY_OPTIONS (
            'mergeSchema' = 'true'
        )
        """
    )

    print(
        table_name,
        result.first().asDict()
    )