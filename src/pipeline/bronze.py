# src/pipeline/bronze.py — raw, as delivered, plus lineage columns
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("a2.landing")      # /Volumes/<catalog>/<env>_landing/raw
# hhmm columns and tail numbers must stay strings, or inference turns "0730" into 730
HINTS = "CRSDepTime STRING, DepTime STRING, CRSArrTime STRING, ArrTime STRING, Tail_Number STRING"


def autoload(pattern: str, fmt: str, **options):
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns"))
    for key, value in options.items():
        reader = reader.option(key, value)
    return (reader.load(f"{LANDING}/{pattern}")
            .withColumn("_source_file", F.col("_metadata.file_name"))
            .withColumn("_ingest_ts", F.current_timestamp()))


@dp.table(name="bronze_flights", comment="BTS on-time records exactly as delivered (unzipped CSV)")
def bronze_flights():
    return autoload("ontime_csv/*.csv", "csv", header="true",
                    **{"cloudFiles.inferColumnTypes": "true", "cloudFiles.schemaHints": HINTS})


@dp.table(name="bronze_weather", comment="NOAA CDO responses, one JSON document per file")
def bronze_weather():
    return autoload("weather/*.json", "json", multiLine="true")