# src/pipeline/silver.py — conform, validate, quarantine, de-duplicate
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, functions as F

LAKEHOUSE = spark.conf.get("a2.lakehouse")

RAW_DROP_RULES = {
    "period_matches_file": "period = file_period",
    "flags_present": "cancelled IS NOT NULL AND diverted IS NOT NULL",
    "origin_ne_dest": "origin <> dest",
    "distance_positive": "distance_mi > 0",
    "crs_time_valid": "try_cast(crs_dep_hhmm AS INT) BETWEEN 0 AND 2400",
}
DROP_RULES = {k: f"coalesce({v}, false)" for k, v in RAW_DROP_RULES.items()}   # a NULL fails the rule
WARN_RULES = {
    "cancel_code_when_cancelled": "NOT cancelled OR cancellation_code IS NOT NULL",
    "arr_delay_when_completed": "NOT is_completed OR arr_delay_min IS NOT NULL",
}
ALL_DROP = " AND ".join(DROP_RULES.values())
KEY_COLS = ["flight_date", "carrier_code", "flight_number", "origin", "crs_dep_hhmm"]
CAUSES = [("CarrierDelay", "carrier_delay"), ("WeatherDelay", "weather_delay"), ("NASDelay", "nas_delay"),
          ("SecurityDelay", "security_delay"), ("LateAircraftDelay", "late_aircraft_delay")]


def hhmm(col: str):
    return F.lpad(F.col(col).cast("string"), 4, "0")


def local_ts(date_col: str, hhmm_col: str):
    """Scheduled local time. BTS writes midnight as 2400, which belongs to the next day."""
    is_2400 = F.col(hhmm_col) == "2400"
    day = F.when(is_2400, F.date_add(F.col(date_col), 1)).otherwise(F.col(date_col))
    clock = F.when(is_2400, F.lit("0000")).otherwise(F.col(hhmm_col))
    return F.try_to_timestamp(F.concat_ws(" ", day.cast("string"), clock), F.lit("yyyy-MM-dd HHmm"))


def conform(df: DataFrame) -> DataFrame:
    return df.select(
        F.to_date("FlightDate").alias("flight_date"),
        F.col("Reporting_Airline").alias("carrier_code"),
        F.col("DOT_ID_Reporting_Airline").cast("int").alias("dot_carrier_id"),
        F.col("Flight_Number_Reporting_Airline").cast("string").alias("flight_number"),
        F.col("Tail_Number").alias("tail_number"),
        F.col("Origin").alias("origin"),
        F.col("Dest").alias("dest"),
        F.col("OriginAirportID").cast("int").alias("origin_airport_id"),
        F.col("DestAirportID").cast("int").alias("dest_airport_id"),
        hhmm("CRSDepTime").alias("crs_dep_hhmm"),
        hhmm("DepTime").alias("dep_hhmm"),
        hhmm("CRSArrTime").alias("crs_arr_hhmm"),
        hhmm("ArrTime").alias("arr_hhmm"),
        F.col("DepDelay").cast("double").alias("dep_delay_min"),
        F.col("ArrDelay").cast("double").alias("arr_delay_min"),
        F.col("DepDel15").cast("int").alias("dep_del15"),
        F.col("ArrDel15").cast("int").alias("arr_del15"),
        (F.col("Cancelled").cast("double") == 1).alias("cancelled"),
        (F.col("Diverted").cast("double") == 1).alias("diverted"),
        F.col("CancellationCode").alias("cancellation_code"),
        F.col("Distance").cast("double").alias("distance_mi"),
        *[F.col(src).cast("double").alias(dst) for src, dst in CAUSES],
        F.col("_source_file"),
    )


def add_derived(df: DataFrame) -> DataFrame:
    return (df
            .withColumn("sched_dep_ts", local_ts("flight_date", "crs_dep_hhmm"))
            .withColumn("dep_hour", F.expr("try_cast(substr(crs_dep_hhmm, 1, 2) AS INT) % 24"))
            .withColumn("period", F.date_format("flight_date", "yyyy-MM"))
            .withColumn("file_period", F.regexp_extract("_source_file", r"ontime_(\d{4}-\d{2})_", 1))
            .withColumn("is_completed", ~F.col("cancelled") & ~F.col("diverted"))
            .withColumn("route", F.concat_ws("-", "origin", "dest"))
            .withColumn("_row_key", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in KEY_COLS]), 256)))


@dp.temporary_view()
def v_flights_conformed():
    return add_derived(conform(spark.readStream.table("bronze_flights")))


@dp.table(name="silver_flights_clean", cluster_by=["flight_date"], comment="Flights that pass every drop rule")
@dp.expect_or_fail("has_source_file", "_source_file IS NOT NULL")
@dp.expect_all(WARN_RULES)
@dp.expect_all_or_drop(DROP_RULES)
def silver_flights_clean():
    return spark.readStream.table("v_flights_conformed")


@dp.table(name="silver_flights_quarantine", comment="Flights that failed at least one drop rule")
def silver_flights_quarantine():
    return spark.readStream.table("v_flights_conformed").where(f"NOT ({ALL_DROP})")


@dp.materialized_view(name="silver_flights", cluster_by=["flight_date", "origin"],
                      comment="Conformed flights, de-duplicated on _row_key")
def silver_flights():
    return spark.read.table("silver_flights_clean").dropDuplicates(["_row_key"])


@dp.materialized_view(name="dim_carrier", comment="Carrier names from the BTS lookup (check its column names)")
def dim_carrier():
    return (spark.read.table(f"{LAKEHOUSE}.ref_unique_carriers")
            .select(F.col("Code").alias("carrier_code"), F.col("Description").alias("carrier_name")))


@dp.materialized_view(name="dim_airport", comment="Airports by IATA code, from OurAirports")
def dim_airport():
    return (spark.read.table(f"{LAKEHOUSE}.ref_ourairports")
            .where(F.col("iata_code").isNotNull() & F.col("iso_country").isin("US", "PR", "VI", "GU", "AS", "MP"))
            .select(F.col("iata_code").alias("airport_code"), F.col("name").alias("airport_name"),
                    F.col("municipality").alias("city"), F.col("iso_region").alias("region"),
                    F.col("latitude_deg").cast("double").alias("lat"),
                    F.col("longitude_deg").cast("double").alias("lon"))
            .dropDuplicates(["airport_code"]))


@dp.materialized_view(name="silver_weather_daily", comment="One row per station and day; the latest fetch wins")
def silver_weather_daily():
    obs = (spark.read.table("bronze_weather")
           .select("fetched_at", F.explode("results").alias("r"))
           .select("fetched_at",
                   F.to_date(F.substring("r.date", 1, 10)).alias("obs_date"),
                   F.regexp_replace("r.station", "^GHCND:", "").alias("station_id"),
                   F.col("r.datatype").alias("datatype"),
                   F.col("r.value").cast("double").alias("value")))
    # Each scheduled fetch lands a new file: keep the most recently fetched value per station, day and element.
    latest = obs.groupBy("obs_date", "station_id", "datatype").agg(F.max_by("value", "fetched_at").alias("value"))
    # Pipelines do not support .pivot(); conditional aggregation does the same job.
    return (latest.groupBy("obs_date", "station_id")
                  .agg(*[F.max(F.when(F.col("datatype") == t, F.col("value"))).alias(t.lower())
                         for t in ["PRCP", "SNOW", "TMAX", "TMIN"]]))