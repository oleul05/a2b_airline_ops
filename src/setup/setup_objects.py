# Databricks notebook source
# src/setup/setup_objects.py — idempotent; the setup job runs it on every target

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")

CAT = dbutils.widgets.get("catalog")
ENV = dbutils.widgets.get("env")
OPS = f"{CAT}.{ENV}_ops"

# Create schemas
for layer in ("landing", "lakehouse", "gold", "ops"):
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {CAT}.{ENV}_{layer}"
    )

# Create landing volume
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CAT}.{ENV}_landing.raw"
)

# Create landing folders
for folder in [
    "ontime_zip",
    "ontime_csv",
    "reference",
    "weather"
]:
    dbutils.fs.mkdirs(
        f"/Volumes/{CAT}/{ENV}_landing/raw/{folder}"
    )

# Operational tables
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OPS}.file_manifest (
    dataset STRING,
    landed_file STRING,
    unpacked_file STRING,
    period STRING,
    expected_rows BIGINT,
    registered_at TIMESTAMP
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OPS}.reconciliation (
    run_id STRING,
    period STRING,
    dataset STRING,
    expected_rows BIGINT,
    bronze_rows BIGINT,
    silver_rows BIGINT,
    quarantined_rows BIGINT,
    status STRING,
    checked_at TIMESTAMP
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OPS}.release (
    run_id STRING,
    periods STRING,
    certified_at TIMESTAMP,
    notes STRING
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OPS}.incidents (
    run_id STRING,
    reason STRING,
    raised_at TIMESTAMP
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OPS}.entitlements (
    user_email STRING,
    scope_value STRING,
    can_see_sensitive BOOLEAN
)
""")

# Give current user full scope
spark.sql(f"""
MERGE INTO {OPS}.entitlements t
USING (
    SELECT session_user() AS user_email
) s
ON t.user_email = s.user_email

WHEN NOT MATCHED THEN
INSERT (
    user_email,
    scope_value,
    can_see_sensitive
)
VALUES (
    s.user_email,
    '*',
    true
)
""")

# Row-filter function
spark.sql(f"""
CREATE OR REPLACE FUNCTION {OPS}.rf_scope(scope STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: caller sees a row when entitled to its scope value, or to *'
RETURN EXISTS (
    SELECT 1
    FROM {OPS}.entitlements e
    WHERE e.user_email = session_user()
      AND (
          e.scope_value = '*'
          OR e.scope_value = scope
      )
)
""")

# Masking function
spark.sql(f"""
CREATE OR REPLACE FUNCTION {OPS}.mask_text(v STRING)
RETURNS STRING
COMMENT 'Column mask: identifiers only for entitled readers, others see the first character'
RETURN CASE
    WHEN EXISTS (
        SELECT 1
        FROM {OPS}.entitlements e
        WHERE e.user_email = session_user()
          AND e.can_see_sensitive
    )
    THEN v
    ELSE concat(substr(v, 1, 1), '****')
END
""")