# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# tools/drift_drill.py — DEV ONLY. Rehearses a vendor schema change on real rows:
# copies 1,000 rows of a landed CSV into a new file that carries one extra column.
LANDING = "/Volumes/workspace/dev_landing/raw/ontime_csv"
first = sorted(f.name for f in dbutils.fs.ls(LANDING) if f.name.endswith(".csv"))[0]
with open(f"{LANDING}/{first}") as fh:
    lines = [next(fh).rstrip("\n") for _ in range(1001)]
header, rows = lines[0], lines[1:]
drifted = [header + ',"feed_version"'] + [row + ',"v2"' for row in rows]
drill_name = first[:-4] + "_drift.csv"          # keeps the prefix and period that Silver parses
dbutils.fs.put(f"{LANDING}/{drill_name}", "\n".join(drifted) + "\n", overwrite=False)
print("landed", drill_name, "- now run the pipeline and watch Bronze")

# COMMAND ----------

display(
    dbutils.fs.ls(
        "/Volumes/workspace/dev_landing/raw/ontime_csv"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.dev_lakehouse.bronze_flights;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     _source_file,
# MAGIC     COUNT(*) AS row_count,
# MAGIC     COUNT(feed_version) AS feed_version_rows
# MAGIC FROM workspace.dev_lakehouse.bronze_flights
# MAGIC GROUP BY _source_file
# MAGIC ORDER BY _source_file;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     _source_file,
# MAGIC     _rescued_data
# MAGIC FROM workspace.dev_lakehouse.bronze_flights
# MAGIC WHERE _rescued_data IS NOT NULL
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     get_json_object(_rescued_data, '$._c109') AS rescued_c109,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM workspace.dev_lakehouse.bronze_flights
# MAGIC WHERE _rescued_data IS NOT NULL
# MAGIC GROUP BY get_json_object(_rescued_data, '$._c109');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _rescued_data
# MAGIC FROM workspace.dev_lakehouse.bronze_flights
# MAGIC LIMIT 50;