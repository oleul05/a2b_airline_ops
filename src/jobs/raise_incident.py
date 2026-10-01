# Databricks notebook source
# src/jobs/raise_incident.py — the "false" branch: record why, then fail the run on purpose

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("quarantine_pct", "")

CAT = dbutils.widgets.get("catalog")
ENV = dbutils.widgets.get("env")
OPS = f"{CAT}.{ENV}_ops"

reason = (
    f"quality gate failed "
    f"(quarantine {dbutils.widgets.get('quarantine_pct')}%, "
    f"see reconciliation)"
)

spark.sql(
    """
    INSERT INTO IDENTIFIER(:t)
    VALUES (:run_id, :reason, current_timestamp())
    """,
    args={
        "t": f"{OPS}.incidents",
        "run_id": dbutils.widgets.get("run_id"),
        "reason": reason,
    },
)

# Fail intentionally so the build job becomes FAILED
# and the on_failure e-mail notification can fire.
raise RuntimeError(reason)