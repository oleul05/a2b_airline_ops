# Databricks notebook source
# tools/export_dashboard_data.py — run in Databricks.
# Re-runs every dataset of the client dashboard (as currently saved, including UI edits) and writes
# one CSV per dataset + the dashboard definition into a single zip on a Volume, for the client deck.
import json
import os
import shutil
import zipfile

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("dashboard_id", "")      # from the dashboard URL: .../dashboardsv3/<id>/...
dbutils.widgets.text("volume", "workspace.prd_ops.demo_exports")

VOLUME = dbutils.widgets.get("volume")
OUT = "/Volumes/" + VOLUME.replace(".", "/") + "/dashboard_data"
w = WorkspaceClient()

# ---- find the dashboard
dash_id = dbutils.widgets.get("dashboard_id").strip()
if not dash_id:
    hits = [d for d in w.lakeview.list() if "airline" in (d.display_name or "").lower()]
    if len(hits) != 1:
        for d in hits:
            print(f"{d.dashboard_id}  {d.display_name}")
        raise ValueError(f"Found {len(hits)} matching dashboards; paste the right id into the dashboard_id widget")
    dash_id = hits[0].dashboard_id
dash = w.lakeview.get(dash_id)
spec = json.loads(dash.serialized_dashboard)
print(f"Dashboard: {dash.display_name} ({dash_id})")

# ---- run every dataset
spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")
shutil.rmtree(OUT, ignore_errors=True)
os.makedirs(OUT, exist_ok=True)

EXTRA = {   # not on the dashboard, but useful for the architecture slide
    "extra_layer_counts": """
        SELECT 'bronze_flights' AS layer, COUNT(*) AS row_count FROM workspace.prd_lakehouse.bronze_flights
        UNION ALL SELECT 'silver_flights_clean', COUNT(*) FROM workspace.prd_lakehouse.silver_flights_clean
        UNION ALL SELECT 'silver_flights_quarantine', COUNT(*) FROM workspace.prd_lakehouse.silver_flights_quarantine
        UNION ALL SELECT 'silver_flights (de-duplicated)', COUNT(*) FROM workspace.prd_lakehouse.silver_flights""",
}
queries = {ds["name"]: "".join(ds.get("queryLines", [])) for ds in spec["datasets"]} | EXTRA

summary = []
for name, sql in queries.items():
    try:
        pdf = spark.sql(sql).toPandas()
        pdf.to_csv(f"{OUT}/{name}.csv", index=False)
        summary.append((name, len(pdf), "OK"))
    except Exception as e:
        summary.append((name, 0, f"FAILED: {str(e).splitlines()[0][:200]}"))

# widget title -> dataset map, so the deck matches what each chart shows
widgets = []
for page in spec["pages"]:
    for item in page.get("layout", []):
        wd = item["widget"]
        title = wd.get("spec", {}).get("frame", {}).get("title", "")
        for q in wd.get("queries", []):
            widgets.append({"page": page["displayName"], "widget": title,
                            "type": wd.get("spec", {}).get("widgetType"), "dataset": q["query"].get("datasetName")})
with open(f"{OUT}/_dashboard.json", "w") as f:
    json.dump(spec, f, indent=2)
with open(f"{OUT}/_widgets.json", "w") as f:
    json.dump(widgets, f, indent=2)

# ---- one zip to download
tmp = "/tmp/dashboard_data_export.zip"
with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as z:
    for fn in sorted(os.listdir(OUT)):
        z.write(f"{OUT}/{fn}", fn)
shutil.copy(tmp, f"{OUT}/../dashboard_data_export.zip")

display(spark.createDataFrame(summary, "dataset string, row_count int, status string"))
print(f"Download: Catalog > {VOLUME.replace('.', ' > ')} > dashboard_data_export.zip > Download")
