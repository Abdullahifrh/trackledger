# Databricks notebook source
import os
from datetime import datetime

from pyspark.sql import functions as F

from trackledger import contract as c
from trackledger import ingest, ledger
from trackledger.storage import get_spark, get_store, on_databricks, task_param

# COMMAND ----------

started = datetime.now()
spark = get_spark()
store = get_store(spark)
run_id = task_param("run_id", f"manual-{started:%Y%m%d%H%M%S}")
mode = task_param("ingest_mode", c.MODE_AUTOLOADER if on_databricks() else c.MODE_BATCH)
services_dir = store.landing(c.LANDING_SERVICES)

def landing_files(folder: str) -> list:
    """Data files in a landing folder."""
    names = sorted(n for n in os.listdir(folder) if n.endswith(c.LANDING_SUFFIXES))
    return [os.path.join(folder, n) for n in names]

# COMMAND ----------

if mode == c.MODE_AUTOLOADER:
    checkpoint = store.checkpoint(c.RAW_SERVICES)
    query = (
        ingest.add_ingest_metadata(ingest.read_services_stream(spark, services_dir, checkpoint), run_id)
        .writeStream.option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(store.name(c.RAW_SERVICES))
    )
    query.awaitTermination()
    store.comment(c.RAW_SERVICES)
else:
    seen = ledger.ledger_files(store, "services", "raw")
    new_paths = [p for p in landing_files(services_dir) if os.path.basename(p) not in seen]
    if new_paths:
        batch = ingest.read_csv_batch(spark, new_paths, c.SERVICES_COLUMNS, c.SERVICES_TS_FORMAT, on_databricks())
        store.write(ingest.add_ingest_metadata(batch, run_id), c.RAW_SERVICES, "append")

# COMMAND ----------

# Row balance landing -> raw for every services file not yet in the ledger
seen = ledger.ledger_files(store, "services", "raw")
raw = store.read(c.RAW_SERVICES) if store.exists(c.RAW_SERVICES) else None
new_files = []
if raw is not None:
    found = raw.select(c.FILE_NAME, c.SOURCE_FILE).distinct().collect()
    new_files = sorted({(r[0], r[1]) for r in found if r[0] not in seen})
services_rows = 0
if new_files:
    names = [n for n, _ in new_files]
    raw_counts = ingest.rows_per_file(raw.filter(F.col(c.FILE_NAME).isin(names)), "raw_rows")
    lines = ingest.landing_line_counts(spark, [p for _, p in new_files])
    balance = raw_counts.join(lines, c.FILE_NAME, "full").withColumn(
        "balanced", F.coalesce(F.col("landing_rows") == F.col("raw_rows"), F.lit(False))
    )
    rows = ledger.ledger_rows(balance, run_id, "services", "raw")
    store.write(rows, c.FILE_LEDGER, "append")
    result = rows.select("file_name", "landing_rows", "raw_rows", "balanced").collect()
    services_rows = sum(r["raw_rows"] or 0 for r in result)
    unbalanced = [r["file_name"] for r in result if not r["balanced"]]
else:
    unbalanced = []

# COMMAND ----------

# Disruptions and stations are full snapshots: reread and overwrite each run
snapshots = {}
for dataset, folder, columns, ts_format, table in [
    ("disruptions", c.LANDING_DISRUPTIONS, c.DISRUPTIONS_COLUMNS, c.DISRUPTIONS_TS_FORMAT, c.RAW_DISRUPTIONS),
    ("stations", c.LANDING_STATIONS, c.STATIONS_COLUMNS, c.DISRUPTIONS_TS_FORMAT, c.REF_STATIONS),
]:
    paths = landing_files(store.landing(folder))
    df = ingest.add_ingest_metadata(ingest.read_csv_batch(spark, paths, columns, ts_format, on_databricks()), run_id)
    store.write(df, table, "overwrite")
    counts = ingest.rows_per_file(store.read(table), "raw_rows").join(
        ingest.landing_line_counts(spark, paths), c.FILE_NAME, "full"
    )
    counts = counts.withColumn("balanced", F.coalesce(F.col("landing_rows") == F.col("raw_rows"), F.lit(False)))
    rows = ledger.ledger_rows(counts, run_id, dataset, "raw")
    store.write(rows, c.FILE_LEDGER, "append")
    result = rows.select("file_name", "raw_rows", "balanced").collect()
    snapshots[dataset] = sum(r["raw_rows"] for r in result)
    unbalanced += [r["file_name"] for r in result if not r["balanced"]]

# COMMAND ----------

status = "failed" if unbalanced else "ok"
ledger.log_run(
    spark,
    store,
    {
        "run_id": run_id,
        "task": "ingest",
        "status": status,
        "files_processed": len(new_files),
        "rows_in": services_rows,
        "rows_out": services_rows,
        "details": {
            "mode": mode,
            "new_files": [n for n, _ in new_files],
            "snapshots": snapshots,
            "unbalanced": unbalanced,
        },
    },
    started,
)
print(f"ingest {status}: mode={mode}, new files={len(new_files)}, services rows={services_rows}, snapshots={snapshots}")
if unbalanced:
    raise RuntimeError(f"Landing lines and raw rows differ for {unbalanced}")
