# Databricks notebook source
import json
import os
from datetime import datetime

from pyspark.sql import functions as F

from trackledger import contract as c
from trackledger import kpis, ledger, reconcile
from trackledger.storage import get_spark, get_store, repo_root, task_param

# COMMAND ----------

started = datetime.now()
spark = get_spark()
store = get_store(spark)
run_id = task_param("run_id", f"manual-{started:%Y%m%d%H%M%S}")
zone = spark.conf.get("spark.sql.session.timeZone")
if zone != c.LOCAL_TZ:
    raise RuntimeError(f"Session time zone is {zone}, months would shift; expected {c.LOCAL_TZ}")

# COMMAND ----------

ours = kpis.disruption_metrics(store.read(c.CLEAN_DISRUPTIONS))
published = reconcile.read_published(spark, os.path.join(repo_root(), c.REFERENCE_FILE))
store.write(reconcile.compare(spark, ours, published, run_id), c.RECONCILIATION, "overwrite")
result = store.read(c.RECONCILIATION)
counts = {r["status"]: r["n"] for r in result.groupBy("status").agg(F.count("*").alias("n")).collect()}
failed = result.filter(F.col("status") == c.STATUS_FAIL).select("metric", "period", "diff")
breaches = [r.asDict() for r in failed.collect()]

# COMMAND ----------

status = "failed" if breaches else "ok"
ledger.log_run(
    spark,
    store,
    {"run_id": run_id, "task": "reconcile", "status": status, "rows_out": sum(counts.values()), "details": counts},
    started,
)
summary = ledger.build_summary(store)
print(json.dumps(summary, sort_keys=True, default=str))

# COMMAND ----------

if breaches:
    raise RuntimeError(f"Reconciliation breach: {breaches}")
