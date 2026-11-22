# Databricks notebook source
from datetime import datetime

from trackledger import contract as c
from trackledger import kpis, ledger
from trackledger.storage import get_spark, get_store, task_param

# COMMAND ----------

started = datetime.now()
spark = get_spark()
store = get_store(spark)
run_id = task_param("run_id", f"manual-{started:%Y%m%d%H%M%S}")
services = store.read(c.CLEAN_SERVICES)
disruptions = store.read(c.CLEAN_DISRUPTIONS)

# COMMAND ----------

store.write(kpis.operator_monthly(services), c.REPORT_OPERATORS, "overwrite")
store.write(kpis.punctuality_monthly(store.read(c.REPORT_OPERATORS)), c.REPORT_PUNCTUALITY, "overwrite")
store.write(kpis.disruptions_monthly(disruptions), c.REPORT_DISRUPTIONS, "overwrite")
rows = {t: store.read(t).count() for t in [c.REPORT_OPERATORS, c.REPORT_PUNCTUALITY, c.REPORT_DISRUPTIONS]}

# COMMAND ----------

ledger.log_run(
    spark,
    store,
    {"run_id": run_id, "task": "report", "status": "ok", "rows_out": sum(rows.values()), "details": rows},
    started,
)
print(f"report ok: {rows}")
