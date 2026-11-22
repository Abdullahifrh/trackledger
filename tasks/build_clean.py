# Databricks notebook source
from datetime import datetime

from pyspark.sql import functions as F

from trackledger import cleaning, ledger
from trackledger import contract as c
from trackledger.storage import get_spark, get_store, task_param

# COMMAND ----------

started = datetime.now()
spark = get_spark()
store = get_store(spark)
run_id = task_param("run_id", f"manual-{started:%Y%m%d%H%M%S}")
service_columns = [dst for _, dst, _ in c.SERVICES_COLUMNS] + [c.RESCUED]
disruption_columns = [dst for _, dst, _ in c.DISRUPTIONS_COLUMNS] + [c.RESCUED]

# COMMAND ----------

# Services: only raw files the clean layer has not seen yet
done = ledger.ledger_files(store, "services", "clean")
raw = store.read(c.RAW_SERVICES)
new_files = sorted(r[0] for r in raw.select(c.FILE_NAME).distinct().collect() if r[0] not in done)
merge = {"inserted": 0, "updated": 0}
totals = {"raw_rows": 0, "clean_rows": 0, "quarantined_rows": 0, "duplicate_rows": 0, "warn_unknown_station": 0}
if new_files:
    batch = raw.filter(F.col(c.FILE_NAME).isin(new_files))
    store.write(cleaning.stage_services(batch, store.read(c.REF_STATIONS)), c.STAGE_SERVICES, "overwrite")
    staged = store.read(c.STAGE_SERVICES)
    balance = cleaning.balance_per_file(staged, "services")
    per_file = balance.collect()
    totals = {k: sum(r[k] for r in per_file) for k in totals}
    store.write(cleaning.quarantined_rows(staged, "services", service_columns), c.QUARANTINE, "append")
    share = totals["quarantined_rows"] / max(totals["raw_rows"], 1)
    unbalanced = [r[c.FILE_NAME] for r in per_file if not r["balanced"]]
    if share > c.QUARANTINE_FAIL_SHARE or unbalanced:
        details = {"quarantine_share": share, "unbalanced": unbalanced, "files": new_files}
        entry = {"run_id": run_id, "task": "clean", "status": "failed", "details": details}
        counts = {"rows_in": totals["raw_rows"], "rows_quarantined": totals["quarantined_rows"]}
        ledger.log_run(spark, store, {**entry, **counts}, started)
        raise RuntimeError(f"Quality gate: quarantine share {share:.4%}, unbalanced files {unbalanced}")
    merge = store.upsert(cleaning.kept_rows(staged), c.CLEAN_SERVICES, c.SERVICES_KEY, c.INGESTED_AT)
    store.write(ledger.ledger_rows(balance, run_id, "services", "clean"), c.FILE_LEDGER, "append")

# COMMAND ----------

# Disruptions: the snapshot is rebuilt in full
staged_d = cleaning.stage_disruptions(store.read(c.RAW_DISRUPTIONS))
balance_d = cleaning.balance_per_file(staged_d, "disruptions")
store.write(cleaning.kept_rows(staged_d), c.CLEAN_DISRUPTIONS, "overwrite")
store.write(cleaning.quarantined_rows(staged_d, "disruptions", disruption_columns), c.QUARANTINE, "append")
store.write(ledger.ledger_rows(balance_d, run_id, "disruptions", "clean"), c.FILE_LEDGER, "append")
disruptions = balance_d.first().asDict()

# COMMAND ----------

ledger.log_run(
    spark,
    store,
    {
        "run_id": run_id,
        "task": "clean",
        "status": "ok",
        "files_processed": len(new_files),
        "rows_in": totals["raw_rows"],
        "rows_out": totals["clean_rows"],
        "rows_quarantined": totals["quarantined_rows"],
        "details": {"files": new_files, "merge": merge, "services": totals, "disruptions": disruptions},
    },
    started,
)
print(f"clean ok: files={new_files}, merge={merge}, services={totals}, disruptions={disruptions}")
