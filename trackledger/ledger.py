import json
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from trackledger import contract as c
from trackledger import kpis
from trackledger.storage import Store

RUN_LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("task", StringType()),
        StructField("status", StringType()),
        StructField("files_processed", LongType()),
        StructField("rows_in", LongType()),
        StructField("rows_out", LongType()),
        StructField("rows_quarantined", LongType()),
        StructField("details", StringType()),
        StructField("started_at", TimestampType()),
        StructField("finished_at", TimestampType()),
    ]
)
SHARE_DIGITS = 6

def ledger_files(store: Store, dataset: str, stage: str) -> set:
    """File names already recorded for a dataset and stage (a tiny result)."""
    if not store.exists(c.FILE_LEDGER):
        return set()
    rows = store.read(c.FILE_LEDGER).filter((F.col("dataset") == dataset) & (F.col("stage") == stage))
    return {r[0] for r in rows.select("file_name").distinct().collect()}

def ledger_rows(balance: DataFrame, run_id: str, dataset: str, stage: str) -> DataFrame:
    """Shape per-file counts into file_ledger rows; missing counts become null."""
    counts = ["landing_rows", "raw_rows", "clean_rows", "quarantined_rows", "duplicate_rows"]
    warn_cols = [col for col in balance.columns if col.startswith("warn_")]
    warnings = F.to_json(F.struct(*warn_cols)) if warn_cols else F.lit(None).cast("string")
    return balance.select(
        F.lit(run_id).alias("run_id"),
        F.lit(dataset).alias("dataset"),
        F.lit(stage).alias("stage"),
        F.col(c.FILE_NAME).alias("file_name"),
        *[(F.col(n) if n in balance.columns else F.lit(None)).cast("long").alias(n) for n in counts],
        warnings.alias("warnings"),
        F.col("balanced").cast("boolean").alias("balanced"),
        F.current_timestamp().alias("logged_at"),
    )

def log_run(spark: SparkSession, store: Store, entry: dict, started_at: datetime) -> None:
    """Append one task result to run_log."""
    row = (
        entry["run_id"],
        entry["task"],
        entry["status"],
        int(entry.get("files_processed", 0)),
        int(entry.get("rows_in", 0)),
        int(entry.get("rows_out", 0)),
        int(entry.get("rows_quarantined", 0)),
        json.dumps(entry.get("details", {}), sort_keys=True),
        started_at,
        datetime.now(),
    )
    store.write(spark.createDataFrame([row], RUN_LOG_SCHEMA), c.RUN_LOG, "append")

def as_dicts(df: DataFrame, order: list) -> list:
    """Collect a small table as sorted plain dictionaries."""
    return [r.asDict() for r in df.orderBy(*order).collect()]

def rounded(df: DataFrame) -> DataFrame:
    """Round share and average columns so the summary compares cleanly across platforms."""
    cols = [
        F.round(col, SHARE_DIGITS).alias(col) if col.endswith(("_share", "_minutes", "ours", "diff")) else F.col(col)
        for col in df.columns
    ]
    return df.select(*cols)

def build_summary(store: Store) -> dict:
    """Everything the parity check and the README need, from small aggregates only."""
    run_log = store.read(c.RUN_LOG)
    last_run = run_log.orderBy(F.desc("finished_at")).first()["run_id"]
    tasks = as_dicts(
        run_log.filter(F.col("run_id") == last_run).select(
            "task", "status", "files_processed", "rows_in", "rows_out", "rows_quarantined", "details"
        ),
        ["task"],
    )
    latest = store.read(c.FILE_LEDGER).filter(F.col("dataset") == "services")
    landing = latest.filter(F.col("stage") == "raw").select("file_name", "landing_rows")
    clean = latest.filter(F.col("stage") == "clean").drop("landing_rows")
    counts = ["landing_rows", "raw_rows", "clean_rows", "quarantined_rows", "duplicate_rows"]
    files = landing.join(clean, "file_name", "left").select("file_name", *counts, "balanced", "warnings")
    sums = [F.sum(n).alias(n) for n in kpis.ARRIVAL_COUNTS]
    operators = store.read(c.REPORT_OPERATORS).groupBy("operator").agg(*sums)
    operators = kpis.with_shares(operators)
    disruptions = store.read(c.REPORT_DISRUPTIONS)
    recon = store.read(c.RECONCILIATION)
    status = recon.groupBy("status").agg(F.count("*").alias("n")).collect()
    shown = ["metric", "period", "ours", "published", "diff", "status"]
    not_pass = recon.filter(F.col("status") != c.STATUS_PASS).select(*shown)
    return {
        "run_id": last_run,
        "tasks": tasks,
        "table_rows": {
            t: store.read(t).count()
            for t in [c.RAW_SERVICES, c.CLEAN_SERVICES, c.QUARANTINE, c.RAW_DISRUPTIONS, c.CLEAN_DISRUPTIONS]
        },
        "services_files": as_dicts(files, ["file_name"]),
        "punctuality_monthly": as_dicts(rounded(store.read(c.REPORT_PUNCTUALITY)), ["service_month"]),
        "operators_year": as_dicts(rounded(operators), ["operator"]),
        "operator_monthly_rows": store.read(c.REPORT_OPERATORS).count(),
        "disruptions_by_group_year": as_dicts(
            disruptions.groupBy("cause_group").agg(F.sum("disruptions").alias("disruptions")), ["cause_group"]
        ),
        "reconciliation_status": {r["status"]: r["n"] for r in status},
        "reconciliation_not_pass": as_dicts(rounded(not_pass), ["metric"]),
    }
