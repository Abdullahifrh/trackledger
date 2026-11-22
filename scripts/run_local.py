import json
import os
import runpy
import shutil
import sys
from datetime import date

from trackledger import contract as c
from trackledger import ledger
from trackledger.storage import get_spark, get_store, repo_root

REPO = repo_root()

TASKS = ["ingest_landing", "build_clean", "build_report", "check_published"]

def link_landing(source: str, target: str, months: list) -> None:
    """Expose the chosen services months plus the snapshots in a local landing folder."""
    for folder in [c.LANDING_SERVICES, c.LANDING_DISRUPTIONS, c.LANDING_STATIONS]:
        os.makedirs(os.path.join(target, folder), exist_ok=True)
        for name in os.listdir(os.path.join(source, folder)):
            keep = folder != c.LANDING_SERVICES or any(f"-{m}." in name for m in months)
            link = os.path.join(target, folder, name)
            if keep and not os.path.exists(link):
                os.symlink(os.path.join(source, folder, name), link)

def run_all(run_id: str) -> dict:
    """Run the four tasks in order, like the Databricks job, and return the run summary."""
    os.environ["TRACKLEDGER_RUN_ID"] = run_id
    for task in TASKS:
        runpy.run_path(os.path.join(REPO, "tasks", f"{task}.py"), run_name="__main__")
    return ledger.build_summary(get_store(get_spark()))

def run_view(summary: dict) -> dict:
    """What each run did: per-task status and row counts."""
    keep = ["status", "files_processed", "rows_in", "rows_out", "rows_quarantined"]
    tasks = {t["task"]: {k: t[k] for k in keep} | {"details": json.loads(t["details"])} for t in summary["tasks"]}
    return {"run_id": summary["run_id"], "tasks": tasks, "table_rows": summary["table_rows"]}

def main(source: str) -> None:
    work = os.path.join(REPO, c.LOCAL_WAREHOUSE)
    landing = os.path.join(work, "landing")
    shutil.rmtree(work, ignore_errors=True)
    os.environ["TRACKLEDGER_WAREHOUSE"] = os.path.join(work, "tables")
    os.environ["TRACKLEDGER_LANDING"] = landing
    link_landing(source, landing, [f"{m:02d}" for m in range(1, 12)])
    initial = run_all("local-1")
    link_landing(source, landing, ["12"])
    december = run_all("local-2")
    rerun = run_all("local-3")
    expected = {
        "generated_on": date.today().isoformat(),
        "spark_version": get_spark().version,
        "runs": {"initial": run_view(initial), "december": run_view(december), "rerun": run_view(rerun)},
        "final": {k: v for k, v in rerun.items() if k not in ("run_id", "tasks")},
    }
    with open(os.path.join(REPO, c.LOCAL_EXPECTED_FILE), "w", encoding="utf-8") as handle:
        json.dump(expected, handle, indent=2, sort_keys=True, default=str)
        handle.write("\n")

if __name__ == "__main__":
    main(os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else c.LOCAL_LANDING))
