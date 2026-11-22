import argparse
import csv
import json
import os
import sys

from trackledger import contract as c
from trackledger.storage import repo_root

RUN_SPECIFIC = {"run_id", "tasks"}
TOLERANCE = 1e-9

def differences(local, remote, path: str = "") -> list:
    """Every place where two summaries disagree, with floats compared to a tiny tolerance."""
    if isinstance(local, dict) and isinstance(remote, dict):
        keys = sorted(set(local) | set(remote))
        return [d for k in keys for d in differences(local.get(k), remote.get(k), f"{path}.{k}")]
    if isinstance(local, list) and isinstance(remote, list):
        if len(local) != len(remote):
            return [{"path": path, "local": len(local), "databricks": len(remote)}]
        pairs = enumerate(zip(local, remote, strict=True))
        return [d for i, (a, b) in pairs for d in differences(a, b, f"{path}[{i}]")]
    if isinstance(local, float) or isinstance(remote, float):
        if local is None or remote is None or abs(local - remote) > TOLERANCE:
            return [{"path": path, "local": local, "databricks": remote}]
        return []
    return [] if local == remote else [{"path": path, "local": local, "databricks": remote}]

def read_run_log(path: str) -> list:
    """The run_log export from Databricks, with the details column parsed."""
    with open(path, encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    counts = ["files_processed", "rows_in", "rows_out", "rows_quarantined"]
    return [{**r, **{k: int(r[k]) for k in counts}, "details": json.loads(r["details"])} for r in rows]

def year_totals(summary: dict) -> dict:
    """Year figures for the README, summed from the monthly and per-file tables."""
    months, files = summary["punctuality_monthly"], summary["services_files"]
    counts = ["planned_arrivals", "cancelled_arrivals", "measured_arrivals", "on_time_arrivals"]
    total = {k: sum(m[k] for m in months) for k in counts}
    return {
        **total,
        "on_time_share": round(total["on_time_arrivals"] / total["measured_arrivals"], 6),
        "cancelled_share": round(total["cancelled_arrivals"] / total["planned_arrivals"], 6),
        "landing_rows": sum(f["landing_rows"] for f in files),
        "unknown_station_rows": sum(json.loads(f["warnings"])["warn_unknown_station"] for f in files),
    }

def main(summary_path: str, run_log_path: str) -> int:
    with open(summary_path, encoding="utf-8") as handle:
        remote = json.load(handle)
    with open(os.path.join(repo_root(), c.LOCAL_EXPECTED_FILE), encoding="utf-8") as handle:
        local = json.load(handle)["final"]
    compared = {k: v for k, v in remote.items() if k not in RUN_SPECIFIC}
    found = differences(local, compared)
    result = {
        "source": "Databricks Free Edition, serverless compute, job trackledger-monthly",
        "parity": {"compared_with": c.LOCAL_EXPECTED_FILE, "sections": sorted(compared), "differences": found},
        "year": year_totals(compared),
        "runs": read_run_log(run_log_path),
        "final": compared,
    }
    with open(os.path.join(repo_root(), c.RUN_SUMMARY_FILE), "w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(f"{len(found)} differences across {len(compared)} sections")
    return 1 if found else 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare a Databricks run summary with the local reference run.")
    parser.add_argument("summary", help="JSON line printed by the check_published task")
    parser.add_argument("run_log", help="CSV export of workspace.trackledger.run_log")
    args = parser.parse_args()
    sys.exit(main(args.summary, args.run_log))
