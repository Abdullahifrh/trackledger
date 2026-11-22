import csv
import gzip
import os

from conftest import fixture_path

from trackledger import contract as c
from trackledger.reconcile import parse_duration_text

REPO = os.path.dirname(os.path.dirname(__file__))

def header(path: str) -> list:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as handle:
        return next(csv.reader(handle))

def test_schemas_match_file_headers():
    for folder, columns in [
        ("services", c.SERVICES_COLUMNS),
        ("disruptions", c.DISRUPTIONS_COLUMNS),
        ("stations", c.STATIONS_COLUMNS),
    ]:
        assert header(fixture_path(folder)) == [src for src, _, _ in columns]

def test_rule_names_unique_per_dataset():
    keys = [(r.dataset, r.name) for r in c.RULES]
    assert len(keys) == len(set(keys))
    assert {r.severity for r in c.RULES} == {c.DROP, c.WARN}

def test_published_reference_is_consistent():
    with open(os.path.join(REPO, c.REFERENCE_FILE), encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 36
    for row in rows:
        if row["metric"] == c.METRIC_AVG_DURATION:
            assert float(row["value"]) == parse_duration_text(row["as_shown"])
        else:
            assert f"{int(row['value']):,}" in row["as_shown"]
    monthly = sum(int(r["value"]) for r in rows if r["metric"] == c.METRIC_COUNT and r["period"] != c.REPORT_YEAR)
    yearly = [int(r["value"]) for r in rows if r["metric"] == c.METRIC_COUNT and r["period"] == c.REPORT_YEAR]
    assert [monthly] == yearly
