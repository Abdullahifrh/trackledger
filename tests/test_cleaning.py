from pyspark.sql import functions as F

from trackledger import cleaning
from trackledger import contract as c

def test_rules_quarantine_and_dedupe(raw_services, stations):
    staged = cleaning.stage_services(raw_services, stations)
    quarantine = cleaning.quarantined_rows(staged, "services", ["service_rdt_id"]).collect()
    assert [(r["rule"], r["row_json"]) for r in quarantine] == [("unparsed_fields", '{"service_rdt_id":15086999}')]
    assert cleaning.kept_rows(staged).count() == 10
    assert staged.filter(F.col(c.ROW_RANK) > 1).count() == 1

def test_row_balance_and_station_warning(raw_services, stations):
    balance = cleaning.balance_per_file(cleaning.stage_services(raw_services, stations), "services").first()
    assert (balance["raw_rows"], balance["clean_rows"], balance["quarantined_rows"], balance["duplicate_rows"]) == (
        12,
        10,
        1,
        1,
    )
    assert balance["balanced"]
    assert balance["warn_unknown_station"] == 2

def test_disruptions_keep_rows_without_end_time(raw_disruptions):
    staged = cleaning.stage_disruptions(raw_disruptions)
    balance = cleaning.balance_per_file(staged, "disruptions").first()
    assert balance["clean_rows"] == 6
    assert balance["warn_no_end_time"] == 1
    assert balance["warn_duration_mismatch"] == 0
