from pyspark.sql import functions as F

from trackledger import cleaning, kpis

def test_punctuality_excludes_buses_and_counts_cancellations(raw_services, stations):
    clean = cleaning.kept_rows(cleaning.stage_services(raw_services, stations))
    row = kpis.punctuality_monthly(kpis.operator_monthly(clean)).first()
    counts = (row["planned_arrivals"], row["cancelled_arrivals"], row["measured_arrivals"], row["on_time_arrivals"])
    assert counts == (7, 1, 6, 5)
    assert row["on_time_share"] == 5 / 6
    assert row["cancelled_share"] == 1 / 7

def test_operator_table_has_one_row_per_operator_month(raw_services, stations):
    clean = cleaning.kept_rows(cleaning.stage_services(raw_services, stations))
    assert [r["operator"] for r in kpis.operator_monthly(clean).collect()] == ["NS"]

def test_operator_without_delay_data_gets_no_on_time_share(spark):
    counts = [("2025-01", "A", 1000, 10, 990, 900, 300), ("2025-01", "B", 1000, 0, 1000, 1000, 1)]
    operators = kpis.with_shares(spark.createDataFrame(counts, ["service_month", "operator", *kpis.ARRIVAL_COUNTS]))
    shares = {r["operator"]: (r["has_delay_data"], r["on_time_share"]) for r in operators.collect()}
    assert shares == {"A": (True, 900 / 990), "B": (False, None)}
    network = kpis.punctuality_monthly(operators).first()
    assert (network["measured_arrivals"], network["arrivals_without_delay_data"]) == (990, 1000)

def test_disruption_metrics_in_long_form(raw_disruptions):
    metrics = kpis.disruption_metrics(cleaning.kept_rows(cleaning.stage_disruptions(raw_disruptions)))
    rows = {(r["period"], r["metric"]): r["ours"] for r in metrics.collect()}
    assert rows[("2025", "disruption_count")] == 6
    assert rows[("2025-02", "disruption_count")] == 1
    assert rows[("2025", "cause_count:collision")] == 2
    assert rows[("2025-01", "avg_duration_minutes")] == 63.5
    # May holds only a disruption without an end time, so its average is empty rather than zero
    assert metrics.filter(F.col("ours").isNull()).select("period").collect()[0][0] == "2025-05"
