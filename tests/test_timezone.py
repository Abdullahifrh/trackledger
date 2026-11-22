from conftest import fixture_path
from pyspark.sql import functions as F

from trackledger import cleaning, ingest
from trackledger import contract as c

def test_daylight_saving_durations_match_in_local_time(raw_disruptions):
    staged = cleaning.stage_disruptions(raw_disruptions).filter(F.col("rdt_id").isin(58962, 62767))
    rows = {r["rdt_id"]: (r["duration_minutes"], r["elapsed_minutes"]) for r in staged.collect()}
    # 58962 spans the spring change (one hour shorter), 62767 the autumn change (one hour longer)
    assert rows == {58962: (11051, 11051), 62767: (4325, 4325)}

def test_utc_reading_would_break_the_daylight_saving_records(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        df = ingest.read_csv_batch(
            spark, [fixture_path("disruptions")], c.DISRUPTIONS_COLUMNS, c.DISRUPTIONS_TS_FORMAT, False
        )
        staged = cleaning.stage_disruptions(ingest.add_ingest_metadata(df, "utc"))
        wrong = staged.filter(F.col("duration_minutes") != F.col("elapsed_minutes")).select("rdt_id").collect()
        assert sorted(r[0] for r in wrong) == [58962, 62767]
    finally:
        spark.conf.set("spark.sql.session.timeZone", c.LOCAL_TZ)

def test_month_is_assigned_in_local_time(raw_disruptions):
    # 99001 is a synthetic record at 00:30 local on 1 February, which is still 31 January in UTC
    staged = cleaning.stage_disruptions(raw_disruptions)
    assert staged.filter(F.col("rdt_id") == 99001).first()["start_month"] == "2025-02"

def test_service_offsets_are_read_as_instants(raw_services):
    row = raw_services.filter(F.col("stop_rdt_id") == 136167815).select(
        F.date_format("arrival_time", "HH:mm").alias("local"),
        F.date_format(F.to_utc_timestamp("arrival_time", c.LOCAL_TZ), "HH:mm").alias("utc"),
    )
    assert tuple(row.first()) == ("02:12", "01:12")
