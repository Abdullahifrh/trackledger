from conftest import fixture_path

from trackledger import contract as c
from trackledger import ingest

def test_every_landing_line_becomes_a_raw_row(spark, raw_services):
    lines = ingest.landing_line_counts(spark, [fixture_path("services")]).first()["landing_rows"]
    assert raw_services.count() == lines == 12

def test_columns_are_renamed_and_typed(raw_services):
    expected = [dst for _, dst, _ in c.SERVICES_COLUMNS]
    assert raw_services.columns[: len(expected)] == expected
    assert dict(raw_services.dtypes)["arrival_time"] == "timestamp"
    assert raw_services.select(c.FILE_NAME).distinct().first()[0] == "services-2025-01.csv"

def test_bad_value_goes_to_rescued_column(raw_services):
    # Read every column, as the raw write does; a narrow projection would skip parsing the bad field
    bad = [r["service_rdt_id"] for r in raw_services.collect() if r[c.RESCUED] is not None]
    assert bad == [15086999]
