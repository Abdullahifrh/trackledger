import pytest

from trackledger import contract as c
from trackledger import reconcile

@pytest.mark.parametrize(
    "text, minutes",
    [("3 hours", 180), ("2 hours, 47 minutes", 167), ("1 hour, 41 minutes", 101), ("1 day, 14 hours", 2280)],
)
def test_parse_duration_text(text, minutes):
    assert reconcile.parse_duration_text(text) == minutes

def test_status_uses_fixed_tolerances_and_known_gaps(spark):
    gap = next(iter(c.KNOWN_GAPS))
    published = spark.createDataFrame(
        [
            ("2025-01", c.METRIC_AVG_DURATION, 180.0, "u", "d"),
            ("2025-02", c.METRIC_AVG_DURATION, 141.0, "u", "d"),
            ("2025-01", c.METRIC_COUNT, 656.0, "u", "d"),
            (gap[0], gap[1], 290.0, "u", "d"),
            ("2025", f"{c.CAUSE_PREFIX}collision", 404.0, "u", "d"),
        ],
        reconcile.PUBLISHED_SCHEMA,
    )
    ours = spark.createDataFrame(
        [
            ("2025-01", c.METRIC_AVG_DURATION, 179.2),
            ("2025-02", c.METRIC_AVG_DURATION, 143.0),
            ("2025-01", c.METRIC_COUNT, 656.0),
            (gap[0], gap[1], 302.0),
        ],
        "period string, metric string, ours double",
    )
    result = reconcile.compare(spark, ours, published, "test")
    status = {(r["period"], r["metric"]): r["status"] for r in result.collect()}
    assert status == {
        ("2025-01", c.METRIC_AVG_DURATION): c.STATUS_PASS,
        ("2025-02", c.METRIC_AVG_DURATION): c.STATUS_FAIL,
        ("2025-01", c.METRIC_COUNT): c.STATUS_PASS,
        gap: c.STATUS_EXPLAINED,
        ("2025", f"{c.CAUSE_PREFIX}collision"): c.STATUS_FAIL,
    }
