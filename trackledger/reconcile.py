import csv
import re

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from trackledger import contract as c

UNIT_MINUTES = {"minute": 1, "hour": 60, "day": 1440}
PUBLISHED_SCHEMA = StructType(
    [
        StructField("period", StringType()),
        StructField("metric", StringType()),
        StructField("published", DoubleType()),
        StructField("source_url", StringType()),
        StructField("retrieved_on", StringType()),
    ]
)

def parse_duration_text(text: str) -> int:
    """Turn a duration as shown on the site, such as '2 hours, 47 minutes', into minutes."""
    parts = re.findall(r"(\d+)\s+(minute|hour|day)s?", text)
    if not parts:
        raise ValueError(f"no duration in {text!r}")
    return sum(int(n) * UNIT_MINUTES[unit] for n, unit in parts)

def read_published(spark: SparkSession, path: str) -> DataFrame:
    """Load the small reference CSV that is kept in the repository."""
    with open(path, encoding="utf-8", newline="") as handle:
        rows = [
            (r["period"], r["metric"], float(r["value"]), r["source_url"], r["retrieved_on"])
            for r in csv.DictReader(handle)
        ]
    return spark.createDataFrame(rows, PUBLISHED_SCHEMA)

def tolerance_for(metric: Column) -> Column:
    """Tolerance per metric from the contract."""
    return (
        F.when(metric == c.METRIC_COUNT, F.lit(c.TOLERANCES[c.METRIC_COUNT]))
        .when(metric == c.METRIC_AVG_DURATION, F.lit(c.TOLERANCES[c.METRIC_AVG_DURATION]))
        .when(metric.startswith(c.CAUSE_PREFIX), F.lit(c.TOLERANCES[c.CAUSE_PREFIX]))
    )

def compare(spark: SparkSession, ours: DataFrame, published: DataFrame, run_id: str) -> DataFrame:
    """One row per published figure with ours, the difference, the tolerance and a status."""
    gaps = spark.createDataFrame(
        [(p, m, note) for (p, m), note in c.KNOWN_GAPS.items()], "period string, metric string, gap_note string"
    )
    joined = published.join(ours, ["period", "metric"], "left").join(gaps, ["period", "metric"], "left")
    diff = F.col("ours") - F.col("published")
    within = F.coalesce(F.abs(diff) <= F.col("tolerance"), F.lit(False))
    status = (
        F.when(within, F.lit(c.STATUS_PASS))
        .when(F.col("gap_note").isNotNull() & F.col("ours").isNotNull(), F.lit(c.STATUS_EXPLAINED))
        .otherwise(F.lit(c.STATUS_FAIL))
    )
    return (
        joined.withColumn("tolerance", tolerance_for(F.col("metric")))
        .withColumn("diff", diff)
        .withColumn("status", status)
        .select(
            "metric",
            "period",
            "ours",
            "published",
            "diff",
            "tolerance",
            "status",
            F.when(F.col("status") == c.STATUS_EXPLAINED, F.col("gap_note")).alias("note"),
            "source_url",
            F.lit(run_id).alias("run_id"),
            F.current_timestamp().alias("checked_at"),
        )
    )
