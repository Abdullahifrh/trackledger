from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from trackledger import contract as c

def train_arrivals(clean_services: DataFrame) -> DataFrame:
    """Planned arrivals of train services; replacement buses are left out."""
    return clean_services.filter(~F.expr(c.NON_TRAIN_SERVICE) & F.col("arrival_time").isNotNull())

ARRIVAL_COUNTS = ["planned_arrivals", "cancelled_arrivals", "measured_arrivals", "on_time_arrivals", "delayed_arrivals"]

def operator_monthly(clean_services: DataFrame) -> DataFrame:
    """Arrival counts and shares per operator and month (tile 3)."""
    arrivals = train_arrivals(clean_services)
    cancelled = F.coalesce(F.col("arrival_cancelled"), F.lit(False))
    measured = ~cancelled & F.col("arrival_delay_min").isNotNull()
    agg = arrivals.groupBy("service_month", "operator").agg(
        F.count("*").alias("planned_arrivals"),
        F.sum(cancelled.cast("long")).alias("cancelled_arrivals"),
        F.sum(measured.cast("long")).alias("measured_arrivals"),
        F.sum((measured & (F.col("arrival_delay_min") < c.ON_TIME_LIMIT_MIN)).cast("long")).alias("on_time_arrivals"),
        F.sum((measured & (F.col("arrival_delay_min") > 0)).cast("long")).alias("delayed_arrivals"),
    )
    return with_shares(agg)

def with_shares(agg: DataFrame) -> DataFrame:
    """Add the delay-data flag and the on-time and cancelled shares to arrival counts."""
    has_data = F.col("delayed_arrivals") >= F.col("measured_arrivals") * c.MIN_DELAYED_SHARE
    return agg.withColumns(
        {
            "has_delay_data": has_data & (F.col("measured_arrivals") > 0),
            "on_time_share": F.when(has_data, F.col("on_time_arrivals") / F.col("measured_arrivals")),
            "cancelled_share": F.col("cancelled_arrivals") / F.col("planned_arrivals"),
        }
    )

def punctuality_monthly(operators: DataFrame) -> DataFrame:
    """Network figures per month (tile 1); on-time counts only use operators with delay data."""
    with_data = F.col("has_delay_data")
    agg = operators.groupBy("service_month").agg(
        F.sum("planned_arrivals").alias("planned_arrivals"),
        F.sum("cancelled_arrivals").alias("cancelled_arrivals"),
        F.sum(F.when(with_data, F.col("measured_arrivals")).otherwise(0)).alias("measured_arrivals"),
        F.sum(F.when(with_data, F.col("on_time_arrivals")).otherwise(0)).alias("on_time_arrivals"),
        F.sum(F.when(with_data, F.col("delayed_arrivals")).otherwise(0)).alias("delayed_arrivals"),
        F.sum(F.when(~with_data, F.col("measured_arrivals")).otherwise(0)).alias("arrivals_without_delay_data"),
    )
    return with_shares(agg)

def disruptions_monthly(clean_disruptions: DataFrame) -> DataFrame:
    """Disruptions and average duration per local start month and cause group (tile 2)."""
    return clean_disruptions.groupBy("start_month", "cause_group").agg(
        F.count("*").alias("disruptions"),
        F.avg("duration_minutes").alias("avg_duration_minutes"),
    )

def disruption_metrics(clean_disruptions: DataFrame) -> DataFrame:
    """Our figures in long form (period, metric, ours), keyed like the published reference."""
    year = clean_disruptions.filter(F.col("start_month").startswith(c.REPORT_YEAR))
    totals = [F.count("*").alias(c.METRIC_COUNT), F.avg("duration_minutes").alias(c.METRIC_AVG_DURATION)]
    count, avg = c.METRIC_COUNT, c.METRIC_AVG_DURATION
    stack = f"stack(2, '{count}', double({count}), '{avg}', {avg})"
    monthly = year.groupBy(F.col("start_month").alias("period")).agg(*totals)
    yearly = year.agg(*totals).withColumn("period", F.lit(c.REPORT_YEAR))
    long = monthly.unionByName(yearly).select("period", F.expr(f"{stack} AS (metric, ours)"))
    causes = year.groupBy(c.CAUSE_FIELD).agg(F.count("*").cast("double").alias("ours")).select(
        F.lit(c.REPORT_YEAR).alias("period"),
        F.concat(F.lit(c.CAUSE_PREFIX), F.col(c.CAUSE_FIELD)).alias("metric"),
        "ours",
    )
    return long.unionByName(causes)
