from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from trackledger import contract as c

def failed(rule: c.Rule):
    """Boolean column that is true when a row breaks the rule; nulls count as passing."""
    return F.coalesce(F.expr(rule.failed_when), F.lit(False))

def flag_drop_rules(df: DataFrame, dataset: str) -> DataFrame:
    """Add the name of the first drop rule a row breaks, or null for a valid row."""
    first = F.coalesce(*[F.when(failed(r), F.lit(r.name)) for r in c.rules_for(dataset, c.DROP)])
    return df.withColumn(c.FAILED_RULE, first)

def rank_duplicates(df: DataFrame, keys: list, newest: list) -> DataFrame:
    """Rank rows per key with valid rows first and the newest first; rank 1 is the row kept."""
    order = [F.col(c.FAILED_RULE).isNull().desc()] + [F.col(col).desc() for col in newest]
    return df.withColumn(c.ROW_RANK, F.row_number().over(Window.partitionBy(*keys).orderBy(*order)))

def flag_known_stations(df: DataFrame, stations: DataFrame) -> DataFrame:
    """Broadcast join against the stations list and add station_known."""
    codes = stations.select("station_code").distinct().withColumn("station_known", F.lit(True))
    joined = df.join(F.broadcast(codes), "station_code", "left")
    return joined.withColumn("station_known", F.coalesce(F.col("station_known"), F.lit(False)))

def stage_services(raw: DataFrame, stations: DataFrame) -> DataFrame:
    """One pass over a raw batch: rules, station check, report month and duplicate rank."""
    checked = flag_drop_rules(flag_known_stations(raw, stations), "services")
    ranked = rank_duplicates(checked, c.SERVICES_KEY, [c.INGESTED_AT, "stop_rdt_id"])
    return ranked.withColumn("service_month", F.date_format("service_date", c.MONTH_FORMAT))

def stage_disruptions(raw: DataFrame) -> DataFrame:
    """Rules, local month, elapsed minutes and duplicate rank for the disruptions snapshot."""
    elapsed = F.round((F.unix_timestamp("end_time") - F.unix_timestamp("start_time")) / 60).cast("int")
    enriched = raw.withColumns(
        {"start_month": F.date_format("start_time", c.MONTH_FORMAT), "elapsed_minutes": elapsed}
    )
    return rank_duplicates(flag_drop_rules(enriched, "disruptions"), c.DISRUPTIONS_KEY, [c.INGESTED_AT])

def kept_rows(staged: DataFrame) -> DataFrame:
    """Valid rows that survive deduplication, without working columns."""
    return staged.filter(F.col(c.FAILED_RULE).isNull() & (F.col(c.ROW_RANK) == 1)).drop(c.FAILED_RULE, c.ROW_RANK)

def quarantined_rows(staged: DataFrame, dataset: str, columns: list) -> DataFrame:
    """Rows that broke a drop rule, with the rule and the original values as JSON."""
    return staged.filter(F.col(c.FAILED_RULE).isNotNull()).select(
        F.lit(dataset).alias("dataset"),
        F.col(c.FAILED_RULE).alias("rule"),
        F.col(c.FILE_NAME).alias("file_name"),
        F.col(c.RUN_ID).alias("run_id"),
        F.to_json(F.struct(*columns)).alias("row_json"),
        F.current_timestamp().alias("quarantined_at"),
    )

def balance_per_file(staged: DataFrame, dataset: str) -> DataFrame:
    """Rows in, kept, quarantined, duplicates and warnings per file, in one aggregation."""
    is_dropped = F.col(c.FAILED_RULE).isNotNull()
    is_dup = ~is_dropped & (F.col(c.ROW_RANK) > 1)
    warns = {
        f"warn_{r.name}": F.sum(failed(r).cast("long")) for r in c.rules_for(dataset, c.WARN)
    }
    agg = staged.groupBy(c.FILE_NAME).agg(
        F.count("*").alias("raw_rows"),
        F.sum((~is_dropped & ~is_dup).cast("long")).alias("clean_rows"),
        F.sum(is_dropped.cast("long")).alias("quarantined_rows"),
        F.sum(is_dup.cast("long")).alias("duplicate_rows"),
        *[v.alias(k) for k, v in warns.items()],
    )
    total = F.col("clean_rows") + F.col("quarantined_rows") + F.col("duplicate_rows")
    return agg.withColumn("balanced", F.col("raw_rows") == total)
