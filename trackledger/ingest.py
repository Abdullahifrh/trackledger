from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from trackledger import contract as c

def file_name(path: Column) -> Column:
    """Last part of a file path, used as the stable file key across platforms."""
    return F.element_at(F.split(path, "/"), -1)

def rename_columns(df: DataFrame, columns: list) -> DataFrame:
    """Rename header names to lakehouse names and keep the rescued column if present."""
    renamed = [F.col(f"`{src}`").alias(dst) for src, dst, _ in columns]
    extra = [F.col(c.RESCUED)] if c.RESCUED in df.columns else [F.lit(None).cast("string").alias(c.RESCUED)]
    return df.select(*renamed, *extra, F.col("_metadata.file_path").alias(c.SOURCE_FILE))

def add_ingest_metadata(df: DataFrame, run_id: str) -> DataFrame:
    """Add file name, ingest time and run id."""
    return df.select(
        "*",
        file_name(F.col(c.SOURCE_FILE)).alias(c.FILE_NAME),
        F.current_timestamp().alias(c.INGESTED_AT),
        F.lit(run_id).alias(c.RUN_ID),
    )

def read_csv_batch(spark: SparkSession, paths: list, columns: list, ts_format: str, databricks: bool) -> DataFrame:
    """Batch CSV read with an explicit schema; bad values land in the rescued column."""
    reader = spark.read.options(**c.CSV_OPTIONS, timestampFormat=ts_format, dateFormat=c.DATE_FORMAT)
    schema = c.source_schema(columns)
    if databricks:
        reader = reader.option("rescuedDataColumn", c.RESCUED)
    else:
        schema = StructType(schema.fields + [StructField(c.RESCUED, StringType(), True)])
        reader = reader.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", c.RESCUED)
    return rename_columns(reader.schema(schema).csv(paths), columns)

def read_services_stream(spark: SparkSession, landing_dir: str, schema_dir: str) -> DataFrame:
    """Auto Loader stream over the services folder; only files it has not seen are read."""
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_dir)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", c.RESCUED)
        .options(**c.CSV_OPTIONS)
        .option("timestampFormat", c.SERVICES_TS_FORMAT)
        .option("dateFormat", c.DATE_FORMAT)
        .schema(c.source_schema(c.SERVICES_COLUMNS))
        .load(landing_dir)
    )
    return rename_columns(stream, c.SERVICES_COLUMNS)

def landing_line_counts(spark: SparkSession, paths: list) -> DataFrame:
    """Data lines per file counted as plain text, independent of the CSV parser."""
    lines = spark.read.text(paths).select(file_name(F.col("_metadata.file_path")).alias(c.FILE_NAME))
    return lines.groupBy(c.FILE_NAME).agg((F.count("*") - 1).alias("landing_rows"))

def rows_per_file(df: DataFrame, alias: str) -> DataFrame:
    """Row count per landing file."""
    return df.groupBy(c.FILE_NAME).agg(F.count("*").alias(alias))
