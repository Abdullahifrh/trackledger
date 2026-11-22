import os
import shutil
import sys

import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from trackledger import contract as c

try:
    from databricks.sdk.runtime import dbutils
except ImportError:
    dbutils = None

def repo_root() -> str:
    """Folder that holds the trackledger package, the same on Databricks and locally."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def on_databricks() -> bool:
    """True when the code runs on Databricks compute."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

def get_spark() -> SparkSession:
    """Return the one SparkSession, with the session time zone set to Dutch local time."""
    if on_databricks():
        spark = SparkSession.builder.getOrCreate()
    else:
        # Use the pinned pip PySpark even if an older Spark install sets SPARK_HOME, and this interpreter
        os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("trackledger")
            .config("spark.driver.memory", "5g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.csv.parser.columnPruning.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", c.LOCAL_TZ)
    return spark

class Store:
    """Unity Catalog tables in workspace.trackledger."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def name(self, table: str) -> str:
        return f"{c.CATALOG}.{c.SCHEMA}.{table}"

    def landing(self, folder: str) -> str:
        return f"{c.VOLUME_ROOT}/{folder}"

    def checkpoint(self, table: str) -> str:
        return f"{c.VOLUME_ROOT}/{c.CHECKPOINT_DIR}/{table}"

    def exists(self, table: str) -> bool:
        return self.spark.catalog.tableExists(self.name(table))

    def read(self, table: str) -> DataFrame:
        return self.spark.read.table(self.name(table))

    def write(self, df: DataFrame, table: str, mode: str) -> None:
        schema_option = "overwriteSchema" if mode == "overwrite" else "mergeSchema"
        df.write.mode(mode).option(schema_option, "true").saveAsTable(self.name(table))
        self.comment(table)

    def comment(self, table: str) -> None:
        """Apply table and column comments from the contract."""
        text = c.TABLE_COMMENTS.get(table, "").replace("'", "")
        self.spark.sql(f"COMMENT ON TABLE {self.name(table)} IS '{text}'")
        for column, note in c.COLUMN_COMMENTS.get(table, {}).items():
            self.spark.sql(f"ALTER TABLE {self.name(table)} ALTER COLUMN {column} COMMENT '{note}'")

    def upsert(self, batch: DataFrame, table: str, keys: list, newer: str) -> dict:
        """Idempotent Delta MERGE: insert new keys, update a key only when the batch row is newer."""
        if not self.exists(table):
            self.write(batch.limit(0), table, "overwrite")
        batch.createOrReplaceTempView("trackledger_batch")
        on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        result = self.spark.sql(
            f"MERGE INTO {self.name(table)} t USING trackledger_batch s ON {on} "
            f"WHEN MATCHED AND s.{newer} > t.{newer} THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        ).first()
        return {"inserted": int(result["num_inserted_rows"]), "updated": int(result["num_updated_rows"])}

class LocalStore(Store):
    """Parquet folders under a local warehouse directory, for tests and the local reference run."""

    def __init__(self, spark: SparkSession, root: str, landing_root: str):
        super().__init__(spark)
        self.root = os.path.abspath(root)
        self.landing_root = os.path.abspath(landing_root)

    def name(self, table: str) -> str:
        return os.path.join(self.root, table)

    def landing(self, folder: str) -> str:
        return os.path.join(self.landing_root, folder)

    def exists(self, table: str) -> bool:
        return os.path.isdir(self.name(table))

    def read(self, table: str) -> DataFrame:
        return self.spark.read.parquet(self.name(table))

    def write(self, df: DataFrame, table: str, mode: str) -> None:
        if mode == "overwrite" and self.exists(table):
            tmp = self.name(table) + "__tmp"
            df.write.mode("overwrite").parquet(tmp)
            shutil.rmtree(self.name(table))
            os.rename(tmp, self.name(table))
        else:
            df.write.mode(mode).option("mergeSchema", "true").parquet(self.name(table))

    def upsert(self, batch: DataFrame, table: str, keys: list, newer: str) -> dict:
        """Same outcome as the Delta MERGE, built from a join plus overwrite."""
        if not self.exists(table):
            self.write(batch, table, "overwrite")
            return {"inserted": batch.count(), "updated": 0}
        target = self.read(table)
        matched = batch.alias("s").join(target.alias("t"), keys, "inner")
        updated = matched.filter(F.col(f"s.{newer}") > F.col(f"t.{newer}")).count()
        new_rows = batch.join(target.select(*keys), keys, "left_anti")
        inserted = new_rows.count()
        if updated:
            newer_rows = matched.filter(F.col(f"s.{newer}") > F.col(f"t.{newer}")).select("s.*")
            kept = target.join(newer_rows.select(*keys), keys, "left_anti")
            self.write(kept.unionByName(newer_rows).unionByName(new_rows), table, "overwrite")
        elif inserted:
            new_rows.write.mode("append").parquet(self.name(table))
        return {"inserted": inserted, "updated": updated}

def task_param(name: str, default: str) -> str:
    """Read a Databricks job or widget parameter, or an environment variable locally."""
    if on_databricks():
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name) or default
    return os.environ.get(f"TRACKLEDGER_{name.upper()}", default)

def get_store(spark: SparkSession) -> Store:
    """Pick the store for the current platform."""
    if on_databricks():
        return Store(spark)
    root = os.environ.get("TRACKLEDGER_WAREHOUSE", c.LOCAL_WAREHOUSE)
    landing = os.environ.get("TRACKLEDGER_LANDING", c.LOCAL_LANDING)
    return LocalStore(spark, root, landing)
