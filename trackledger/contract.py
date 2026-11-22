from dataclasses import dataclass

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Unity Catalog names and storage locations
CATALOG = "workspace"
SCHEMA = "trackledger"
VOLUME = "landing"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
LANDING_SERVICES = "services"
LANDING_DISRUPTIONS = "disruptions"
LANDING_STATIONS = "stations"
CHECKPOINT_DIR = "_checkpoints"
LOCAL_LANDING = "data/landing"
LOCAL_WAREHOUSE = ".warehouse"
REFERENCE_FILE = "reference/published_stats.csv"
LOCAL_EXPECTED_FILE = "results/local_expected.json"

# Table names (unqualified; the store adds catalog and schema)
RAW_SERVICES = "raw_services"
RAW_DISRUPTIONS = "raw_disruptions"
REF_STATIONS = "ref_stations"
STAGE_SERVICES = "stage_services"
CLEAN_SERVICES = "clean_services"
CLEAN_DISRUPTIONS = "clean_disruptions"
QUARANTINE = "quarantine"
FILE_LEDGER = "file_ledger"
RUN_LOG = "run_log"
REPORT_PUNCTUALITY = "report_punctuality_monthly"
REPORT_OPERATORS = "report_operator_monthly"
REPORT_DISRUPTIONS = "report_disruptions_monthly"
RECONCILIATION = "reconciliation"

# Time: every timestamp is shown and grouped in Dutch local time
LOCAL_TZ = "Europe/Amsterdam"
SERVICES_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"
DISRUPTIONS_TS_FORMAT = "yyyy-MM-dd HH:mm:ss"
DATE_FORMAT = "yyyy-MM-dd"
MONTH_FORMAT = "yyyy-MM"
REPORT_YEAR = "2025"

# Metadata columns added at ingest
RESCUED = "_rescued_data"
SOURCE_FILE = "_source_file"
FILE_NAME = "_file_name"
INGESTED_AT = "_ingested_at"
RUN_ID = "_run_id"
FAILED_RULE = "_failed_rule"
ROW_RANK = "_row_rank"

# Ingest modes and run limits
MODE_AUTOLOADER = "autoloader"
MODE_BATCH = "batch"
LANDING_SUFFIXES = (".csv", ".csv.gz")
QUARANTINE_FAIL_SHARE = 0.01
CSV_OPTIONS = {"header": "true", "sep": ",", "quote": '"', "escape": '"', "encoding": "UTF-8"}

# Source columns, in file order: (header name in the file, column name in the lakehouse, type)
SERVICES_COLUMNS = [
    ("Service:RDT-ID", "service_rdt_id", LongType()),
    ("Service:Date", "service_date", DateType()),
    ("Service:Type", "service_type", StringType()),
    ("Service:Company", "operator", StringType()),
    ("Service:Train number", "train_number", IntegerType()),
    ("Service:Completely cancelled", "service_completely_cancelled", BooleanType()),
    ("Service:Partly cancelled", "service_partly_cancelled", BooleanType()),
    ("Service:Maximum delay", "service_max_delay_min", IntegerType()),
    ("Stop:RDT-ID", "stop_rdt_id", LongType()),
    ("Stop:Station code", "station_code", StringType()),
    ("Stop:Station name", "station_name", StringType()),
    ("Stop:Arrival time", "arrival_time", TimestampType()),
    ("Stop:Arrival delay", "arrival_delay_min", IntegerType()),
    ("Stop:Arrival cancelled", "arrival_cancelled", BooleanType()),
    ("Stop:Departure time", "departure_time", TimestampType()),
    ("Stop:Departure delay", "departure_delay_min", IntegerType()),
    ("Stop:Departure cancelled", "departure_cancelled", BooleanType()),
    ("Stop:Platform change", "platform_change", BooleanType()),
    ("Stop:Planned platform", "planned_platform", StringType()),
    ("Stop:Actual platform", "actual_platform", StringType()),
]
DISRUPTIONS_COLUMNS = [
    ("rdt_id", "rdt_id", LongType()),
    ("ns_lines", "ns_lines", StringType()),
    ("rdt_lines", "rdt_lines", StringType()),
    ("rdt_lines_id", "rdt_lines_id", StringType()),
    ("rdt_station_names", "rdt_station_names", StringType()),
    ("rdt_station_codes", "rdt_station_codes", StringType()),
    ("cause_nl", "cause_nl", StringType()),
    ("cause_en", "cause_en", StringType()),
    ("statistical_cause_nl", "statistical_cause_nl", StringType()),
    ("statistical_cause_en", "statistical_cause_en", StringType()),
    ("cause_group", "cause_group", StringType()),
    ("start_time", "start_time", TimestampType()),
    ("end_time", "end_time", TimestampType()),
    ("duration_minutes", "duration_minutes", IntegerType()),
]
STATIONS_COLUMNS = [
    ("id", "station_id", IntegerType()),
    ("code", "station_code", StringType()),
    ("uic", "uic", StringType()),
    ("name_short", "name_short", StringType()),
    ("name_medium", "name_medium", StringType()),
    ("name_long", "name_long", StringType()),
    ("slug", "slug", StringType()),
    ("country", "country", StringType()),
    ("type", "station_type", StringType()),
    ("geo_lat", "geo_lat", DoubleType()),
    ("geo_lng", "geo_lng", DoubleType()),
]

def source_schema(columns: list) -> StructType:
    """Read schema that uses the exact header names, so Auto Loader can match columns by name."""
    return StructType([StructField(src, dtype, True) for src, _, dtype in columns])

# Keys
SERVICES_KEY = ["service_rdt_id", "station_code"]
DISRUPTIONS_KEY = ["rdt_id"]

@dataclass(frozen=True)
class Rule:
    """A data quality rule; `failed_when` is a Spark SQL predicate that is true for a bad row."""

    name: str
    dataset: str
    severity: str
    failed_when: str
    description: str

DROP = "drop"
WARN = "warn"

RULES = [
    Rule("unparsed_fields", "services", DROP, f"{RESCUED} IS NOT NULL", "A value did not fit the schema."),
    Rule("missing_key", "services", DROP, "service_rdt_id IS NULL OR station_code IS NULL", "No service or station."),
    Rule("missing_service_date", "services", DROP, "service_date IS NULL", "No operating day."),
    Rule(
        "delay_out_of_range",
        "services",
        DROP,
        "arrival_delay_min NOT BETWEEN 0 AND 1440 OR departure_delay_min NOT BETWEEN 0 AND 1440",
        "Delay outside 0 to 1440 minutes.",
    ),
    Rule("unknown_station", "services", WARN, "NOT station_known", "Station code not in the stations list."),
    Rule("unparsed_fields", "disruptions", DROP, f"{RESCUED} IS NOT NULL", "A value did not fit the schema."),
    Rule("missing_key", "disruptions", DROP, "rdt_id IS NULL OR start_time IS NULL", "No id or start time."),
    Rule("no_end_time", "disruptions", WARN, "end_time IS NULL", "Disruption has no end time and no duration."),
    Rule(
        "duration_mismatch",
        "disruptions",
        WARN,
        "duration_minutes <> elapsed_minutes",
        "Stated duration differs from end minus start in local time.",
    ),
]

def rules_for(dataset: str, severity: str) -> list:
    """Rules of one dataset and severity, in contract order."""
    return [r for r in RULES if r.dataset == dataset and r.severity == severity]

# KPI definitions (internal definitions, documented in docs/architecture.md)
ON_TIME_LIMIT_MIN = 3
MIN_DELAYED_SHARE = 0.01
NON_TRAIN_SERVICE = "lower(service_type) LIKE '%bus%'"
CAUSE_FIELD = "statistical_cause_en"
CAUSE_PREFIX = "cause_count:"
METRIC_COUNT = "disruption_count"
METRIC_AVG_DURATION = "avg_duration_minutes"

# Reconciliation tolerances, fixed before the first run
TOLERANCES = {METRIC_COUNT: 0.0, METRIC_AVG_DURATION: 1.0, CAUSE_PREFIX: 0.0}
STATUS_PASS = "pass"
STATUS_FAIL = "fail"
STATUS_EXPLAINED = "explained"
KNOWN_GAPS = {
    (REPORT_YEAR, f"{CAUSE_PREFIX}technical investigation"): (
        "The site's own monthly pages also add up to 290, and every monthly total matches the open "
        "data exactly, so 12 records carry a different cause label, not a different month. The open "
        "data is a yearly snapshot ('The data is updated yearly') while the statistics pages are "
        "computed from the live archive, so the most likely reason is a cause revised after the snapshot."
    ),
}

TABLE_COMMENTS = {
    RAW_SERVICES: "Append-only copy of the monthly services files, one row per train stop.",
    RAW_DISRUPTIONS: "Snapshot of the yearly disruptions file as delivered.",
    REF_STATIONS: "Station reference list used for the station-code check.",
    STAGE_SERVICES: "Working table for the current services batch, rebuilt each run.",
    CLEAN_SERVICES: "Typed, checked and deduplicated train stops, merged on service id and station.",
    CLEAN_DISRUPTIONS: "Checked and deduplicated disruptions with local-time month.",
    QUARANTINE: "Rows removed by a drop rule, with the rule name and the original row.",
    FILE_LEDGER: "Row balance per landing file and stage.",
    RUN_LOG: "One row per task run with rows in, rows out and status.",
    REPORT_PUNCTUALITY: "Monthly arrival punctuality and cancellations for train services.",
    REPORT_OPERATORS: "Monthly arrival punctuality and cancellations per operator.",
    REPORT_DISRUPTIONS: "Disruptions per month and cause group.",
    RECONCILIATION: "Our disruption figures against the published Rijden de Treinen statistics.",
}
COLUMN_COMMENTS = {
    CLEAN_SERVICES: {
        "service_date": "Operating day of the service; month KPIs use this date.",
        "arrival_delay_min": "Arrival delay in whole minutes as published.",
        "station_known": "True when the station code is in the stations list.",
    },
    REPORT_OPERATORS: {
        "has_delay_data": f"False when under {MIN_DELAYED_SHARE:.0%} of arrivals show any delay (no live delay feed).",
    },
    REPORT_PUNCTUALITY: {
        "on_time_share": f"Share of measured arrivals less than {ON_TIME_LIMIT_MIN} minutes late.",
        "cancelled_share": "Share of planned arrivals that were cancelled.",
    },
    RECONCILIATION: {
        "tolerance": "Allowed absolute difference, fixed before the first run.",
        "status": "pass, fail, or explained for a documented known gap.",
    },
}
