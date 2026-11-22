import os

import pytest

from trackledger import contract as c
from trackledger import ingest
from trackledger.storage import get_spark

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "landing")

def fixture_path(folder: str) -> str:
    """Path of the single fixture file in a landing folder."""
    directory = os.path.join(FIXTURES, folder)
    return os.path.join(directory, os.listdir(directory)[0])

@pytest.fixture(scope="session")
def spark():
    return get_spark()

@pytest.fixture(scope="session")
def raw_services(spark):
    df = ingest.read_csv_batch(spark, [fixture_path("services")], c.SERVICES_COLUMNS, c.SERVICES_TS_FORMAT, False)
    return ingest.add_ingest_metadata(df, "test")

@pytest.fixture(scope="session")
def raw_disruptions(spark):
    df = ingest.read_csv_batch(
        spark, [fixture_path("disruptions")], c.DISRUPTIONS_COLUMNS, c.DISRUPTIONS_TS_FORMAT, False
    )
    return ingest.add_ingest_metadata(df, "test")

@pytest.fixture(scope="session")
def stations(spark):
    return ingest.read_csv_batch(spark, [fixture_path("stations")], c.STATIONS_COLUMNS, c.DATE_FORMAT, False)
