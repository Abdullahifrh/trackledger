import os
from datetime import datetime

import pytest

from trackledger.storage import LocalStore

@pytest.mark.skipif(
    os.name == "nt",
    reason="Spark needs Hadoop winutils.exe to write Parquet on Windows; CI runs this on Linux",
)
def test_local_upsert_is_idempotent(spark, tmp_path):
    store = LocalStore(spark, str(tmp_path), str(tmp_path))
    schema = "k long, v string, t timestamp"
    first = spark.createDataFrame([(1, "a", datetime(2025, 1, 1)), (2, "b", datetime(2025, 1, 1))], schema)
    assert store.upsert(first, "t1", ["k"], "t") == {"inserted": 2, "updated": 0}
    assert store.upsert(first, "t1", ["k"], "t") == {"inserted": 0, "updated": 0}
    newer = spark.createDataFrame([(2, "b2", datetime(2025, 2, 1)), (3, "c", datetime(2025, 2, 1))], schema)
    assert store.upsert(newer, "t1", ["k"], "t") == {"inserted": 1, "updated": 1}
    assert sorted(tuple(r)[:2] for r in store.read("t1").collect()) == [(1, "a"), (2, "b2"), (3, "c")]
