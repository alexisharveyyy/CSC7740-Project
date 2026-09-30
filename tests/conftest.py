import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Makes the pipeline modules importable without a pytest.ini at the repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


# Module scope matches the fixtures in the existing test files, which stop their session when done
@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("backblaze-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
