from decimal import Decimal
from pathlib import Path
import sys
import pytest
from pyspark.sql import SparkSession
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.pipeline import pipeline

@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[2]").appName("RetailTest").getOrCreate()
    yield session
    session.stop()

def test_sample(spark):
    overall, categories = pipeline(spark, Path(__file__).resolve().parents[1] / "data")
    row = overall.first()
    assert row.units == 18
    assert row.orders == 4
    assert row.customers == 3
    assert row.revenue == Decimal("2395.50")
    assert row.cost == Decimal("1125.00")
    assert row.margin == Decimal("1270.50")
    assert categories.count() == 3
