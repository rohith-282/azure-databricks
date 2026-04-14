"""
test_transformations.py — Unit tests for Silver layer transformations
Run with: pytest tests/
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import col
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'databricks_notebooks'))
from utils.spark_utils import get_spark_session


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("unit_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_orders(spark):
    data = [
        ("ORD-001", "CUST-101", "2025-01-05", "Laptop",  89999.0, "completed"),
        ("ORD-002", "CUST-102", "2025-01-06", "Mouse",    1299.0, "COMPLETED"),
        ("ORD-003", None,       "2025-01-07", "Keyboard", 4599.0, "completed"),  # null customer
        ("ORD-001", "CUST-101", "2025-01-05", "Laptop",  89999.0, "completed"),  # duplicate
    ]
    schema = StructType([
        StructField("order_id",     StringType()),
        StructField("customer_id",  StringType()),
        StructField("order_date",   StringType()),
        StructField("product_name", StringType()),
        StructField("amount",       DoubleType()),
        StructField("status",       StringType()),
    ])
    return spark.createDataFrame(data, schema)


def test_deduplication(spark, sample_orders):
    """Deduplication on order_id should reduce 4 rows to 3."""
    df = sample_orders.dropDuplicates(["order_id"])
    assert df.count() == 3


def test_null_customer_filter(spark, sample_orders):
    """Rows with null customer_id must be dropped."""
    df = sample_orders.filter(col("customer_id").isNotNull())
    assert df.count() == 3


def test_status_uppercased(spark, sample_orders):
    """Status column should be uppercased."""
    from pyspark.sql.functions import upper, trim
    df = sample_orders.withColumn("status", upper(trim(col("status"))))
    statuses = [r.status for r in df.collect()]
    assert all(s == s.upper() for s in statuses)


def test_amount_positive(spark, sample_orders):
    """All amounts in the fixture are positive."""
    df = sample_orders.filter(col("amount") > 0)
    assert df.count() == sample_orders.count()


def test_no_empty_order_ids(spark, sample_orders):
    """order_id should not be null or empty string."""
    from pyspark.sql.functions import trim
    df = sample_orders.filter(trim(col("order_id")) != "")
    assert df.count() == sample_orders.count()
