"""
test_validation.py — Unit tests for data quality validation functions
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'databricks_notebooks'))
from utils.validation import validate_nulls, validate_duplicates, validate_schema


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("validation_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def test_validate_nulls_passes(spark):
    data   = [("ORD-001", "CUST-1"), ("ORD-002", "CUST-2")]
    schema = StructType([
        StructField("order_id",    StringType()),
        StructField("customer_id", StringType()),
    ])
    df = spark.createDataFrame(data, schema)
    assert validate_nulls(df, ["order_id", "customer_id"]) is True


def test_validate_nulls_fails(spark):
    data   = [("ORD-001", None), ("ORD-002", "CUST-2")]
    schema = StructType([
        StructField("order_id",    StringType()),
        StructField("customer_id", StringType()),
    ])
    df = spark.createDataFrame(data, schema)
    with pytest.raises(ValueError, match="Null check FAILED"):
        validate_nulls(df, ["customer_id"])


def test_validate_duplicates_passes(spark):
    data   = [("ORD-001",), ("ORD-002",)]
    schema = StructType([StructField("order_id", StringType())])
    df = spark.createDataFrame(data, schema)
    assert validate_duplicates(df, "order_id") is True


def test_validate_duplicates_fails(spark):
    data   = [("ORD-001",), ("ORD-001",)]
    schema = StructType([StructField("order_id", StringType())])
    df = spark.createDataFrame(data, schema)
    with pytest.raises(ValueError, match="Duplicate check FAILED"):
        validate_duplicates(df, "order_id")


def test_validate_schema_passes(spark):
    data   = [("ORD-001", "CUST-1")]
    schema = StructType([
        StructField("order_id",    StringType()),
        StructField("customer_id", StringType()),
    ])
    df = spark.createDataFrame(data, schema)
    assert validate_schema(df, ["order_id", "customer_id"]) is True


def test_validate_schema_fails(spark):
    data   = [("ORD-001",)]
    schema = StructType([StructField("order_id", StringType())])
    df = spark.createDataFrame(data, schema)
    with pytest.raises(ValueError, match="Schema check FAILED"):
        validate_schema(df, ["order_id", "customer_id"])
