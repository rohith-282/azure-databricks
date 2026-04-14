"""
validation.py — Data quality checks for each pipeline layer
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull

logger = logging.getLogger(__name__)


def validate_row_count(
    spark: SparkSession,
    delta_path: str,
    expected_count: int,
    layer: str,
    entity: str,
    tolerance: float = 0.01
) -> bool:
    """
    Check that the written Delta table has at least
    (1 - tolerance) * expected_count rows.
    Raises ValueError if check fails.
    """
    actual_count = spark.read.format("delta").load(delta_path).count()
    min_expected = int(expected_count * (1 - tolerance))

    if actual_count < min_expected:
        msg = (
            f"[{layer.upper()}] Row count check FAILED for {entity}. "
            f"Expected >= {min_expected}, got {actual_count}."
        )
        logger.error(msg)
        raise ValueError(msg)

    logger.info(
        f"[{layer.upper()}] Row count OK for {entity}: "
        f"{actual_count} rows (expected ~{expected_count})."
    )
    return True


def validate_nulls(df: DataFrame, critical_cols: list) -> bool:
    """
    Ensure that critical columns contain no null values.
    Raises ValueError listing all offending columns.
    """
    null_report = {}
    for c in critical_cols:
        null_count = df.filter(col(c).isNull() | isnan(col(c))).count()
        if null_count > 0:
            null_report[c] = null_count

    if null_report:
        msg = f"Null check FAILED. Nulls found: {null_report}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info(f"Null check passed for columns: {critical_cols}")
    return True


def validate_schema(df: DataFrame, expected_columns: list) -> bool:
    """
    Check that all expected columns are present in the DataFrame.
    """
    actual_cols   = set(df.columns)
    expected_cols = set(expected_columns)
    missing = expected_cols - actual_cols

    if missing:
        msg = f"Schema check FAILED. Missing columns: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Schema check passed.")
    return True


def validate_duplicates(df: DataFrame, unique_key: str) -> bool:
    """
    Ensure no duplicate values exist on the unique key column.
    """
    total       = df.count()
    distinct    = df.select(unique_key).distinct().count()

    if total != distinct:
        duplicates = total - distinct
        msg = f"Duplicate check FAILED on '{unique_key}': {duplicates} duplicate rows found."
        logger.error(msg)
        raise ValueError(msg)

    logger.info(f"Duplicate check passed on '{unique_key}'.")
    return True


def log_pipeline_stats(df: DataFrame, layer: str, entity: str):
    """Print a quick summary of the DataFrame for pipeline observability."""
    row_count = df.count()
    col_count = len(df.columns)
    logger.info(
        f"[{layer.upper()}] {entity}: {row_count:,} rows, {col_count} columns"
    )
    return {"rows": row_count, "columns": col_count}
