"""
spark_utils.py — Shared PySpark helper functions
"""

import json
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "AzureDataPipeline") -> SparkSession:
    """Return an active SparkSession or create one."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )


def read_config(config_path: str) -> dict:
    """Read pipeline config from a JSON file."""
    with open(config_path, "r") as f:
        return json.load(f)


def get_jdbc_url(config: dict) -> str:
    """Build JDBC connection string for Azure SQL Database."""
    server   = config["sql_server"]
    database = config["sql_database"]
    user     = config["sql_user"]
    password = config["sql_password"]
    return (
        f"jdbc:sqlserver://{server}.database.windows.net:1433;"
        f"database={database};user={user}@{server};"
        f"password={password};encrypt=true;"
        f"trustServerCertificate=false;loginTimeout=30;"
    )


def optimize_delta_table(spark: SparkSession, path: str, z_order_cols: list = None):
    """
    Run OPTIMIZE on a Delta table with optional Z-ordering.
    Z-ordering improves query performance on high-cardinality columns.
    """
    if z_order_cols:
        cols = ", ".join(z_order_cols)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")


def vacuum_delta_table(spark: SparkSession, path: str, retention_hours: int = 168):
    """Remove old Delta files beyond the retention window (default 7 days)."""
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")


def broadcast_join(spark, df_large, df_small, join_key: str, join_type: str = "inner"):
    """
    Perform a broadcast join — broadcasts the smaller DataFrame to all executors
    to avoid a shuffle on the large DataFrame. Significant performance improvement
    when df_small is under ~200MB.
    """
    from pyspark.sql.functions import broadcast
    return df_large.join(broadcast(df_small), join_key, join_type)


def apply_partitioned_cache(df, partition_col: str):
    """
    Repartition a DataFrame by a column and cache it in memory.
    Useful when the same DataFrame is read multiple times in a notebook.
    """
    return df.repartition(partition_col).cache()
