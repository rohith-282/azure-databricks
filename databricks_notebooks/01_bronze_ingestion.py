# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Data Ingestion
# MAGIC Reads raw data from ADLS Gen2 and writes to Delta Lake bronze layer.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from utils.spark_utils import get_spark_session, read_config
from utils.validation import validate_row_count
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Configuration
config = read_config("/dbfs/config/config.json")

STORAGE_ACCOUNT   = config["storage_account"]
RAW_CONTAINER     = config["raw_container"]
BRONZE_CONTAINER  = config["bronze_container"]
ADLS_PATH         = f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_PATH       = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# COMMAND ----------

spark = get_spark_session()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest CSV — Orders

def ingest_csv_orders():
    logger.info("Starting CSV ingestion: orders")

    raw_path = f"{ADLS_PATH}/orders/"
    bronze_path = f"{BRONZE_PATH}/orders/"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(raw_path)
    )

    # Add audit columns
    df = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_layer", lit("bronze"))
    )

    source_count = df.count()
    logger.info(f"Records read from source: {source_count}")

    # Write to Delta Lake
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_timestamp")
        .save(bronze_path)
    )

    # Validate
    validate_row_count(spark, bronze_path, source_count, layer="bronze", entity="orders")
    logger.info("CSV ingestion complete: orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest JSON — Events

def ingest_json_events():
    logger.info("Starting JSON ingestion: events")

    raw_path = f"{ADLS_PATH}/events/"
    bronze_path = f"{BRONZE_PATH}/events/"

    df = (
        spark.read
        .option("multiLine", "true")
        .json(raw_path)
    )

    df = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_layer", lit("bronze"))
    )

    source_count = df.count()
    logger.info(f"Records read from source: {source_count}")

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(bronze_path)
    )

    validate_row_count(spark, bronze_path, source_count, layer="bronze", entity="events")
    logger.info("JSON ingestion complete: events")

# COMMAND ----------

if __name__ == "__main__":
    ingest_csv_orders()
    ingest_json_events()
    logger.info("Bronze layer ingestion finished.")
