# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Data Transformation & Cleansing
# MAGIC Reads from Bronze Delta tables, applies transformations, writes to Silver.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, to_date, to_timestamp,
    when, isnan, isnull, regexp_replace, current_timestamp, lit
)
from pyspark.sql.types import DoubleType, IntegerType, StringType
from delta.tables import DeltaTable
from utils.spark_utils import get_spark_session, read_config
from utils.validation import validate_row_count, validate_nulls
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

config = read_config("/dbfs/config/config.json")
STORAGE_ACCOUNT  = config["storage_account"]
BRONZE_CONTAINER = config["bronze_container"]
SILVER_CONTAINER = config["silver_container"]

BRONZE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

spark = get_spark_session()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Orders

def transform_orders():
    logger.info("Transforming orders: bronze → silver")

    df = spark.read.format("delta").load(f"{BRONZE_PATH}/orders/")

    # Deduplicate
    df = df.dropDuplicates(["order_id"])

    # Clean and cast columns
    df = (
        df
        .withColumn("order_id",     trim(col("order_id")))
        .withColumn("customer_id",  trim(col("customer_id")))
        .withColumn("order_date",   to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("amount",       col("amount").cast(DoubleType()))
        .withColumn("status",       upper(trim(col("status"))))
        .withColumn("product_name", trim(col("product_name")))
        # Remove nulls in critical fields
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("amount") > 0)
        # Add silver audit columns
        .withColumn("_silver_timestamp", current_timestamp())
        .withColumn("_layer", lit("silver"))
        # Drop bronze audit columns
        .drop("_ingestion_timestamp", "_source_file")
    )

    validate_nulls(df, critical_cols=["order_id", "customer_id", "order_date"])

    # Upsert into Silver Delta table (merge)
    silver_orders_path = f"{SILVER_PATH}/orders/"

    if DeltaTable.isDeltaTable(spark, silver_orders_path):
        delta_table = DeltaTable.forPath(spark, silver_orders_path)
        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                "target.order_id = source.order_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("order_date")
            .save(silver_orders_path)
        )

    final_count = spark.read.format("delta").load(silver_orders_path).count()
    logger.info(f"Silver orders table count: {final_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Events

def transform_events():
    logger.info("Transforming events: bronze → silver")

    df = spark.read.format("delta").load(f"{BRONZE_PATH}/events/")

    df = df.dropDuplicates(["event_id"])

    df = (
        df
        .withColumn("event_id",        trim(col("event_id")))
        .withColumn("user_id",         trim(col("user_id")))
        .withColumn("event_type",      lower(trim(col("event_type"))))
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("page_url",        regexp_replace(col("page_url"), " ", ""))
        .filter(col("event_id").isNotNull())
        .filter(col("user_id").isNotNull())
        .withColumn("_silver_timestamp", current_timestamp())
        .withColumn("_layer", lit("silver"))
        .drop("_ingestion_timestamp", "_source_file")
    )

    validate_nulls(df, critical_cols=["event_id", "user_id", "event_timestamp"])

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_type")
        .save(f"{SILVER_PATH}/events/")
    )

    logger.info("Events silver layer complete.")

# COMMAND ----------

if __name__ == "__main__":
    transform_orders()
    transform_events()
    logger.info("Silver transformation complete.")
