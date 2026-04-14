# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business Aggregations
# MAGIC Reads Silver Delta tables, builds analytics-ready aggregates, writes to Gold + Azure SQL.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, avg, max as _max, min as _min,
    date_trunc, countDistinct, current_timestamp, lit, round as _round
)
from utils.spark_utils import get_spark_session, read_config, get_jdbc_url
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

config     = read_config("/dbfs/config/config.json")
spark      = get_spark_session()

STORAGE_ACCOUNT  = config["storage_account"]
SILVER_CONTAINER = config["silver_container"]
GOLD_CONTAINER   = config["gold_container"]

SILVER_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_PATH   = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

JDBC_URL    = get_jdbc_url(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Sales Aggregation

def build_daily_sales():
    logger.info("Building daily sales aggregate")

    df = spark.read.format("delta").load(f"{SILVER_PATH}/orders/")

    daily_sales = (
        df
        .filter(col("status") == "COMPLETED")
        .groupBy(date_trunc("day", col("order_date")).alias("sales_date"))
        .agg(
            count("order_id").alias("total_orders"),
            _sum("amount").alias("total_revenue"),
            _round(avg("amount"), 2).alias("avg_order_value"),
            _max("amount").alias("max_order_value"),
            countDistinct("customer_id").alias("unique_customers")
        )
        .withColumn("_gold_timestamp", current_timestamp())
        .orderBy("sales_date")
    )

    # Write to Gold Delta
    (
        daily_sales.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{GOLD_PATH}/daily_sales/")
    )

    # Write to Azure SQL for Power BI
    (
        daily_sales.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dbo.daily_sales")
        .option("mode", "overwrite")
        .save()
    )

    logger.info(f"Daily sales rows written: {daily_sales.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Summary

def build_customer_summary():
    logger.info("Building customer summary")

    df = spark.read.format("delta").load(f"{SILVER_PATH}/orders/")

    customer_summary = (
        df
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            _sum("amount").alias("lifetime_value"),
            _round(avg("amount"), 2).alias("avg_order_value"),
            _max("order_date").alias("last_order_date"),
            _min("order_date").alias("first_order_date")
        )
        .withColumn("_gold_timestamp", current_timestamp())
    )

    (
        customer_summary.write
        .format("delta")
        .mode("overwrite")
        .save(f"{GOLD_PATH}/customer_summary/")
    )

    (
        customer_summary.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dbo.customer_summary")
        .option("mode", "overwrite")
        .save()
    )

    logger.info(f"Customer summary rows written: {customer_summary.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event Funnel Analysis

def build_event_funnel():
    logger.info("Building event funnel")

    df = spark.read.format("delta").load(f"{SILVER_PATH}/events/")

    funnel = (
        df
        .groupBy("event_type")
        .agg(
            count("event_id").alias("event_count"),
            countDistinct("user_id").alias("unique_users")
        )
        .withColumn("_gold_timestamp", current_timestamp())
        .orderBy(col("event_count").desc())
    )

    (
        funnel.write
        .format("delta")
        .mode("overwrite")
        .save(f"{GOLD_PATH}/event_funnel/")
    )

    (
        funnel.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dbo.event_funnel")
        .option("mode", "overwrite")
        .save()
    )

    logger.info("Event funnel complete.")

# COMMAND ----------

if __name__ == "__main__":
    build_daily_sales()
    build_customer_summary()
    build_event_funnel()
    logger.info("Gold layer aggregation complete.")
