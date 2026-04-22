# Databricks notebook source
import logging

# Create logger
logger = logging.getLogger("BronzePipelineLogger")
logger.setLevel(logging.INFO)

# Create console handler
if not logger.handlers:
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

logger.info("Logger initialized successfully")

# COMMAND ----------

try:
    logger.info("Starting to read JSON data from ADLS Bronze layer")

    df = spark.read.json(
        "abfss://raw@stretailinventory1010.dfs.core.windows.net/external/bronze/products"
    )

    logger.info(f"Data read successfully. Row count: {df.count()}")
    display(df)

except Exception as e:
    logger.error("Error while reading JSON data", exc_info=True)
    raise

# COMMAND ----------

from pyspark.sql.functions import col, explode

try:
    logger.info("Starting data transformation (flattening JSON)")

    products_flat_df = df.withColumn("product", explode("products")) \
        .select(
            "limit",
            "skip",
            "total",
            "product.*"
        )

    logger.info(f"Transformation successful. Row count: {products_flat_df.count()}")
    display(products_flat_df)

except Exception as e:
    logger.error("Error during transformation step", exc_info=True)
    raise

# COMMAND ----------

try:
    logger.info("Starting write operation to Bronze Delta table")

    products_flat_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("retail_inventory.bronze.raw_products")

    logger.info("Data successfully written to retail_inventory.bronze.raw_products")

except Exception as e:
    logger.error("Error while writing data to Delta table", exc_info=True)
    raise

# COMMAND ----------

try:
    logger.info("Starting validation check (row count)")

    result_df = spark.sql("""
        SELECT COUNT(*) AS Total_row 
        FROM retail_inventory.bronze.raw_products
    """)

    count_value = result_df.collect()[0]["Total_row"]

    logger.info(f"Validation successful. Total rows in table: {count_value}")
    display(result_df)

except Exception as e:
    logger.error("Error during validation step", exc_info=True)
    raise