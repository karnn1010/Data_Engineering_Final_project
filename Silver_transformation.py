# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG retail_inventory
# MAGIC

# COMMAND ----------

import logging

logger = logging.getLogger("SilverPipelineLogger")
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

logger.info("Silver layer pipeline started")

# COMMAND ----------

try:
    logger.info("Setting catalog to retail_inventory")

    spark.sql("USE CATALOG retail_inventory")

    logger.info("Catalog set successfully")

except Exception as e:
    logger.error("Error while setting catalog", exc_info=True)
    raise

# COMMAND ----------

try:
    logger.info("Reading data from Bronze table")

    bronze_df = spark.table("bronze.raw_products")

    logger.info(f"Bronze data loaded successfully. Row count: {bronze_df.count()}")

except Exception as e:
    logger.error("Error while reading Bronze table", exc_info=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC #### incremntal load 

# COMMAND ----------

from pyspark.sql.functions import col

try:
    logger.info("Starting data transformation and casting")

    silver_df = bronze_df.select(
        col("id").cast("int"),
        col("category").cast("string"),
        col("title").cast("string"),
        col("brand").cast("string"),
        col("price").cast("double"),
        col("discountPercentage").cast("double"),
        col("stock").cast("int"),
        col("rating").cast("double")
    )

    logger.info(f"Transformation successful. Row count: {silver_df.count()}")

except Exception as e:
    logger.error("Error during transformation", exc_info=True)
    raise

# COMMAND ----------

from delta.tables import DeltaTable

table_name = "silver.products"

try:
    logger.info("Starting incremental load process")

    if spark.catalog.tableExists(table_name):

        logger.info("Table exists. Performing MERGE operation")

        target = DeltaTable.forName(spark, table_name)

        target.alias("existing").merge(
            silver_df.alias("incoming"),
            "existing.id = incoming.id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        logger.info("MERGE operation completed successfully")

    else:
        logger.info("Table does not exist. Performing initial load")

        silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("retail_inventory.silver.products")

        logger.info("Initial load completed successfully")

except Exception as e:
    logger.error("Error during incremental load", exc_info=True)
    raise

# COMMAND ----------

from pyspark.sql.functions import lit, rand, round

try:
    logger.info("Generating store-level inventory data")

    products_df = spark.table("silver.products")

    store1 = products_df.withColumn("store_id", lit(1)) \
                        .withColumn("store_name", lit("Pune Store")) \
                        .withColumn("store_stock", round(rand() * 100))

    store2 = products_df.withColumn("store_id", lit(2)) \
                        .withColumn("store_name", lit("Mumbai Store")) \
                        .withColumn("store_stock", round(rand() * 100))

    store3 = products_df.withColumn("store_id", lit(3)) \
                        .withColumn("store_name", lit("Nagpur Store")) \
                        .withColumn("store_stock", round(rand() * 100))

    inventory_df = store1.union(store2).union(store3)

    logger.info(f"Inventory dataset created. Row count: {inventory_df.count()}")

    display(inventory_df)

except Exception as e:
    logger.error("Error while creating inventory dataset", exc_info=True)
    raise

# COMMAND ----------

try:
    logger.info("Writing inventory data to silver.store_inventory")

    inventory_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver.store_inventory")

    logger.info("Inventory table written successfully")

except Exception as e:
    logger.error("Error while writing inventory table", exc_info=True)
    raise

# COMMAND ----------

try:
    logger.info("Validating silver.products table")

    result1 = spark.sql("""
        SELECT COUNT(*) AS total_product_after_cleaning 
        FROM retail_inventory.silver.products
    """)

    count1 = result1.collect()[0][0]

    logger.info(f"Total products after cleaning: {count1}")

    display(result1)

except Exception as e:
    logger.error("Error during product validation", exc_info=True)
    raise