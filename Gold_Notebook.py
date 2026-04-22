# Databricks notebook source
import logging

logger = logging.getLogger("GoldPipelineLogger")
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

logger.info("Gold layer pipeline started")

# COMMAND ----------

try:
    logger.info("Creating dim_product table")

    dim_product = spark.table("retail_inventory.silver.products").select(
        "id",
        "title",
        "brand",
        "category",
        "price"
    )

    logger.info(f"dim_product created. Row count: {dim_product.count()}")

    dim_product.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("retail_inventory.gold.dim_product")

    logger.info("dim_product table written successfully")

except Exception as e:
    logger.error("Error creating dim_product", exc_info=True)
    raise

# COMMAND ----------

try:
    logger.info("Creating dim_store table")

    dim_store = spark.table("retail_inventory.silver.store_inventory").select(
        "store_id",
        "store_name"
    ).dropDuplicates()

    logger.info(f"dim_store created. Row count: {dim_store.count()}")

    dim_store.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("retail_inventory.gold.dim_store")

    logger.info("dim_store table written successfully")

except Exception as e:
    logger.error("Error creating dim_store", exc_info=True)
    raise

# COMMAND ----------



# COMMAND ----------

try:
    logger.info("Creating fact_inventory table")

    fact_inventory = spark.table("retail_inventory.silver.store_inventory").select(
        "id",
        "store_id",
        "store_stock"
    )

    logger.info(f"fact_inventory created. Row count: {fact_inventory.count()}")

    fact_inventory.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("retail_inventory.gold.fact_inventory")

    logger.info("fact_inventory table written successfully")

except Exception as e:
    logger.error("Error creating fact_inventory", exc_info=True)
    raise

# COMMAND ----------

try:
    logger.info("Setting catalog and schema to gold")

    spark.sql("USE CATALOG retail_inventory")
    spark.sql("USE SCHEMA gold")

    logger.info("Catalog and schema set successfully")

except Exception as e:
    logger.error("Error setting catalog/schema", exc_info=True)
    raise

# COMMAND ----------

try:
    logger.info("Creating low_stock_alert table")

    spark.sql("""
        CREATE OR REPLACE TABLE low_stock_alert AS
        SELECT 
            i.store_id,
            i.store_name,
            i.id,
            p.title,
            p.category,
            i.store_stock
        FROM retail_inventory.silver.store_inventory i
        JOIN retail_inventory.silver.products p
        ON i.id = p.id
        WHERE i.store_stock < 10
    """)

    count_df = spark.sql("SELECT COUNT(*) AS cnt FROM low_stock_alert")
    count_val = count_df.collect()[0][0]

    logger.info(f"low_stock_alert created. Rows: {count_val}")

except Exception as e:
    logger.error("Error creating low_stock_alert", exc_info=True)
    raise

# COMMAND ----------

try:
    logger.info("Creating overstock_alert table")

    spark.sql("""
        CREATE OR REPLACE TABLE overstock_alert AS
        SELECT 
            i.store_id,
            i.store_name,
            i.id,
            p.title,
            p.category,
            i.store_stock
        FROM retail_inventory.silver.store_inventory i
        JOIN retail_inventory.silver.products p
        ON i.id = p.id
        WHERE i.store_stock > 80
    """)

    count_df = spark.sql("SELECT COUNT(*) AS cnt FROM overstock_alert")
    count_val = count_df.collect()[0][0]

    logger.info(f"overstock_alert created. Rows: {count_val}")

except Exception as e:
    logger.error("Error creating overstock_alert", exc_info=True)
    raise

# COMMAND ----------

try:
    logger.info("Creating category_stock_summary table")

    spark.sql("""
        CREATE OR REPLACE TABLE category_stock_summary AS
        SELECT 
            p.category,
            SUM(i.store_stock) AS total_stock,
            AVG(i.store_stock) AS avg_stock,
            MIN(i.store_stock) AS min_stock,
            MAX(i.store_stock) AS max_stock
        FROM retail_inventory.silver.store_inventory i
        JOIN retail_inventory.silver.products p
        ON i.id = p.id
        GROUP BY p.category
    """)

    logger.info("category_stock_summary created successfully")

except Exception as e:
    logger.error("Error creating category summary", exc_info=True)
    raise

# COMMAND ----------

try:
    logger.info("Validating gold tables")

    overstock_count = spark.sql("""
        SELECT COUNT(*) FROM retail_inventory.gold.overstock_alert
    """).collect()[0][0]

    lowstock_count = spark.sql("""
        SELECT COUNT(*) FROM retail_inventory.gold.low_stock_alert
    """).collect()[0][0]

    logger.info(f"Overstock count: {overstock_count}")
    logger.info(f"Low stock count: {lowstock_count}")

except Exception as e:
    logger.error("Error during validation", exc_info=True)
    raise

# COMMAND ----------

logger.info("Gold pipeline execution completed successfully")