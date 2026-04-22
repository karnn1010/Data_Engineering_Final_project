# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists retail_inventory;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog retail_inventory;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION retail_inventory_ext_loc
# MAGIC URL 'abfss://raw@stretailinventory1010.dfs.core.windows.net/external'
# MAGIC WITH (STORAGE CREDENTIAL inventory_management);

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema IF not exists bronze;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC MANAGED LOCATION 'abfss://raw@stretailinventory1010.dfs.core.windows.net/external/silver/';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC MANAGED LOCATION 'abfss://raw@stretailinventory1010.dfs.core.windows.net/external/gold/';