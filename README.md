 
#  Retail Inventory Management System (End-to-End Data Engineering Project)

##  Project Overview

This project is an **end-to-end data engineering pipeline** designed to track inventory across multiple stores and detect:

*  Low stock situations
*  Overstock situations

The pipeline is built using **Azure Data Factory, Azure Data Lake, Databricks, PySpark, and Delta Lake** following the **Medallion Architecture (Bronze → Silver → Gold)**.

---

##  Architecture Overview

* Data is fetched daily from an external API
* Stored in ADLS (Bronze zone)
* Processed in Databricks using layered architecture
* Alerts and analytics generated in Gold layer
* Pipeline monitored and scheduled using ADF

---

##  Tech Stack

* **Azure Data Factory (ADF)** – Pipeline orchestration & scheduling
* **Azure Data Lake Storage (ADLS Gen2)** – Data storage
* **Azure Databricks** – Data processing
* **PySpark** – Transformation logic
* **Delta Lake** – Storage & incremental processing
* **Unity Catalog** – Data governance
* **Databricks SQL Warehouse** – Dashboard & analytics

---

##  End-to-End Pipeline Workflow

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/2e154d07-1968-47c3-8c95-da5575aedad9" />


### 🔹 Step 1: Data Ingestion (ADF)

* ADF pipeline is triggered **daily at 6:00 AM**
* Data is fetched from API:

```id="t2yxg9"
https://dummyjson.com/products
```

* Data is copied into **ADLS Gen2 (Bronze layer)**

---

### 🔹 Step 2: Data Storage (ADLS)

* Raw JSON files stored in:

```id="b7f3qp"
raw/products/products_YYYY-MM-DD.json
```

* Each day new file is created → supports historical tracking

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/3e962adf-88d8-49bd-bd3f-41e164c1d8d1" />

---

### 🔹 Step 3: Bronze Layer (Databricks)

* Data is read from ADLS using **external location**
* JSON is ingested as-is (no transformation)
* Nested structure is flattened (explode)

**Output Table:**

```id="vxhsls"
retail_inventory.bronze.raw_products
```

---

### 🔹 Step 4: Silver Layer (Cleaning + Incremental Load)

* Data cleaning performed:

  * Remove duplicates
  * Handle null values
  * Data type corrections

* **Incremental processing using Delta MERGE**

  * New records → INSERT
  * Existing records → UPDATE
  * Unchanged → SKIP

**Output Table:**

```id="6zhj78"
retail_inventory.silver.products
```

---

### 🔹 Step 5: Gold Layer (Business Logic)

* Created **Fact and Dimension tables**
* Applied business rules:

```id="9f4zq1"
Low Stock: stock < 10  
Overstock: stock > 80
```

**Output Tables:**

```id="ew1y9y"
retail_inventory.gold.low_stock_alerts  
retail_inventory.gold.overstock_alerts
```

### 🔹 Step 6: Pipeline Monitoring (ADF)

* ADF monitors pipeline execution:

  * ✅ Success → pipeline completed
  * ❌ Failure → retry + alert mechanism


## 📊 Key Features

* ✅ End-to-End Automated Pipeline
* ✅ Daily Data Ingestion (ADF Trigger)
* ✅ Medallion Architecture (Bronze → Silver → Gold)
* ✅ Incremental Data Processing (MERGE)
* ✅ Data Quality Handling
* ✅ Business Alerts (Low Stock / Overstock)
* ✅ Pipeline Monitoring System
* ✅ Scalable and Production-Ready Design

---

## 📈 Business Use Case

This system helps businesses:

* Avoid stockouts (increase sales)
* Reduce overstock (optimize storage cost)
* Monitor inventory in real-time
* Make data-driven decisions


## 👤 Author

**Karan Shende**
Data Engineering Intern
Azure Data Engineering Project — 2026

