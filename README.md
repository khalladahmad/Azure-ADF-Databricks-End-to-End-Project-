<div align="center">

<br/>

```
╔═══════════════════════════════════════════════════════════════╗
║         A Z U R E   D A T A   P I P E L I N E               ║
║              ADF  ×  Databricks  ×  Medallion                ║
╚═══════════════════════════════════════════════════════════════╝
```

<br/>

[![Azure](https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/data-factory)
[![Blob Storage](https://img.shields.io/badge/Azure_Blob_Storage-0089D6?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/storage/blobs)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python_3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

<br/>

> **End-to-end cloud data pipeline** ingesting a sales & e-commerce dataset (7 Parquet files)  
> from **Azure Blob Storage** → ADF → ADLS Gen2 → Databricks Medallion Architecture → Power BI

<br/>

</div>

---

## 📌 Table of Contents

- [Pipeline Overview](#-pipeline-overview)
- [Architecture](#-architecture)
- [Dataset](#-dataset--sales--e-commerce-parquet-files)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Step-by-Step Implementation](#-step-by-step-implementation)
  - [Step 1 — Data Source](#step-1--data-source)
  - [Step 2 — Azure Data Factory](#step-2--azure-data-factory)
  - [Step 3 — Databricks Medallion](#step-3--databricks-medallion-architecture)
  - [Step 4 — Transformations](#step-4--transformations)
  - [Step 5 — Output](#step-5--output)
- [Key Business Insights](#-key-business-insights)
- [How to Run](#-how-to-run)
- [Learnings](#-learnings)

---

## 🔭 Pipeline Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│   AZURE BLOB STORAGE         ADF PIPELINE          ADLS Gen2                │
│                                                                              │
│   ┌──────────────────┐      ┌────────────┐      ┌──────────────┐            │
│   │ customer_first   │      │            │      │              │            │
│   │ customer_second  │─────►│  7× Copy   │─────►│   bronze/    │            │
│   │ orders_first     │      │  Data      │      │   raw/       │            │
│   │ orders_second    │      │  (parallel)│      │              │            │
│   │ products_first   │      │            │      └──────┬───────┘            │
│   │ products_second  │      └────────────┘             │                    │
│   │ regions          │                                 │  Databricks mount  │
│   └──────────────────┘                                 ▼                    │
│                                               ┌──────────────────┐          │
│                                               │  🥉 Bronze Layer │          │
│                                               │  Parquet → Delta │          │
│                                               └────────┬─────────┘          │
│                                                        │                    │
│                                                        ▼                    │
│                                               ┌──────────────────┐          │
│                                               │  🥈 Silver Layer │          │
│                                               │  Union · Clean · │          │
│                                               │  Join · Cast     │          │
│                                               └────────┬─────────┘          │
│                                                        │                    │
│                                                        ▼                    │
│                          ┌──────────┐        ┌──────────────────┐          │
│                          │ Power BI │◄───────│  🥇 Gold Layer   │          │
│                          │Dashboard │        │  KPIs · Insights │          │
│                          └──────────┘        └──────────────────┘          │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 🏗 Architecture

This project implements the **Medallion Architecture** — an industry-standard lakehouse pattern that organises data into three progressive quality layers:

| Layer | Container | What happens |
|-------|-----------|--------------|
| 🥉 **Bronze** | `adls://bronze/raw/` | Raw Parquet files copied from Blob Storage via ADF, saved as Delta — no changes |
| 🥈 **Silver** | `adls://silver/delta/` | Partitions unioned, nulls handled, duplicates removed, tables joined |
| 🥇 **Gold** | `adls://gold/delta/` | Business aggregations, KPIs, analytics-ready tables for Power BI |

**Why Medallion?**
- Raw data in Bronze is **never modified** — full audit trail always available
- Each layer has a **single responsibility** — easy to debug and reprocess
- Gold tables are **BI-ready** — plug directly into Power BI with no extra transformation
- Industry standard used at Microsoft, Databricks, and modern data engineering teams worldwide

---

## 📦 Dataset — Sales & E-Commerce Parquet Files

Source: Video-provided dataset, stored in **Azure Blob Storage**, split across 7 Parquet files:

| File | Description | Key columns |
|------|-------------|-------------|
| `customer_first` | First partition of customer records | `customer_id`, `customer_name`, `region_id` |
| `customer_second` | Second partition of customer records | `customer_id`, `customer_name`, `region_id` |
| `orders_first` | First partition of sales orders | `order_id`, `customer_id`, `product_id`, `order_date`, `amount` |
| `orders_second` | Second partition of sales orders | `order_id`, `customer_id`, `product_id`, `order_date`, `amount` |
| `products_first` | First partition of product catalogue | `product_id`, `product_name`, `category`, `price` |
| `products_second` | Second partition of product catalogue | `product_id`, `product_name`, `category`, `price` |
| `regions` | Region / geography lookup | `region_id`, `region_name`, `country` |

> **Note:** The `_first` / `_second` partition pairs are merged using `union()` in the Silver layer before cleaning and joining.

---

## 🛠 Tech Stack

```
Cloud Platform      →   Microsoft Azure
Source Storage      →   Azure Blob Storage  (7 Parquet files)
Ingestion           →   Azure Data Factory v2  (7 parallel Copy Data activities)
Sink Storage        →   Azure Data Lake Storage Gen2  (ADLS Gen2)
Processing          →   Azure Databricks  (Runtime 13.3 LTS / Spark 3.4)
Table Format        →   Delta Lake
Languages           →   PySpark  ·  Spark SQL  ·  Python
Visualisation       →   Power BI Desktop  +  Databricks SQL Warehouse
```

---

## 📁 Project Structure

```
azure-databricks-pipeline/
│
├── 📂 notebooks/
│   ├── 00_Mount_Storage.py        # Mount ADLS Gen2 containers to Databricks
│   ├── 01_Bronze_Layer.py         # Read Parquet → write Delta (raw)
│   ├── 02_Silver_Layer.py         # Union partitions, clean, join all tables
│   ├── 03_Gold_Layer.py           # Aggregations & business KPIs
│   └── 04_Transformations.py      # Filtering, joins, window functions, SQL
│
├── 📂 adf/
│   └── pipeline_IngestParquetData.json   # Exported ADF pipeline definition
│
├── 📂 docs/
│   └── architecture_diagram.png          # Pipeline architecture diagram
│
└── README.md
```

---

## 🚀 Step-by-Step Implementation

### Step 1 — Data Source

7 Parquet files (provided in the video) were uploaded to an **Azure Blob Storage** container:

```
Blob Storage container: raw-parquet/
├── customer_first.parquet
├── customer_second.parquet
├── orders_first.parquet
├── orders_second.parquet
├── products_first.parquet
├── products_second.parquet
└── regions.parquet
```

---

### Step 2 — Azure Data Factory

#### 2.1 Azure Resources Created

```
Resource Group     →   rg-pipeline-project
Blob Storage       →   source account holding the 7 raw Parquet files
ADLS Gen2          →   datalakepipeline  (hierarchical namespace ON)
  └── Containers   →   bronze  /  silver  /  gold
Data Factory       →   adf-pipeline-project  (v2)
```

#### 2.2 Linked Services

| Name | Type | Points to |
|------|------|-----------|
| `LS_BlobStorage` | Azure Blob Storage | Source container with raw Parquet files |
| `LS_DataLake` | ADLS Gen2 | `datalakepipeline` — bronze container |

#### 2.3 Pipeline — `PL_IngestParquetData`

One **Copy Data** activity per file (7 total), all running **in parallel** — no sequential dependency arrows between activities:

```
Copy_CustomerFirst   →  bronze/raw/customer_first/
Copy_CustomerSecond  →  bronze/raw/customer_second/
Copy_OrdersFirst     →  bronze/raw/orders_first/
Copy_OrdersSecond    →  bronze/raw/orders_second/
Copy_ProductsFirst   →  bronze/raw/products_first/
Copy_ProductsSecond  →  bronze/raw/products_second/
Copy_Regions         →  bronze/raw/regions/
```

> Source format: **Parquet** → Sink format: **Parquet** (preserved as-is into Bronze)

---

### Step 3 — Databricks Medallion Architecture

#### Mount Storage (`00_Mount_Storage.py`)

```python
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type":
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id":     "<app-id>",
    "fs.azure.account.oauth2.client.secret": "<client-secret>",
    "fs.azure.account.oauth2.client.endpoint":
        "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
}

for container in ['bronze', 'silver', 'gold']:
    dbutils.fs.mount(
        source=f"abfss://{container}@datalakepipeline.dfs.core.windows.net/",
        mount_point=f"/mnt/{container}",
        extra_configs=configs
    )
    print(f"✅ Mounted /mnt/{container}")
```

---

#### 🥉 Bronze Layer (`01_Bronze_Layer.py`)

Read each Parquet file from ADLS and write as Delta — zero transformation at this stage.

```python
tables = [
    ("customer_first",   "/mnt/bronze/raw/customer_first/"),
    ("customer_second",  "/mnt/bronze/raw/customer_second/"),
    ("orders_first",     "/mnt/bronze/raw/orders_first/"),
    ("orders_second",    "/mnt/bronze/raw/orders_second/"),
    ("products_first",   "/mnt/bronze/raw/products_first/"),
    ("products_second",  "/mnt/bronze/raw/products_second/"),
    ("regions",          "/mnt/bronze/raw/regions/"),
]

for name, path in tables:
    df = spark.read.format("parquet").load(path)
    df.write.format("delta").mode("overwrite") \
      .save(f"/mnt/bronze/delta/{name}")
    print(f"✅ Bronze saved: {name}  |  rows: {df.count()}")
```

---

#### 🥈 Silver Layer (`02_Silver_Layer.py`)

Union the split partitions, clean each table, then join everything into one enriched dataset.

```python
from pyspark.sql.functions import col, to_date, trim

# ── Load all 7 Bronze Delta tables ────────────────────────────
cust_1    = spark.read.format("delta").load("/mnt/bronze/delta/customer_first")
cust_2    = spark.read.format("delta").load("/mnt/bronze/delta/customer_second")
ord_1     = spark.read.format("delta").load("/mnt/bronze/delta/orders_first")
ord_2     = spark.read.format("delta").load("/mnt/bronze/delta/orders_second")
prod_1    = spark.read.format("delta").load("/mnt/bronze/delta/products_first")
prod_2    = spark.read.format("delta").load("/mnt/bronze/delta/products_second")
regions   = spark.read.format("delta").load("/mnt/bronze/delta/regions")

# ── Union split partitions ────────────────────────────────────
customers = cust_1.union(cust_2).dropDuplicates(["customer_id"])
orders    = ord_1.union(ord_2).dropDuplicates(["order_id"])
products  = prod_1.union(prod_2).dropDuplicates(["product_id"])

# ── Clean ─────────────────────────────────────────────────────
customers = (customers
    .dropna(subset=["customer_id"])
    .withColumn("customer_name", trim(col("customer_name")))
)

orders = (orders
    .dropna(subset=["order_id", "customer_id", "amount"])
    .filter(col("amount") > 0)
    .withColumn("order_date", to_date(col("order_date")))
)

products = (products
    .dropna(subset=["product_id"])
    .fillna({"category": "Uncategorized"})
)

# ── Master join: orders + customers + products + regions ──────
df_silver = (orders
    .join(customers, "customer_id", "left")
    .join(products,  "product_id",  "left")
    .join(regions,   "region_id",   "left")
)

df_silver.write.format("delta").mode("overwrite") \
    .save("/mnt/silver/delta/orders_enriched")

print(f"✅ Silver saved  |  rows: {df_silver.count()}")
```

---

#### 🥇 Gold Layer (`03_Gold_Layer.py`)

Four business-ready KPI tables written to the Gold layer.

```python
from pyspark.sql.functions import sum, count, avg, round as spark_round, year, month

df = spark.read.format("delta").load("/mnt/silver/delta/orders_enriched")

# ── KPI 1: Monthly Revenue Trend ─────────────────────────────
monthly_revenue = (df
    .withColumn("year",  year("order_date"))
    .withColumn("month", month("order_date"))
    .groupBy("year", "month")
    .agg(
        spark_round(sum("amount"), 2).alias("total_revenue"),
        count("order_id").alias("total_orders"),
        spark_round(avg("amount"), 2).alias("avg_order_value")
    ).orderBy("year", "month")
)

# ── KPI 2: Revenue by Product Category ───────────────────────
category_revenue = (df
    .groupBy("category")
    .agg(
        spark_round(sum("amount"), 2).alias("revenue"),
        count("order_id").alias("total_orders")
    ).orderBy("revenue", ascending=False)
)

# ── KPI 3: Revenue by Region ──────────────────────────────────
region_revenue = (df
    .groupBy("region_name", "country")
    .agg(
        spark_round(sum("amount"), 2).alias("revenue"),
        count("order_id").alias("total_orders"),
        spark_round(avg("amount"), 2).alias("avg_order_value")
    ).orderBy("revenue", ascending=False)
)

# ── KPI 4: Top Customers by Lifetime Spend ───────────────────
top_customers = (df
    .groupBy("customer_id", "customer_name", "region_name")
    .agg(
        spark_round(sum("amount"), 2).alias("total_spend"),
        count("order_id").alias("order_count")
    ).orderBy("total_spend", ascending=False)
    .limit(100)
)

# ── Save all Gold tables ──────────────────────────────────────
for name, frame in [("monthly_revenue",  monthly_revenue),
                    ("category_revenue", category_revenue),
                    ("region_revenue",   region_revenue),
                    ("top_customers",    top_customers)]:
    frame.write.format("delta").mode("overwrite") \
         .save(f"/mnt/gold/delta/{name}")
    print(f"✅ Gold saved: {name}")
```

---

### Step 4 — Transformations

#### Filtering

```python
# High-value delivered orders in a specific year
df_filtered = df.filter(
    (year(col("order_date")) == 2023) &
    (col("amount") > 100)
)
```

#### Joins

```python
# Enrich orders with full region details
df_regional = (orders
    .join(customers, "customer_id", "left")
    .join(regions,   "region_id",   "left")
    .select("order_id", "customer_name",
            "region_name", "country", "amount", "order_date")
)
```

#### Aggregations via Spark SQL

```python
df.createOrReplaceTempView("sales")

result = spark.sql("""
    SELECT
        region_name,
        category,
        COUNT(order_id)        AS total_orders,
        ROUND(SUM(amount), 2)  AS revenue,
        ROUND(AVG(amount), 2)  AS avg_order_value
    FROM sales
    GROUP BY region_name, category
    ORDER BY revenue DESC
    LIMIT 50
""")
result.show()
```

#### Window Functions — Rank customers within each region

```python
from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window

window = Window.partitionBy("region_name").orderBy(col("total_spend").desc())

df_ranked = top_customers \
    .withColumn("rank_in_region", dense_rank().over(window)) \
    .filter(col("rank_in_region") <= 5)

df_ranked.show()
```

---

### Step 5 — Output

#### Export Gold tables as CSV

```python
for name in ["monthly_revenue", "category_revenue", "region_revenue", "top_customers"]:
    df_gold = spark.read.format("delta").load(f"/mnt/gold/delta/{name}")
    df_gold.toPandas().to_csv(
        f"/dbfs/mnt/gold/exports/{name}.csv", index=False
    )
    print(f"📤 Exported: {name}.csv")
```

#### Connect Power BI via Databricks SQL Warehouse

```
Databricks → SQL → SQL Warehouses → Create (2X-Small, Serverless)
  → Copy: Server hostname + HTTP path

Power BI Desktop
  → Get Data → Azure Databricks
  → Paste hostname + HTTP path
  → Authenticate with Personal Access Token
  → Select tables: monthly_revenue, category_revenue,
                   region_revenue, top_customers
  → Load → Build dashboard
```

---

## 📊 Key Business Insights

The Gold layer surfaces the following analytics from the pipeline:

- 📈 **Monthly revenue trends** — track sales growth and seasonality across the full order history
- 🛍️ **Top product categories** — identify which categories drive the most revenue
- 🗺️ **Regional performance** — compare revenue and order volume across all regions and countries
- 👤 **Top 100 customers** — ranked by lifetime spend, segmented by region
- 💰 **Average order value** — broken down by category and region for pricing strategy insights

---

## ▶️ How to Run

### Prerequisites

| Requirement | Detail |
|-------------|--------|
| Azure Subscription | Active (free tier works for learning) |
| Azure Blob Storage | Container with the 7 Parquet source files |
| ADLS Gen2 | 3 containers created: `bronze`, `silver`, `gold` |
| Azure Data Factory | v2, with Blob + ADLS Gen2 linked services |
| Azure Databricks | Standard tier, Runtime 13.3 LTS |
| App Registration | For OAuth mount (client ID + secret + tenant ID) |
| Power BI Desktop | Latest version (optional) |

### Steps

```bash
# 1. Clone this repo
git clone https://github.com/<your-username>/azure-databricks-pipeline.git
cd azure-databricks-pipeline

# 2. Upload all 7 Parquet files to your Azure Blob Storage container

# 3. In ADF:
#    → Create linked services: LS_BlobStorage + LS_DataLake
#    → Build pipeline PL_IngestParquetData (7 parallel Copy Data activities)
#    → Trigger Now → Monitor until all 7 show Succeeded ✅

# 4. In Databricks — import notebooks from the /notebooks folder
#    Run in this exact order:
#      00_Mount_Storage.py   →  verify all 3 mounts succeed
#      01_Bronze_Layer.py    →  7 Delta tables in /mnt/bronze/delta/
#      02_Silver_Layer.py    →  enriched master table in /mnt/silver/delta/
#      03_Gold_Layer.py      →  4 KPI tables in /mnt/gold/delta/
#      04_Transformations.py →  filtering, joins, SQL, window functions

# 5. (Optional) Connect Power BI to Databricks SQL Warehouse
#    Load Gold tables and build your dashboard
```

---

## 💡 Learnings

```
✦  Parquet is a superior source format — schema embedded, no inferSchema guesswork
✦  Split partitions (_first / _second) merge cleanly with union() before deduplication
✦  Medallion Architecture keeps raw data intact while progressive layers add quality
✦  Delta Lake enables reliable overwrites and ACID transactions at scale
✦  Running 7 Copy Data activities in parallel in ADF is significantly faster than sequential
✦  Joining on region_id enriches every downstream query with geographic context
✦  Databricks SQL Warehouses make Power BI integration seamless — no extra tooling needed
```

---

<div align="center">

<br/>

**Built with ☁️ Azure · ⚡ Databricks · 🐍 PySpark · 📦 Delta Lake**

<br/>

*If you found this useful, consider giving it a ⭐*

</div>
