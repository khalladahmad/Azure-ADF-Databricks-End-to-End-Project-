# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.table("bronze.products")


# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION hi_databricks_e2e_workspace.bronze.discount_func(p_price DOUBLE) 
# MAGIC RETURNS DOUBLE  
# MAGIC LANGUAGE SQL 
# MAGIC RETURN p_price * 0.90
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id, price, hi_databricks_e2e_workspace.bronze.discount_func(price) as discounted_price
# MAGIC FROM products
# MAGIC

# COMMAND ----------

df = df.withColumn("discounted_price", expr("hi_databricks_e2e_workspace.bronze.discount_func(price)"))
df.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION hi_databricks_e2e_workspace.bronze.upper_func(p_brand STRING)
# MAGIC RETURNS STRING 
# MAGIC LANGUAGE PYTHON 
# MAGIC AS 
# MAGIC $$ 
# MAGIC     return p_brand.upper()
# MAGIC $$
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id, brand, hi_databricks_e2e_workspace.bronze.upper_func(brand) as brand_upper
# MAGIC FROM products
# MAGIC

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("silver.products_silver")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.products_silver LIMIT 5
# MAGIC

# COMMAND ----------

# %sql
# CREATE OR REPLACE FUNCTION hi_databricks_e2e_workspace.bronze.tablefunc(p_year DOUBLE)
# RETURNS TABLE(order_id STRING, year DOUBLE)
# LANGUAGE SQL
# RETURN 
# ( SELECT order_id, year FROM hi_databricks_e2e_workspace.gold.factorders
#   WHERE year = p_year )


# COMMAND ----------

# %sql
# SELECT * FROM hi_databricks_e2e_workspace.bronze.tablefunc(2024)
