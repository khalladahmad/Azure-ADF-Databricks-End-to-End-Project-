# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.table("bronze.customer")


# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df = df.withColumn("domains",split(col('email'),'@')[1])
df.display()

# COMMAND ----------

df.groupBy("domains").agg(count("customer_id").alias("total_customers")).sort("total_customers",ascending=False).display()

# COMMAND ----------

df_gmail = df.filter(col('domains')=="gmail.com")
df_gmail.display()
time.sleep(5)

df_yahoo = df.filter(col('domains')=="yahoo.com")
df_yahoo.display()
time.sleep(5)

df_hotmail = df.filter(col('domains')=="hotmail.com")
df_hotmail.display()
time.sleep(5)

# COMMAND ----------

df = df.withColumn("full_name",concat(col('first_name'),lit(' '),col('last_name')))
df = df.drop('first_name','last_name')

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("silver.customers_silver")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.customers_silver LIMIT 5
# MAGIC

# COMMAND ----------

