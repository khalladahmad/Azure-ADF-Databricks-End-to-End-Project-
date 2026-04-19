# Databricks notebook source
df = spark.read.table("bronze.regions")


# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("silver.regions_silver")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.regions_silver LIMIT 5
# MAGIC

# COMMAND ----------

