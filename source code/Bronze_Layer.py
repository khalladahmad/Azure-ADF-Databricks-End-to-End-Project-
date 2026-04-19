# Databricks notebook source
# MAGIC %md
# MAGIC ### **Dynamic Capabilities**

# COMMAND ----------

# Authenticate to ADLS Gen2 using Secret Scope
storage_account_name = "hidatabrickse2e"
storage_account_access_key = dbutils.secrets.get(scope="hide2escope", key="storage-account-key")
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_access_key)

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

dbutils.fs.rm(f"dbfs:/checkpoints/write/{p_file_name}", recurse=True)
dbutils.fs.rm(f"dbfs:/checkpoints/schema/{p_file_name}", recurse=True)
print(f"Checkpoints cleared for: {p_file_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", f"dbfs:/checkpoints/schema/{p_file_name}")\
    .load(f"abfss://source@{storage_account_name}.dfs.core.windows.net/{p_file_name}*.parquet")


# COMMAND ----------

# MAGIC %md
# MAGIC ### **DATA WRITING**

# COMMAND ----------

df.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", f"dbfs:/checkpoints/write/{p_file_name}")\
    .trigger(availableNow=True)\
    .toTable(f"bronze.{p_file_name}")


# COMMAND ----------

df = spark.table(f"bronze.{p_file_name}")
display(df)


# COMMAND ----------

