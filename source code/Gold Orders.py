# Databricks notebook source
# MAGIC %md
# MAGIC # **FACT ORDERS**

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading**

# COMMAND ----------

df = spark.sql("select * from silver.orders_silver")
df.display()


# COMMAND ----------

df_dimcus = spark.sql("select DimCustomerKey, customer_id as dim_customer_id from hi_databricks_e2e_workspace.gold.dim_customers")

df_dimpro = spark.sql("select product_id as DimProductKey, product_id as dim_product_id from hi_databricks_e2e_workspace.gold.dimproducts")


# COMMAND ----------

# MAGIC %md
# MAGIC **Fact Dataframe**

# COMMAND ----------

df_fact = df.join(df_dimcus, df['customer_id'] == df_dimcus['dim_customer_id'], how='left') \
            .join(df_dimpro, df['product_id'] == df_dimpro['dim_product_id'], how='left')

df_fact_new = df_fact.drop('dim_customer_id','dim_product_id','customer_id','product_id')


# COMMAND ----------

df_fact_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Upsert on Fact Table**

# COMMAND ----------

from delta.tables import DeltaTable


# COMMAND ----------

df_fact_new = df_fact_new.dropDuplicates(["order_id", "DimCustomerKey", "DimProductKey"])

try:
    spark.table("hi_databricks_e2e_workspace.gold.fact_orders")
    table_exists = True
except Exception as e:
    table_exists = False

if table_exists:
    dlt_obj = DeltaTable.forName(spark, "hi_databricks_e2e_workspace.gold.fact_orders")

    dlt_obj.alias("trg").merge(
        df_fact_new.alias("src"), 
        "trg.order_id = src.order_id AND trg.DimCustomerKey = src.DimCustomerKey AND trg.DimProductKey = src.DimProductKey"
    )\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

else:
    df_fact_new.write.format("delta")\
            .mode("overwrite")\
            .saveAsTable("hi_databricks_e2e_workspace.gold.fact_orders")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.fact_orders
# MAGIC

# COMMAND ----------

