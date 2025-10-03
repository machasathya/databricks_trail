# Databricks notebook source
# MAGIC %md 
# MAGIC ### transformation
# MAGIC

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").option("inferSchema","true")\
    .load("/Volumes/workspace/default/managed_volume/Fact_Sales_1.csv")

df.display()

# COMMAND ----------

for i in df.columns:
    print(i)

# COMMAND ----------


df_capitalized = df.toDF(*[col.upper() for col in df.columns])
display(df_capitalized)

# COMMAND ----------

df_rmnulls = df_capitalized.dropna()
df_rmnulls.display()

# COMMAND ----------

df_select =df_rmnulls.select("TRANSACTION_ID","TRANSACTIONAL_DATE","PRODUCT_ID","CUSTOMER_ID","PAYMENT")
df_select.display()

# COMMAND ----------

df_select.write.format("delta").mode("overwrite").save("/Volumes/sslc_sales/sslc_sales_tdp/sales_tdp")

# COMMAND ----------


