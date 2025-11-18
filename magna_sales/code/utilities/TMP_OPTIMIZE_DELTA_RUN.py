# Databricks notebook source
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled" ,"false")

# COMMAND ----------

meta_table = "table_meta"
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# Running Vaccum for all the tables.
if table_name == 'ZKONV':
  print("RUNNING VACCUM FOR ->{}".format(table_name))
  spark.sql("VACUUM cur_prod.{} RETAIN 72 HOURS".format("KONV"))
elif table_name == 'ZBSEG':
  print("RUNNING VACCUM FOR ->{}".format(table_name))
  spark.sql("VACUUM cur_prod.{} RETAIN 72 HOURS".format("BSEG"))
elif table_name == 'ZCDPOS':
  print("RUNNING VACCUM FOR ->{}".format(table_name))
  spark.sql("VACUUM cur_prod.{} RETAIN 72 HOURS".format("CDPOS"))
elif table_name ==  'FACT_SALES_INVOICE':
  print("RUNNING VACCUM FOR ->{}".format(table_name))
  spark.sql("VACUUM pro_prod.{} RETAIN 72 HOURS".format(table_name))
else:
  print("RUNNING VACCUM FOR ->{}".format(table_name))
  spark.sql("VACUUM cur_prod.{} RETAIN 72 HOURS".format(table_name))
