# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,max as _max,substring
from pyspark.sql import functions as f
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType, DecimalType
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# COMMAND ----------

# enabling cross join
spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

# code to fetch the data from ADF Parameters
meta_table = "fact_surrogate_meta"
processed_schema_name = dbutils.widgets.get("processed_schema_name")
schema_name = dbutils.widgets.get("schema_name")
db_url = dbutils.widgets.get("db_url")
db = dbutils.widgets.get("db")
user_name = dbutils.widgets.get("user_name")
scope = dbutils.widgets.get("scope")
password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
processed_location = dbutils.widgets.get("processed_path")
curated_db_name = dbutils.widgets.get("curated_db")
processed_db_name = dbutils.widgets.get("processed_db_name")
table_name = "FACT_PO_HEADER_CHANGE_AGGREGATE"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the notebook manually
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata_prod"
# curated_db_name = "cur_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# scope = "AZR-DBR-KV-SCOPE-300"
# processed_location = "mnt/bi_datalake/prod/pro/"
# processed_schema_name = "global_semantic_prod"
# processed_db_name = "pro_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_PO_HEADER_CHANGE_AGGREGATE"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# class object definition
pmu = ProcessMetadataUtility()

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from processed layer
FACT_PO_HEADER_CHANGES = spark.sql("select * from {}.{}".format(processed_db_name,"FACT_PO_HEADER_CHANGES"))

# COMMAND ----------

# renaming column
df = FACT_PO_HEADER_CHANGES.withColumnRenamed("COLUMN_NAME", "TYPE")

# COMMAND ----------

# aggregating data upon po number,date and type for quantity data
QTY = df.where((col("VALUE_NEW") != col("VALUE_OLD")) & (col("TYPE") == 'MENGE')).groupBy("PURCHASE_DOC_NUMBER","DATE_KEY","TYPE").count().withColumnRenamed("count", "NO_OF_CHANGES")

# COMMAND ----------

# aggregating data upon po number,date and type for price value data
PRICE = df.where((col("VALUE_NEW") != col("VALUE_OLD")) & (col("TYPE") == 'NETPR')).groupBy("PURCHASE_DOC_NUMBER","DATE_KEY","TYPE").count().withColumnRenamed("count", "NO_OF_CHANGES")

# COMMAND ----------

# union and defining count of price changes
df2 = QTY.unionAll(PRICE)
df3 = df2.withColumn("NO_OF_PRICE_CHANGES",when(col("TYPE") == "NETPR",col("NO_OF_CHANGES")).otherwise(lit(0))).withColumn("NO_OF_QUANTITY_CHANGES",when(col("TYPE") == "MENGE",col("NO_OF_CHANGES")).otherwise(lit(0)))

# COMMAND ----------

# aggregating data
final_df_1 = df3.groupBy("PURCHASE_DOC_NUMBER","DATE_KEY") \
    .sum("NO_OF_PRICE_CHANGES","NO_OF_QUANTITY_CHANGES").withColumnRenamed("sum(NO_OF_PRICE_CHANGES)", "NO_OF_PRICE_CHANGES").withColumnRenamed("sum(NO_OF_QUANTITY_CHANGES)", "NO_OF_QUANTITY_CHANGES")

# COMMAND ----------

# dropping duplicates
final_df = final_df_1.alias("rac").join(FACT_PO_HEADER_CHANGES.alias("pp"),on=[col("rac.PURCHASE_DOC_NUMBER") == col("pp.PURCHASE_DOC_NUMBER")],how='left').select(["rac." + cols for cols in final_df_1.columns] + [col("pp.PLANT_KEY").alias("PLANT_KEY")] + [col("pp.DOC_TYPE").alias("DOC_TYPE")])
final_df = final_df.dropDuplicates()

# COMMAND ----------

# writing data to processed layer
final_df.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# insert log data into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
