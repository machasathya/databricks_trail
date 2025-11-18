# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,upper,concat,to_date,trim,max,datediff,avg,first
from pyspark.sql import functions as sf
from pyspark.sql import types as T
from ProcessMetadataUtility import ProcessMetadataUtility
pmu = ProcessMetadataUtility()

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
table_name = "FACT_PO_VENDOR_PERFORMANCE"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the notebook manually
# spark.conf.set("spark.sql.broadcastTimeout",36000)
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata_prod"
# curated_db_name = "cur_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# scope = "AZR-DBR-KV-SCOPE-300"
# processed_location = "mnt/bi_datalake/prod/pro/"
# processed_schema_name = "sales_semantic_prod"
# processed_db_name = "pro_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_PO_VENDOR_PERFORMANCE"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from fatc po table from processed layer tables
po_df = spark.sql("select PURCHASE_DOCUMENT,ITEM,DOCUMENT_DATE_KEY,EXPECTED_DELIVERY_DATE,ACTUAL_DELIVERY_DATE,VENDOR_KEY,PLANT_KEY,PURCHASE_ORG_KEY,PURCHASE_GROUP_KEY,PO_TYPE from {}.{} where PO_STATUS = 'CLOSED'".format(processed_db_name,"FACT_PO"))
dim_date = spark.sql("select date_dt,date_created_key from {}.{}".format(processed_db_name,"DIM_DATE"))

# COMMAND ----------

# defining po created date, expected performance,actual performance, vendor performance
df1 = po_df.alias("l_df").join(dim_date.alias("r_df"),on=[col("l_df.document_date_key") == col("date_created_key")],how = 'left').select(["l_df." + cols for cols in po_df.columns] + [col("date_dt").alias("PO_CREATED_DATE")])

df2 = df1.withColumn("EXPECTED_PERFORMANCE",datediff(col("EXPECTED_DELIVERY_DATE"),col("PO_CREATED_DATE"))).withColumn("ACTUAL_PERFORMANCE",datediff(col("ACTUAL_DELIVERY_DATE"),col("PO_CREATED_DATE")))

df3 = df2.withColumn("VENDOR_PERFORMANCE",col("EXPECTED_PERFORMANCE") - col("ACTUAL_PERFORMANCE")).drop(*["EXPECTED_DELIVERY_DATE","PO_CREATED_DATE","ACTUAL_DELIVERY_DATE","EXPECTED_PERFORMANCE","ACTUAL_PERFORMANCE"])

final_df = df3.groupBy("VENDOR_KEY","PURCHASE_DOCUMENT","DOCUMENT_DATE_KEY","PURCHASE_ORG_KEY","PURCHASE_GROUP_KEY","PO_TYPE").agg(avg("VENDOR_PERFORMANCE").alias("VENDOR_PERFORMANCE"),first("PLANT_KEY").alias("PLANT_KEY"))

# COMMAND ----------

# writing data to processed layer
final_df.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# insert log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
