# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,max as _max, trim, sum
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
from functools import reduce
from pyspark.sql import DataFrame

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
table_name = "FACT_VENDOR_PAYMENT_TERMS"
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
# table_name = "FACT_VENDOR_PAYMENT_TERMS"
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

# reading data from curated layer
LFA1 = spark.sql("select LIFNR,LAND1,NAME1 from {}.{}".format(curated_db_name, "LFA1"))
LFM1 = spark.sql("select LIFNR,ZTERM from {}.{}".format(curated_db_name, "LFM1"))
T052 = spark.sql("select ZTERM as PAYMENT_TERM, ZTAGG as DAY_LIMIT, TEXT1 as PAYMENT_TERM_DESCRIPTION from {}.{}".format(curated_db_name, "T052U"))

# COMMAND ----------

# join lfa1 and lfm1
final = LFM1.alias("rac").join(LFA1.alias("pp"),on=[col("rac.LIFNR") == col("pp.LIFNR")],how='left').select(["rac." + cols for cols in LFM1.columns] + [col("pp.LIFNR").alias("LIFNR_LA")])

# COMMAND ----------

# join with t052
final_1 = T052.alias("ra").join(final.alias("pp1"),on=[col("ra.PAYMENT_TERM") == col("pp1.ZTERM")],how='inner').select(["ra." + cols for cols in T052.columns] + [col("pp1.LIFNR").alias("LIFNR")])

# COMMAND ----------

# aggregating data
final_df = final_1.groupBy("PAYMENT_TERM","DAY_LIMIT","PAYMENT_TERM_DESCRIPTION").count()

# COMMAND ----------

# writing data to processed layer
final_df.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# inserting log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
