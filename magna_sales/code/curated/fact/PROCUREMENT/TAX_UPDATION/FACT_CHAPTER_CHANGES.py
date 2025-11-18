# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,max as _max,substring, max as _min
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

# code to fetch the surrogate metadata from ADF Parameters
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
table_name = "FACT_CHAPTER_CHANGES"
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
# table_name = "FACT_CHAPTER_CHANGES"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# class object definition
pmu = ProcessMetadataUtility()

# COMMAND ----------

# reading chapter dimension data from ADF
DIM_CHAPTER = spark.sql("select * from {}.{}".format(processed_db_name,"DIM_CHAPTER")).withColumnRenamed("CHANGED_ON", "CREATED_DATE")

# COMMAND ----------

# reading chapter dimension data from ADF
DIM_CHAPTER_CREATED = spark.sql("select * from {}.{}".format(processed_db_name,"DIM_CHAPTER_CREATED"))

# COMMAND ----------

# joining data
df2 = DIM_CHAPTER.join(DIM_CHAPTER_CREATED.select('CHAPTER_ID','CREATED_DATE'), how='left_anti', on=['CHAPTER_ID','CREATED_DATE'])

# COMMAND ----------

# joining data
final_df_1 = df2.alias("rac").join(DIM_CHAPTER_CREATED.alias("pp"),on=[col("rac.CHAPTER_ID") == col("pp.CHAPTER_ID")],how='inner').select(["rac." + cols for cols in df2.columns] + [col("pp.CREATED_DATE").alias("CREATED_DATE_OLD")])

# COMMAND ----------

# selecting required columns
final_df = final_df_1.selectExpr("CHAPTER_ID","CREATED_DATE as UPDATED_DATE")

# COMMAND ----------

# writing data to processed layer location
final_df.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# insert log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
