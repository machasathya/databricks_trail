# Databricks notebook source
# import statements
from datetime import datetime 
from pyspark.sql.functions import col, lit, expr, to_date, to_timestamp, sum, min, when, count, concat ,first
from pyspark.sql.types import *
from ProcessMetadataUtility import ProcessMetadataUtility

# COMMAND ----------

# declaring class onject for processmetadatautility class
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
table_name = "FACT_LOGISTICS_RLS"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading data from curated layer
grps_df=spark.sql("select * from {}.{}".format(curated_db_name, "GROUPS")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# reading data from curated layer
bi_grps_df=spark.sql("select * from {}.{}".format(curated_db_name, "BI_GROUPS_LOGISTICS")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# joining groups and BI_GROUPS_LOGISTICS to select mail id and plant id mapping
rls=grps_df.alias("df1").join(bi_grps_df.alias("df2"),on=[col("df1.GROUP")==col("df2.GROUP")],how='inner')
rls_final=rls.select("USER_MAIL_ID","PLANT_DEPOT_ID")

# COMMAND ----------

# writing data to processed layer
rls_final.write.mode('overwrite').parquet(processed_location+"FACT_LOGISTICS_RLS")

# COMMAND ----------

# inserting log records to last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
