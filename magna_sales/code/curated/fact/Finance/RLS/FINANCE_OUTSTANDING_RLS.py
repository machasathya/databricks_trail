# Databricks notebook source
# import statements
from datetime import datetime 
from pyspark.sql.functions import col, lit, expr, to_date, to_timestamp, sum, min, when, count, concat ,first,upper
from pyspark.sql.types import *
from ProcessMetadataUtility import ProcessMetadataUtility

# COMMAND ----------

# class object definition
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
table_name = "FACT_FINANCE_OUTSTANDING_RLS"
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
# processed_schema_name = "sales_semantic_prod"
# processed_db_name = "pro_prod"
# curated_db_name = "cur_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_FINANCE_OUTSTANDING_RLS"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading data from curated layer
grps_df=spark.sql("select * from {}.{}".format(curated_db_name, "GROUPS")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# reading data from curated layer
bi_grps_df=spark.sql("select * from {}.{}".format(curated_db_name, "BI_GROUPS_FINANCE_OUTSTANDING")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# defining mail id to product line mapping
rls=bi_grps_df.alias("df1").join(grps_df.alias("df2"),on=[col("df1.GROUP")==col("df2.GROUP")],how='left')

rls_df = rls.select("USER_MAIL_ID","OUTSTANDING_PRODUCT_LINE")

# COMMAND ----------

# writing data to processed location
rls_df.write.mode('overwrite').parquet(processed_location+"FACT_FINANCE_OUTSTANDING_RLS")

# COMMAND ----------

# inserting log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)

