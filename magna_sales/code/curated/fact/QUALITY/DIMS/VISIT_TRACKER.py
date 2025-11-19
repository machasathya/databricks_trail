# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,upper,concat
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
pmu = ProcessMetadataUtility()

# COMMAND ----------

# code to fetch the parameters data from adf
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
table_name = "FACT_VISIT_TRACKER"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the code manually
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
# table_name = "FACT_VISIT_TRACKER"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# fetching surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table pro_prod.fact_cms

# COMMAND ----------

# reading fact table data from processed layer table
CMS = spark.sql("""Select COMPLAINT_TRACKING_NO,first(REGISTERED_DATE) as REGISTERED_DATE,first(CUSTOMER_CODE) as CUSTOMER_CODE,first(CUSTOMER_NAME) as CUSTOMER_NAME,
                first(PRODUCT_TYPE) as PRODUCT_TYPE,
                first(CUSTOMER_CATEGORY) as CUSTOMER_CATEGORY,
                first(STATE_DESC) as STATE_DESC,
                sum(SUPPLIED_QTY) as SUPPLIED_QTY,
                sum(BREAKAGE_DEFECT_QTY) as BREAKAGE_DEFECT_QTY,
                sum(NET_LOSS_QTY) as NET_LOSS_QTY,first(ZONE_NAME) as ZONE_NAME,
                first(RLS) as RLS,first(SECONDARY_CUSTOMER) as SECONDARY_CUSTOMER,first(BUSINESS_UNIT) as BUSINESS_UNIT,first(COMPLAINT_STATUS) as COMPLAINT_STATUS,first(NO_OF_DAYS) as NO_OF_DAYS,first(ATTENDED_DATE_KEY) as ATTENDED_DATE_KEY,first(CUSTOMER_KEY) as CUSTOMER_KEY,first(COMPLAINT_TYPE_DESC) as COMPLAINT_TYPE_DESC,first(PARTY_CODE) as PARTY_CODE,first(PARTY_NAME) as PARTY_NAME from {}.{} group by COMPLAINT_TRACKING_NO """.format(processed_db_name,"FACT_CMS"))

# COMMAND ----------

# selecting columns
final_df = CMS.select(['COMPLAINT_TRACKING_NO','REGISTERED_DATE','CUSTOMER_CODE','CUSTOMER_NAME','PRODUCT_TYPE','CUSTOMER_CATEGORY','STATE_DESC','SUPPLIED_QTY','BREAKAGE_DEFECT_QTY','NET_LOSS_QTY','ZONE_NAME','RLS','SECONDARY_CUSTOMER','BUSINESS_UNIT','COMPLAINT_STATUS','NO_OF_DAYS','ATTENDED_DATE_KEY','CUSTOMER_KEY','COMPLAINT_TYPE_DESC','PARTY_CODE','PARTY_NAME'])

# COMMAND ----------

# writing data to processed layer 
final_df.write.mode('overwrite').parquet(processed_location+"FACT_VISIT_TRACKER")

# COMMAND ----------

# inserting log record to last execution details table in SQL
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
