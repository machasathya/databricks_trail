# Databricks notebook source
# Exception block used to raise an exception if source/target table is not passed
try:
  source_table = dbutils.widgets.get("table_name")
  if source_table is None:
    raise("source_table is not passed")
except Exception as e:
  dbutils.notebook.exit(str(e))
  
try:
  target_table = dbutils.widgets.get("table_name")
  if target_table is None:
    raise("target_table is not passed")
except Exception as e:
  dbutils.notebook.exit(str(e))

# COMMAND ----------

# Import statements
from datetime import datetime 
from ProcessMetadataUtility import ProcessMetadataUtility

# COMMAND ----------

# Commands used to get the values from ADF
processed_schema_name = dbutils.widgets.get("processed_schema_name")
schema_name = dbutils.widgets.get("schema_name")
db_url = dbutils.widgets.get("db_url")
db = dbutils.widgets.get("db")
user_name = dbutils.widgets.get("user_name")
scope = dbutils.widgets.get("scope")
processed_location = dbutils.widgets.get("processed_path")
curated_db_name = dbutils.widgets.get("curated_db")
processed_db_name = dbutils.widgets.get("processed_db")
password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])

# COMMAND ----------

# Variables used to run the notebook manually
# processed_location = "mnt/bi_datalake/prod/pro/FACT_INVENTORY"
# schema_name = "metadata_prod"
# curated_db_name = "cur_prod"
# processed_db_name = "pro_prod"
# source_table = "FACT_INVENTORY"
# db_url = 'tcp:hil-azr-sql-srve.database.windows.net'
# db= 'bi_analytics'
# user_name = 'hil-admin@hil-azr-sql-srve'
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# processed_schema_name = 'sales_semantic_prod'
# target_table = 'FACT_INVENTORY'

# COMMAND ----------

#initializing object for processMetadataUtility and getting the run time
pmu = ProcessMetadataUtility()
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# extracting the pro layer data to source_df dataframe
if "FACT_LOGISTICS_INVENTORY" in source_table:
  source_df = spark.sql("select snapshot_date_key, plant_key, matl_key, sum(BLKD_QTY) as BLKD_QTY, SUM(QLTY_QT) as QLTY_QT, SUM(RTRNS_QTY) as RTRNS_QTY, SUM(REST_QTY) as REST_QTY, SUM(UNREST_QTY) as UNREST_QTY, SUM(TRSN_QTY) as TRSN_QTY, SUM(TOTAL_MT) as TOTAL_MT, SUM(RECLAIMABLE_QTY) as RECLAIMABLE_QTY, SUM(DESTROYABLE_QTY) as DESTROYABLE_QTY, SUM(IN_TRANSIT_QTY) as IN_TRANSIT_QTY, SUM(GOOD_QTY) as GOOD_QTY, SUM(STOCK_CURING) as STOCK_CURING,LAST_EXECUTED_TIME ,sum(PRICE) as PRICE,sum(UNRESTRICTED_VALUE) as UNRESTRICTED_VALUE, sum(QUALITY_VALUE) as QUALITY_VALUE,sum(BLOCKED_VALUE) as BLOCKED_VALUE from {table_name}".format(table_name=processed_db_name + "." + source_table) +" group by snapshot_date_key, plant_key, matl_key,LAST_EXECUTED_TIME")
elif "FACT_NON_MOVING_MATERIALS" in source_table:
  source_df = spark.sql("select * from {table_name} where snapshot_date_KEY in (select distinct snapshot_date_key from {table_name} order by snapshot_date_key desc limit 3)".format(table_name = processed_db_name + "." + source_table))
else:
  source_df = spark.sql("select * from {table_name}".format(table_name=processed_db_name + "." + source_table))

# COMMAND ----------

# SQL DB Properties used to establish connection 
jdbcUrl = "jdbc:sqlserver://{0}:1433;database={1}".format(db_url[4:], db)
connectionProperties = {
  "user" : user_name,
  "password" : password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# Writing date to SQL Table
source_df.write.mode("overwrite").option("truncate", "true").jdbc(url=jdbcUrl, table=processed_schema_name + "." + target_table, properties=connectionProperties)

# COMMAND ----------

# Updating the log record in last_exection_details table
date_now = datetime.utcnow()
vals = "('PROCESSED_TO_SEMANTIC_" + source_table + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', 'PROCESSED_TO_SEMANTIC_" + source_table + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
