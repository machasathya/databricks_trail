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
import pyodbc
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, IntegerType, FloatType, LongType, \
    DecimalType, DateType, TimestampType
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
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# Object initialization for ProcessMetadataUtility class
pmu = ProcessMetadataUtility()

# COMMAND ----------

# Establishing connection to SQL and fetching data from LAST_EXECUTION_DETAILS table
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup="LAST_EXECUTION_DETAILS", value="PROCESSED_TO_SEMANTIC_" + source_table, column="FACT_NAME",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# Extracting the last exected time from the csv_data dataframe
last_executed_time = csv_data.selectExpr("max(LAST_EXECUTION_TIME) as max_date").collect()[0]['max_date']
last_executed_time_str = last_executed_time.strftime("%Y-%m-%d %H:%M:%S")
print(last_executed_time_str)

# COMMAND ----------

# Fetching the incremental data from the pro layer tables
if "FACT_CUST_PAYMENT_OUTSTANDING" in source_table or "FACT_PENDING_ORDER_SNAPSHOT" in source_table or "FACT_OVERDUE_AMOUNT" in source_table or "FACT_SECURITY_DEPOSIT" in source_table or 'FACT_INVENTORY' in source_table:
  source_df = spark.sql("select * from {table_name} where snapshot_date_key in (select distinct snapshot_date_key from {table_name} where LAST_EXECUTED_TIME > '".format(table_name=processed_db_name + "." + source_table) + last_executed_time_str + "')")
elif "FACT_LOGISTICS_INVENTORY" in source_table:
  source_df = spark.sql("select snapshot_date_key, plant_key, matl_key, sum(BLKD_QTY) as BLKD_QTY, SUM(QLTY_QT) as QLTY_QT, SUM(RTRNS_QTY) as RTRNS_QTY, SUM(REST_QTY) as REST_QTY, SUM(UNREST_QTY) as UNREST_QTY, SUM(TRSN_QTY) as TRSN_QTY, SUM(TOTAL_MT) as TOTAL_MT, SUM(RECLAIMABLE_QTY) as RECLAIMABLE_QTY, SUM(DESTROYABLE_QTY) as DESTROYABLE_QTY, SUM(IN_TRANSIT_QTY) as IN_TRANSIT_QTY, SUM(GOOD_QTY) as GOOD_QTY, SUM(STOCK_CURING) as STOCK_CURING, sum(PRICE) as PRICE,sum(UNRESTRICTED_VALUE) as UNRESTRICTED_VALUE, sum(QUALITY_VALUE) as QUALITY_VALUE,sum(BLOCKED_VALUE) as BLOCKED_VALUE,stock_taking_date, LAST_EXECUTED_TIME from {table_name} where snapshot_date_key in (select distinct snapshot_date_key from {table_name} where LAST_EXECUTED_TIME > '".format(table_name=processed_db_name + "." + source_table) + last_executed_time_str + "') group by snapshot_date_key, plant_key, matl_key,stock_taking_date, LAST_EXECUTED_TIME")
else:
  source_df = spark.sql("select * from {table_name} where LAST_EXECUTED_TIME > '".format(table_name=processed_db_name + "." + source_table) + last_executed_time_str + "'")

# COMMAND ----------

# Declaring the properties of the SQL DB
jdbcUrl = "jdbc:sqlserver://{0}:1433;database={1}".format(db_url[4:], db)
connectionProperties = {
  "user" : user_name,
  "password" : password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# Writing the pro layer data to STG table in SQL DB
source_df.write.mode("overwrite").option("truncate", "true").jdbc(url=jdbcUrl, table=processed_schema_name + ".STG_" + target_table, properties=connectionProperties)

# COMMAND ----------

# Updating the log record in LAST_EXECUTION_DETAILS table 
date_now = datetime.utcnow()
vals = "('PROCESSED_TO_SEMANTIC_" + source_table + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', 'PROCESSED_TO_SEMANTIC_" + source_table + "', 'INCREMENTAL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
