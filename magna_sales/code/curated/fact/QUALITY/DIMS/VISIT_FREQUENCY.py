# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,upper,concat,countDistinct
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
pmu = ProcessMetadataUtility()
from pyspark.sql.functions import countDistinct

# COMMAND ----------

# parameters to fetch the data from ADF
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
table_name = "VISIT_FREQUENCY"
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
# processed_schema_name = "global_semantic_prod"
# processed_db_name = "pro_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "VISIT_FREQUENCY"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# fetching surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table pro_prod.FACT_VISIT_TRACKER

# COMMAND ----------

#reading visit tracker data from processed layer
visit_track = spark.sql("""select PRODUCT_TYPE,STATE_DESC,SECONDARY_CUSTOMER,PARTY_NAME,ATTENDED_DATE_KEY,CUSTOMER_KEY from {}.{} where ATTENDED_DATE_KEY IS NOT NULL AND BUSINESS_UNIT = 'SBU1'""".format(processed_db_name,"FACT_VISIT_TRACKER"))

# COMMAND ----------

# reading dimension data from procesed layer
dim_date = spark.sql("select yr_name,date_created_key,MONTH_OF_YR_NUM from {}.{}".format(processed_db_name,"dim_date"))
dim_customer = spark.sql("select customer_key,name1 from {}.{}".format(processed_db_name,"dim_customer"))

# COMMAND ----------

# fetching year and month of year num
interm1 = visit_track.alias("l_df").join(dim_date.alias("r_df"),on=[col("ATTENDED_DATE_KEY") == col("DATE_CREATED_KEY")],how='left').select(["l_df." + cols for cols in visit_track.columns] + [col("YR_NAME"),col("MONTH_OF_YR_NUM")])

# defining fy name
interm2 = interm1.withColumn("FY_NAME_DISPLAY",when(((col("MONTH_OF_YR_NUM")>=4) & (col("MONTH_OF_YR_NUM")<=12)),(col("YR_NAME")+1)).otherwise(col("YR_NAME")))

interm3 = interm2.alias("visits").join(dim_customer.alias("customer"),on=[col("visits.CUSTOMER_KEY") == col("customer.CUSTOMER_KEY") ] , how='left' ).select(["visits." + cols for cols in interm2.columns] + [col("NAME1")])

# defining customer name
interm4 = interm3.withColumn("EFFECTIVE_CUSTOMER_NAME",when(col("NAME1").isNotNull(),col("NAME1")).otherwise( 
  when(col("SECONDARY_CUSTOMER").isNotNull(),col("SECONDARY_CUSTOMER")).otherwise("PARTY_NAME") ))

interm4 = interm4.filter("ATTENDED_DATE_KEY is not null")
final_df = interm4.groupBy("PRODUCT_TYPE","STATE_DESC","FY_NAME_DISPLAY","EFFECTIVE_CUSTOMER_NAME","NAME1","SECONDARY_CUSTOMER","PARTY_NAME").agg(countDistinct("ATTENDED_DATE_KEY").alias("VISIT_COUNT"))
#final_df.show()

# COMMAND ----------

# writing data to processed layer
final_df.write.mode('overwrite').parquet(processed_location+"VISIT_FREQUENCY")

# COMMAND ----------

# inserting log record to last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
