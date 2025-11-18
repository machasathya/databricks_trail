# Databricks notebook source
# import statements
from datetime import datetime 
from pyspark.sql.functions import col, lit, expr, to_date, to_timestamp, sum, min, when, count, concat ,first,upper
from pyspark.sql.types import *
from ProcessMetadataUtility import ProcessMetadataUtility

# COMMAND ----------

# declaring class object to processmetadatautility class
pmu = ProcessMetadataUtility()

# COMMAND ----------

# code to fetch the data from ADF parameters
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
table_name = "FACT_RLS_SALES"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the code manually
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata"
# db_url = "tcp:azr-hil-sql-srvr.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@azr-hil-sql-srvr"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# processed_db_name = "pro_prod"
# table_name = "FACT_RLS_SALES"
# curated_db_name = "cur_prod"
# processed_location = "mnt/bi_datalake/prod/pro/"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading data from curated layer
grps_df=spark.sql("select * from {}.{}".format(curated_db_name, "GROUPS")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# reading data from curated layer
bi_grps_df=spark.sql("select * from {}.{}".format(curated_db_name, "BI_GROUPS_SALES")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# joining to get the group and geo level mapping
rls=bi_grps_df.alias("df1").join(grps_df.alias("df2"),on=[col("df1.GROUP")==col("df2.GROUP")],how='left')

# changing all values as upper case
rls_df=rls.select("USER_MAIL_ID","GEO_UNIT","GEO_UNIT_TYPE","PRODUCT_LINE","SALE_TYPE").withColumn('GEO_UNIT',upper(col('GEO_UNIT'))).withColumn('GEO_UNIT_TYPE',upper(col('GEO_UNIT_TYPE'))).withColumn('PRODUCT_LINE',upper(col('PRODUCT_LINE'))).withColumn('SALE_TYPE',upper(col('SALE_TYPE')))
rls_df=rls_df.fillna({'SALE_TYPE':"<DEFAULT>"})

# COMMAND ----------

# refreshing dim_territory table
spark.sql("refresh table {}.{}".format(processed_db_name,"DIM_TERRITORY"))
spark.catalog.refreshTable("{}.{}".format(processed_db_name,"DIM_TERRITORY"))

# COMMAND ----------

# reading dim territory table and adding state and zone text values
ter_df=spark.sql("select * from {}.{}".format(processed_db_name, "DIM_TERRITORY"))
ter_df=ter_df.withColumn('STATE_TXT',upper(col('STATE_TXT'))).withColumn('ZONE_TXT',upper(col('ZONE_TXT')))

# COMMAND ----------

# filtering state data and joining with territory dataframe
state_df=rls_df.filter(rls_df['GEO_UNIT_TYPE']=='STATE').select("*")
rls_f1=state_df.alias("df1").join(ter_df.alias("df2"),on=[col("df1.GEO_UNIT") == col("df2.STATE"),col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.SALE_TYPE") == col("df2.SALE_TYPE")], how='inner').select([col("df1." +columns)for columns in state_df.columns]+[col("RLS")]).distinct()

# COMMAND ----------

# filtering zone data and joining with territory dataframe
zone_df=rls_df.filter(rls_df['GEO_UNIT_TYPE']=='ZONE').select("*")
rls_f2=zone_df.alias("df1").join(ter_df.alias("df2"),on=[col("df1.GEO_UNIT") == col("df2.ZONE_TXT"),col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.SALE_TYPE") == col("df2.SALE_TYPE")], how='inner').select([col("df1." +columns)for columns in zone_df.columns]+[col("RLS")]).distinct()
# display(rls_f2)

# COMMAND ----------

# filtering global level data and joining with territory dataframe
global_df=rls_df.filter(rls_df['GEO_UNIT_TYPE']=='GLOBAL').select("*")
rls_f3=global_df.alias("df1").join(ter_df.alias("df2"),on=[col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.SALE_TYPE") == col("df2.SALE_TYPE")], how='inner').select([col("df1." +columns)for columns in global_df.columns]+[col("RLS")]).distinct()
# display(rls_f3)

# COMMAND ----------

# filtering records which doesn't have product line defined
others_df=rls_df.where(((col('PRODUCT_LINE')=='OTHERS') | (col('PRODUCT_LINE')=='SBU1_OTHERS')| (col('PRODUCT_LINE')=='SBU2_OTHERS') | (col('PRODUCT_LINE')=='SBU3_OTHERS')))

# defining rls string
rls_f4 = others_df.withColumn("RLS",lit(None).cast("string")).select("USER_MAIL_ID","GEO_UNIT","GEO_UNIT_TYPE","PRODUCT_LINE","SALE_TYPE","RLS")

# COMMAND ----------

# merging all dataframes
rls_final=rls_f1.union(rls_f2).union(rls_f3).union(rls_f4).select("USER_MAIL_ID","PRODUCT_LINE","SALE_TYPE","RLS")

# COMMAND ----------

# writing data to processed layer
rls_final.write.mode('overwrite').parquet(processed_location+"FACT_RLS")

# COMMAND ----------

# inserting log record to last excution details
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)

