# Databricks notebook source
# import statements
import os
from datetime import datetime, timedelta,date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, split, concat, when, count, concat,upper,to_date, lpad, first,last_day, datediff,date_format,date_add,explode,row_number, sum, max as max_,trim,lpad
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType, DecimalType
from pyspark.sql import functions as sf
import pandas as pd
from pyspark.sql.window import Window
spark.conf.set( "spark.sql.crossJoin.enabled" , "true")
spark.conf.set("spark.sql.broadcastTimeout", 36000)

# COMMAND ----------

# Generating current date
now = datetime.now()
day_n = now.day
now_hour = now.time().hour
ist_zone = datetime.now() + timedelta(hours=5.5)
var_date = (ist_zone - timedelta(days=1)).strftime("%Y-%m-%d")
var_date

# COMMAND ----------

# declaring class for ProcessMetadataUtility
pmu = ProcessMetadataUtility()

# COMMAND ----------

# parameters to get the values from adf
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
table_name = "FACT_INVENTORY"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# Variables used to run the notebook manually
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
# table_name = "FACT_INVENTORY"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# check_date = (last_processed_time - timedelta(days=1)).strftime("%Y-%m-%d")

# COMMAND ----------

# Fetching surrogate_meta from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

final_df = spark.read.format('csv').options(header='true').option('delimiter', ',').load('dbfs:/mnt/bi_datalake/test/')
final_df = final_df.withColumn('Material',when(( col('Material').isin(['S7000028','S7000109','S7000152','S7000169','S7000168','S7000151','S7000049']) ), col('Material') ).otherwise( lpad('Material',18,'0')))

final_df = final_df.select(col("Material").alias("MATERIAL_ID"),col("Plant").alias("PLANT_ID"),col("STORAGE_LOCATION"),col("UNREST_QTY").cast("decimal(13,3)"),col("QUALITY_QTY").cast("decimal(13,3)"),col("BLOCKED_QTY").cast("decimal(13,3)"),col('IN_TRANSIT_QTY').cast("decimal(14,3)"),col("MT_CONV_VALUE").cast("decimal(23,11)"),col('CUM_CONV_VALUE').cast("decimal(11,6)"),col("Date").alias("SNAPSHOT_DATE").cast("date"),col('PRICE').cast("decimal(11,2)")).withColumn("LAST_EXECUTED_TIME", lit(None))

final_df.na.fill({"UNREST_QTY":0,"QUALITY_QTY":0,"BLOCKED_QTY":0,"IN_TRANSIT_QTY":0})

# COMMAND ----------

# --------------------SURROGATE IMPLEMENTATION------------------------

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
  final_df.createOrReplaceTempView("{}".format(fact_name))
  for row_dim_fact_mapping in dim_fact_mapping:
    
    fact_table = row_dim_fact_mapping["fact_table"]
    dim_table = row_dim_fact_mapping["dim_table"]
    fact_surrogate = row_dim_fact_mapping["fact_surrogate"]
    dim_surrogate = row_dim_fact_mapping["dim_surrogate"]
    fact_column = row_dim_fact_mapping["fact_column"]
    dim_column = row_dim_fact_mapping["dim_column"]
    spark.sql("refresh table {processed_db_name}.{dim_table}".format(processed_db_name=processed_db_name,
                                                                   dim_table=dim_table))
  
    if(fact_table_name==fact_table or fact_table_name.lower()==fact_table ):
      
      count += 1
      tmp_unmapped_str = "CASE WHEN "
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull({dim_table}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} on {fact_table}.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)    
  select_condition = select_condition[:-2]
  query = """select 
  STORAGE_LOCATION,
  UNREST_QTY,
QUALITY_QTY,
BLOCKED_QTY,
IN_TRANSIT_QTY,
MT_CONV_VALUE,
CUM_CONV_VALUE,
PRICE,
LAST_EXECUTED_TIME
,{select_condition}  from {fact_name} {join_condition}""".format(
              join_condition=join_condition,fact_name=fact_name,select_condition=select_condition)

  print("\nFinal Query: " +query)
  fact_final_view_surrogate = spark.sql(query)
  cols = []
  
  for item in fact_final_view_surrogate.columns:
    cols.append( "{} AS {}".format(item ,item.upper()))
  fact_final_view_surrogate = fact_final_view_surrogate.selectExpr(cols)
  #print(cols)
  return fact_final_view_surrogate
  

# COMMAND ----------

# Generating surrogate key
final = surrogate_mapping_hil(csv_data,table_name,"MARD_INVENTORY",processed_db_name)
final.createOrReplaceTempView("d")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS pr1

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# fetching existing data present in pro layer table
tab = processed_db_name + "." + table_name
pro_df = spark.read.table(tab).select( "STORAGE_LOCATION","UNREST_QTY","QUALITY_QTY","BLOCKED_QTY","IN_TRANSIT_QTY","MT_CONV_VALUE","CUM_CONV_VALUE","PRICE","LAST_EXECUTED_TIME","MATERIAL_KEY","PLANT_KEY","SNAPSHOT_DATE_KEY").write.saveAsTable("pr1")

# COMMAND ----------

full_df = spark.sql("select A.STORAGE_LOCATION,A.UNREST_QTY,A.QUALITY_QTY,A.BLOCKED_QTY,A.IN_TRANSIT_QTY,A.MT_CONV_VALUE,A.CUM_CONV_VALUE,A.PRICE,A.LAST_EXECUTED_TIME,A.MATERIAL_KEY,A.PLANT_KEY,A.SNAPSHOT_DATE_KEY from pr1 A left outer join (select distinct(SNAPSHOT_DATE_KEY)  as SNAPSHOT_DATE_KEY from d) T on A.SNAPSHOT_DATE_KEY=T.SNAPSHOT_DATE_KEY where T.SNAPSHOT_DATE_KEY is null")

# COMMAND ----------

# Union of incremental data with existing data
union_df = full_df.union(final)

# COMMAND ----------

# writing final data to processed location
union_df.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# Inserting log record to LAST_EXECUTION_DETAILS table 
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
