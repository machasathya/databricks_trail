# Databricks notebook source
# import statements
import time
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, lit, expr, to_date, to_timestamp, sum, min, when, count, concat,upper

# COMMAND ----------

# enabling cross join and increasing broadcast time to 36000 seconds
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set("spark.sql.broadcastTimeout", 36000)

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
table_name = "FACT_SALES_ORDERS_AGG"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the code manually
# from ProcessMetadataUtility import ProcessMetadataUtility
# pmu = ProcessMetadataUtility()
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# processed_location = "mnt/bi_datalake/prod/pro/"
# curated_db_name = "cur_prod"
# processed_db_name = "pro_prod"
# table_name = "FACT_SALES_ORDERS_AGG"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer for sales org 2000
VBAK = spark.sql("select * FROM {}.{}".format(curated_db_name, "VBAK")).select("VBELN","VKORG","KUNNR","AUART").where("VKORG = '2000'")

# COMMAND ----------

# reading data from curated layer
VBAP = spark.sql("select VBELN,POSNR,WERKS,NETWR,KWMENG,VRKME,MATNR,ABGRU,KONDM, BRGEW,GEWEI, ERDAT,SPART,NTGEW FROM {}.{}".format(curated_db_name, "VBAP")).withColumn("unit_price",expr("NETWR/KWMENG")).where("ABGRU is null  or  ABGRU != 51")

# COMMAND ----------

# reading data from curated layer
marm_kg = spark.sql("select * FROM {}.{} where MEINH in ('KG')".format(curated_db_name, "MARM")).drop("LAST_UPDATED_DT_TS")
marm_cum = spark.sql("select * FROM {}.{} where MEINH in ('M3')".format(curated_db_name, "MARM")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# reading data from processed layer
dim_prod = spark.sql("select * from {}.{}".format(processed_db_name,"dim_product"))

# COMMAND ----------

# defining order quantity and MT conversion value for each sales order
final_1 = VBAP.alias('vp').join(VBAK.alias('sales_header'), on=['VBELN'],how="INNER").select([col("vp."+cols) for cols in VBAP.columns])

final_2 = final_1.select(col("werks").alias("PLANT_ID"),col("MATNR").alias("MATERIAL_NUMBER"),col("erdaT").alias("CREATED_DATE"),col("KWMENG").alias("ORDER_QUANTITY"),col("VRKME").alias("SALES_UNIT"))

final_3 = final_2.alias("md").join(dim_prod.alias("dp"),on = [col("md.MATERIAL_NUMBER") == col("dp.material_number")],how='left').select([col("md." + cols) for cols in final_2.columns]+[col("MATERIAL_TYPE"),col("UNIT_OF_MEASURE"),col("PRODUCT_LINE")])

final_4 = final_3.alias("l_df").join(marm_kg.alias("r_df"),on=[col("l_df.MATERIAL_NUMBER") == col("r_df.MATNR")],how='left').select([col("l_df."+cols) for cols in final_3.columns] + [col("UMREZ"),col("UMREN")])

final_5 = final_4.withColumn("CONV_VAL",when(col("UMREN").isNotNull(),(col("UMREN")/col("UMREZ"))/1000)).drop(*["UMREN","UMREZ"])

final_6 = final_5.alias("l_df").join(marm_cum.alias("r_df"),on=[col("l_df.MATERIAL_NUMBER") == col("r_df.MATNR")],how='left').select([col("l_df."+cols) for cols in final_5.columns] + [col("UMREZ"),col("UMREN")])

final_7 = final_6.withColumn("CONV_VAL",when((col("UMREN").isNotNull()),(col("UMREN")/col("UMREZ"))).otherwise(col("CONV_VAL"))).drop(*["UMREN","UMREZ"])

final_8 = final_7.withColumn("ORDER_QTY_CONV",when(col("SALES_UNIT").isin(["MT","TO","M3"]),col("ORDER_QUANTITY")).when(col("SALES_UNIT") == 'KG',(col("ORDER_QUANTITY")/1000)))

final_9 = final_8.withColumn("ORDER_QTY_CONV",when(((col("ORDER_QTY_CONV").isNull()) & (col("CONV_VAL").isNotNull())),(col("ORDER_QUANTITY") * col("CONV_VAL")) ).otherwise(col("ORDER_QTY_CONV")))

final_df = final_9.groupBy("PLANT_ID","MATERIAL_NUMBER","CREATED_DATE").agg(sum("ORDER_QUANTITY").alias("ORDER_QUANTITY"),sum("ORDER_QTY_CONV").alias("ORDER_QTY_CONV"))
# display(VBAP.where("werks = '2006' and erdat>='2021-04-01' and erdat<='2021-04-30'"))

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'SALES_ORDERS_AGG', mode='overwrite')

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
ORDER_QUANTITY,
ORDER_QTY_CONV
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

# creating surrogate keys
d = surrogate_mapping_hil(csv_data, table_name, "SALES_ORDER_AGG", processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location + table_name)

# COMMAND ----------

# insert log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
