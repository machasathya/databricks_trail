# Databricks notebook source
# import statements
import time
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
from pyspark.sql import SQLContext
pmu = ProcessMetadataUtility()

# COMMAND ----------

# enabling cross join
spark.conf.set("spark.sql.crossJoin.enabled", "true")

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
table_name = "FACT_QUALITY_TRIAL_MATERIAL_DISPATCH"
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
# curated_db_name = "cur_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_QUALITY_TRIAL_MATERIAL_DISPATCH"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
df_trial_SBU1=spark.sql("select * from {}.{}".format(curated_db_name, "TRIAL_MATERIAL_DISPATCH_SBU1")).drop("LAST_UPDATED_DT_TS")
df_trial_SBU2=spark.sql("select * from {}.{}".format(curated_db_name, "TRIAL_MATERIAL_DISPATCH_SBU2")).drop("LAST_UPDATED_DT_TS")
df_trial_SBU3=spark.sql("select * from {}.{}".format(curated_db_name, "TRIAL_MATERIAL_DISPATCH_SBU3")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# seperating columns other than months
sbu1_col = [c for c in df_trial_SBU1.columns if c not in {'TRIAL_DATE', 'SBU', 'PLANT_ID','PRODUCT_LINE','TRIAL_MATERIAL_DETAILS','DISPATCH_DATE','DEALER_NAME','SUB_DEALER_NAME','MOBILE_NO','AREA','AREA_1','STATE','INVOICE_NO','TRUCK_NO','MATERIAL_ID','SIZE','BATCH_NO','CUSTOMER_FEEDBACK_FOR_SPECIFIC_CHARACTERISTIC','QTY_SUPPLIED'}]

sbu2_col = [c for c in df_trial_SBU2.columns if c not in {'TRIAL_DATE', 'SBU', 'PLANT_ID','PRODUCT_LINE','TRIAL_MATERIAL_DETAILS','DISPATCH_DATE','DEALER_NAME','SUB_DEALER_NAME','MOBILE_NO','AREA','AREA_1','STATE','INVOICE_NO','TRUCK_NO','MATERIAL_ID','SIZE','BATCH_NO','CUSTOMER_FEEDBACK_FOR_SPECIFIC_CHARACTERISTIC','QTY_SUPPLIED'}]

sbu3_col = [c for c in df_trial_SBU3.columns if c not in {'TRIAL_DATE', 'SBU', 'PLANT_ID','PRODUCT_LINE','TRIAL_MATERIAL_DETAILS','DISPATCH_DATE','DEALER_NAME','SUB_DEALER_NAME','MOBILE_NO','AREA','AREA_1','STATE','INVOICE_NO','TRUCK_NO','MATERIAL_ID','SIZE','BATCH_NO','CUSTOMER_FEEDBACK_FOR_SPECIFIC_CHARACTERISTIC','QTY_SUPPLIED'}]

# COMMAND ----------

#handling data for which the defect qty is not provided
if len(sbu1_col) == 0:
  sbu1 = df_trial_SBU1.withColumn("DEFECT_TYPE",lit('')).withColumn("DEFECT_QTY",lit('').cast("decimal(13,2)"))
else:
  sbu1 = df_trial_SBU1.selectExpr('TRIAL_DATE', 'SBU', 'PLANT_ID','PRODUCT_LINE','TRIAL_MATERIAL_DETAILS','DISPATCH_DATE','DEALER_NAME','SUB_DEALER_NAME','MOBILE_NO','AREA','AREA_1','STATE','INVOICE_NO','TRUCK_NO','MATERIAL_ID','SIZE','BATCH_NO','CUSTOMER_FEEDBACK_FOR_SPECIFIC_CHARACTERISTIC','QTY_SUPPLIED', "stack({}, {})".format(len(sbu1_col), ', '.join(("'{}', {}".format(i, i) for i in sbu1_col)))).withColumnRenamed("col0","DEFECT_TYPE").withColumnRenamed("col1","DEFECT_QTY")

# COMMAND ----------

#handling data for which the defect qty is not provided
if len(sbu2_col) == 0:
  sbu2 = df_trial_SBU2.withColumn("DEFECT_TYPE",lit('')).withColumn("DEFECT_QTY",lit('').cast("decimal(13,2)"))
else:
  sbu2 = df_trial_SBU2.selectExpr('TRIAL_DATE', 'SBU', 'PLANT_ID','PRODUCT_LINE','TRIAL_MATERIAL_DETAILS','DISPATCH_DATE','DEALER_NAME','SUB_DEALER_NAME','MOBILE_NO','AREA','AREA_1','STATE','INVOICE_NO','TRUCK_NO','MATERIAL_ID','SIZE','BATCH_NO','CUSTOMER_FEEDBACK_FOR_SPECIFIC_CHARACTERISTIC','QTY_SUPPLIED', "stack({}, {})".format(len(sbu2_col), ', '.join(("'{}', {}".format(i, i) for i in sbu2_col)))).withColumnRenamed("col0","DEFECT_TYPE").withColumnRenamed("col1","DEFECT_QTY")

# COMMAND ----------

#handling data for which the defect qty is not provided
if len(sbu3_col) == 0:
  sbu3 = df_trial_SBU3.withColumn("DEFECT_TYPE",lit('')).withColumn("DEFECT_QTY",lit('').cast("decimal(13,2)"))
else:
  sbu3 = df_trial_SBU3.selectExpr('TRIAL_DATE', 'SBU', 'PLANT_ID','PRODUCT_LINE','TRIAL_MATERIAL_DETAILS','DISPATCH_DATE','DEALER_NAME','SUB_DEALER_NAME','MOBILE_NO','AREA','AREA_1','STATE','INVOICE_NO','TRUCK_NO','MATERIAL_ID','SIZE','BATCH_NO','CUSTOMER_FEEDBACK_FOR_SPECIFIC_CHARACTERISTIC','QTY_SUPPLIED', "stack({}, {})".format(len(sbu3_col), ', '.join(("'{}', {}".format(i, i) for i in sbu3_col)))).withColumnRenamed("col0","DEFECT_TYPE").withColumnRenamed("col1","DEFECT_QTY")

# COMMAND ----------

# merging all sbu's data into one
sbu = sbu1.union(sbu2).union(sbu3)
final_df = sbu.withColumn("DEFECT_TYPE", sf.regexp_replace(col("DEFECT_TYPE"), "_DEFECT_QTY", ""))

# COMMAND ----------

# adding leading 0's to material id field
final_result = final_df.withColumn("MATERIAL_ID",lpad(col("MATERIAL_ID"),18,'0'))

# COMMAND ----------

# writing data to processed layer before generating surrogate key
final_result.write.parquet(processed_location+'TRIAL_MATERIAL_DISPATCH', mode='overwrite')

# COMMAND ----------

# surrogate key mapping
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
  
  final_result.createOrReplaceTempView("{}".format(fact_name))
  
  for row_dim_fact_mapping in dim_fact_mapping:
    
    fact_table = row_dim_fact_mapping["fact_table"]
    dim_table = row_dim_fact_mapping["dim_table"]
    fact_surrogate = row_dim_fact_mapping["fact_surrogate"]
    dim_surrogate = row_dim_fact_mapping["dim_surrogate"]
    fact_column = row_dim_fact_mapping["fact_column"]
    dim_column = row_dim_fact_mapping["dim_column"]
    
    spark.sql("refresh table {processed_db_name}.{dim_table}".format(processed_db_name=processed_db_name,
                                                                   dim_table=dim_table))
    
    if((fact_table_name==fact_table or fact_table_name.lower()==fact_table)):
      count += 1
      tmp_unmapped_str = "CASE WHEN "
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull(A{count}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table, count=count)     
      
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} A{count} on {fact_table}.{fact_column} = A{count}.{dim_column} ".format(
            dim_table=dim_table, count=count, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
      
  select_condition = select_condition[:-2]
  query = """select 
TRIAL_DATE,
{fact_name}.SBU,
TRIAL_MATERIAL_DETAILS,
DEALER_NAME,
SUB_DEALER_NAME,
MOBILE_NO,
AREA,
AREA_1,
{fact_name}.STATE,
INVOICE_NO,
TRUCK_NO,
{fact_name}.SIZE,
QTY_SUPPLIED,
BATCH_NO,
CUSTOMER_FEEDBACK_FOR_SPECIFIC_CHARACTERISTIC,
DEFECT_TYPE,
DEFECT_QTY,
  {select_condition}  from {fact_name}  {join_condition}
  """.format(
              join_condition=join_condition,fact_name=fact_name,select_condition=select_condition)

  print("\nFinal Query for {fact_table}\n (Total Surrogate Keys = {count}) :\n {query}".format(count=count, query=query,fact_table=fact_table))
  fact_final_view_surrogate = spark.sql(query)
  cols = []
  
  for item in fact_final_view_surrogate.columns:
    cols.append( "{} AS {}".format(item ,item.upper()))
  fact_final_view_surrogate = fact_final_view_surrogate.selectExpr(cols)
  #print(cols)
  return fact_final_view_surrogate

# COMMAND ----------

# creating surrogate keys
d = surrogate_mapping_hil(csv_data,table_name,"TRIAL_MATERIAL_DISPATCH",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate key
d.write.mode('overwrite').parquet(processed_location+"FACT_QUALITY_TRIAL_MATERIAL_DISPATCH")

# COMMAND ----------

# insert log data into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
