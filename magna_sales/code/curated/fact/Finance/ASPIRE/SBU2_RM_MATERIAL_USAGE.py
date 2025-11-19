# Databricks notebook source
# import statements
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType, DoubleType, DecimalType
from pyspark.sql import functions as sf
from pyspark.sql import Window
import time
from datetime import datetime
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
spark.conf.set("spark.sql.broadcastTimeout",36000)

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
table_name = "FACT_RM_MATERIAL_USAGE_SBU2"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

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
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_RM_MATERIAL_USAGE_SBU2"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
# csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
# pmu.close(db_connector, db_cursor)

# COMMAND ----------

# DBTITLE 0,MSEG Table
# reading data from curated layer
mseg = spark.sql("""SELECT mblnr,mjahr,zeile,bwart,matnr,werks,lgort,waers,dmbtr,menge,meins,erfme,aufnr,budat_mkpf FROM {}.{} where bwart in ('261','262') and werks in ('2006','2008','2010','2013','2017','2018','2020','2022')""".format(curated_db_name,"mseg")).where("aufnr is not null")

# COMMAND ----------

# DBTITLE 0,MARA Table
# reading data from curated layer
mara = spark.sql("""SELECT matnr,matkl,meins,brgew,volum FROM {}.{} """.format(curated_db_name,"mara"))

# COMMAND ----------

# DBTITLE 0,MARM Table
# reading data from curated layer
marm = spark.sql("""SELECT matnr,umren,umrez,meinh FROM {}.{} where meinh = 'KG' """.format(curated_db_name,"marm"))

# COMMAND ----------

# defining rm quantity in MT for each production order
df1 = mseg.select([col("aufnr").alias("PRODUCTION_ORDER"),col("matnr").alias("RM_MATERIAL_ID"),col("werks").alias("PLANT_ID"),col("bwart").alias("MOVEMENT_TYPE"),col("dmbtr").alias("RM_AMOUNT_IN_LC"),col("menge").alias("RM_QUANTITY"),col("MEINS").alias("BASE_UNIT_OF_MEASURE"),col("budat_mkpf").alias("POSTING_DATE")])

df2 = df1.withColumn("RM_QUANTITY",when(col("MOVEMENT_TYPE") == '261',col("RM_QUANTITY")).otherwise(-(col("RM_QUANTITY")))).withColumn("RM_AMOUNT_IN_LC",when(col("MOVEMENT_TYPE") == '261',col("RM_AMOUNT_IN_LC")).otherwise(-(col("RM_AMOUNT_IN_LC"))))

df3 = df2.alias("l_df").join(marm.alias("r_df"),on=[col("l_df.rm_material_id") == col("r_df.matnr")],how='left').select([col("l_df." + cols) for cols in df2.columns] + [col("r_df.UMREN"),col("r_df.UMREZ")])

final_df = df3.withColumn("RM_QUANTITY_MT",when(col("BASE_UNIT_OF_MEASURE").isin(["MT","TO"]),col("RM_QUANTITY")).otherwise(((col("RM_QUANTITY")*(col("UMREN")/col("UMREZ")))/1000)))

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'RM_MATERIAL_USAGE_SBU2', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION

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
                        " THEN -2 ELSE ifnull(A{count}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table,count=count)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} A{count} on {fact_table}.{fact_column} = A{count}.{dim_column} ".format(
            dim_table=dim_table, count=count,fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)    
  select_condition = select_condition[:-2]      
  query = """select
PRODUCTION_ORDER,
MOVEMENT_TYPE,
RM_AMOUNT_IN_LC,
RM_QUANTITY,
BASE_UNIT_OF_MEASURE,
RM_QUANTITY_MT
  ,{select_condition}  from {fact_name} {join_condition}
  """.format(
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
d = surrogate_mapping_hil(csv_data,table_name,"RM_MATERIAL_USAGE_SBU2",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# insert log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
