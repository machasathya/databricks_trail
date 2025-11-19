# Databricks notebook source
# Import Statements
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType, DoubleType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql import Window
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
spark.conf.set("spark.sql.broadcastTimeout",  36000)

# COMMAND ----------

# class object definition
pmu = ProcessMetadataUtility()

# COMMAND ----------

# Variables that are used to get and store the information coming from ADF Pipeline and
# Establishing connection to SQL DB to fetch the Surrogate key meta from fact_surrogate_meta table
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
table_name = "FACT_MATERIAL_USAGE"
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
# processed_schema_name = "global_semantic_prod"
# processed_db_name = "pro_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_MATERIAL_USAGE"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
# csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
# pmu.close(db_connector, db_cursor)

# COMMAND ----------

# DBTITLE 0,MSEG Table
# Reading data  from curated layer
mseg = spark.sql("""SELECT werks,matnr,BUDAT_MKPF,menge,dmbtr,bwart,meins,aufnr FROM {}.{} where werks in ('2019','2021','2029','2031','2032') and bwart in ('201','202','261','262','101','102','531','532')""".format(curated_db_name,"mseg"))
mseg.createOrReplaceTempView("mseg_df_tmp")

# COMMAND ----------

# Reading data from curated layer
marm_kg = spark.sql("select * FROM {}.{}".format(curated_db_name, "MARM")).where(col("MEINH").isin(["KG"]))

# COMMAND ----------

# Reading data from curated layer
zsbu3 = spark.sql("select * from {}.{}".format(curated_db_name,"ZSBU3"))

# COMMAND ----------

# Reading data from curated layer
caufv = spark.sql("select * from {}.{}".format(curated_db_name,"caufv"))

# COMMAND ----------

# fetching marm conversion value
final_data2 = mseg.join(marm_kg, on=['MATNR'], how='left').select([col(cols) for cols in mseg.columns] +[col("UMREZ"),col("UMREN")])

# COMMAND ----------

# Fetching zsbu3 conversion value
final_data3 = final_data2.alias('ls1').join(zsbu3,[final_data2["matnr"] == zsbu3["matnr"],
                                                final_data2["werks"] == zsbu3["werks"]],
                                                how='left').select(
  [col("ls1." + cols) for cols in final_data2.columns] + [col("STD_WT").alias("ZSBU3_STD_WT"),col("VALID_FROM"),col("VALID_TO")]
).where((col("BUDAT_MKPF").between(col("VALID_FROM"),col("VALID_TO"))) | ((col("VALID_FROM").isNull()) & (col("VALID_TO").isNull()))).drop("VALID_FROM","VALID_TO")

# COMMAND ----------

# defining quantity column by multiplying actual with conversion value
final_data4_temp = final_data3.withColumn("QUANTITY", when(col("UMREN").isNotNull(),col("menge")*(col("UMREN")/col("UMREZ"))).when(col("ZSBU3_STD_WT").isNotNull(),col("menge")*col("ZSBU3_STD_WT")).when(col("meins") == 'KG',col("menge")))

# COMMAND ----------

# fetching production order created date
final_data4 = final_data4_temp.alias("mseg_with_weights").join(caufv,final_data4_temp["aufnr"] == caufv["aufnr"],how='left').select([col("mseg_with_weights." + cols) for cols in final_data4_temp.columns] + [col("ERDAT").alias("PRODUCTION_ORDER_DATE")])

# COMMAND ----------

# handling reversal movement types for quantity and value fields
final_data6 = final_data4.withColumn("CONSUMED_QUANTITY",when((col("bwart").isin(["201","261"])),col("QUANTITY"))
                                     .when(col("bwart").isin(["202","262"]), -1 * col("QUANTITY")).otherwise(0))
final_data7 = final_data6.withColumn("CONSUMED_VALUE",when((col("bwart").isin(["201","261"])),col("dmbtr"))
                                     .when((col("bwart").isin(["202","262"])), -1 * col("dmbtr")).otherwise(0))
final_data8 = final_data7.withColumn("PRODUCTION_QUANTITY",when((col("bwart").isin(["101"])),col("QUANTITY"))
                                     .when((col("bwart").isin(["102"])),-1*col("QUANTITY")).otherwise(0))
final_data9 = final_data8.withColumn("PRODUCTION_VALUE",when((col("bwart").isin(["101"])),col("dmbtr"))
                                     .when((col("bwart").isin(["102"])),-1*col("dmbtr")).otherwise(0))
final_data10 = final_data9.withColumn("SCRAP_QUANTITY",when((col("bwart").isin(["531"])),col("QUANTITY"))
                                     .when((col("bwart").isin(["532"])),-1*col("QUANTITY")).otherwise(0))
final_data11 = final_data10.withColumn("SCRAP_QUANTITY_VALUE",when((col("bwart").isin(["531"])),col("dmbtr"))
                                     .when((col("bwart").isin(["532"])),-1*col("dmbtr")).otherwise(0))
final_date = final_data11.withColumnRenamed("werks","PLANT_ID").withColumnRenamed("matnr","MATERIAL_ID").withColumnRenamed("BUDAT_MKPF","DATE").withColumn("SAP_CLOSING_ADJ_DATE",when(col("aufnr").isNull(),col("DATE")).otherwise(col("PRODUCTION_ORDER_DATE")))

# COMMAND ----------

# calculating final MT conversion values
final_data = final_date.withColumn("CONSUMED_QUANTITY_MT",col("CONSUMED_QUANTITY")/1000).withColumn("PRODUCTION_QUANTITY_MT",col("PRODUCTION_QUANTITY")/1000).withColumn("SCRAP_QUANTITY_MT",col("SCRAP_QUANTITY")/1000)

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_data.write.parquet(processed_location+'MATERIAL_USAGE', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION
# From the surrogate table, we will be mapping the columns(which have keys) to the columns in the final table
# All the columns such as Data, Plant, Material will be changed to distinct keys
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
  final_data.createOrReplaceTempView("{}".format(fact_name))
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
   CONSUMED_QUANTITY,
   CONSUMED_VALUE,
   PRODUCTION_QUANTITY,
   PRODUCTION_VALUE,
   SCRAP_QUANTITY,
   SCRAP_QUANTITY_VALUE,
   CONSUMED_QUANTITY_MT,
   PRODUCTION_QUANTITY_MT,
   SCRAP_QUANTITY_MT
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

# Calling surrogate key function 
d = surrogate_mapping_hil(csv_data,table_name,"MATERIAL_USAGE",processed_db_name)

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
