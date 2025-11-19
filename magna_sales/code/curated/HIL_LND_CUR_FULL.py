# Databricks notebook source
#import statements
import json
from datetime import datetime
from pyspark.sql.functions import *
import glob
import pyodbc
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, IntegerType, FloatType, LongType, \
    DecimalType, DateType, TimestampType
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
import numpy as np

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------

# commands to get the input from ADF.
table_name = dbutils.widgets.get("table_name")
source = dbutils.widgets.get("source")
delimiter = dbutils.widgets.get("delimiter")
full_file_pattern = dbutils.widgets.get("full_file_pattern")
full_load_landing_location = dbutils.widgets.get("base_source_location")
curated_location = dbutils.widgets.get("base_curated_location")
source = dbutils.widgets.get("source")
schema_name = dbutils.widgets.get("schema_name")
column_meta_table = dbutils.widgets.get("column_meta_table")
db_url = dbutils.widgets.get("db_url")
db = dbutils.widgets.get("db")
user_name = dbutils.widgets.get("user_name")
scope = dbutils.widgets.get('scope')
password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])

# COMMAND ----------

# Variables used to run the notebook manually
# table_name = "NSR_SBU1_PACKING_COST"
# delimiter = ","
# full_file_pattern = ""
# full_load_landing_location = "dbfs:/mnt/bi_datalake/prod/lnd/sftp/NSR_SBU1_PACKING_COST/"
# curated_location = "dbfs:/mnt/bi_datalake/prod/cur/NSR_SBU1_PACKING_COST/"
# column_meta_table = "column_meta"
# schema_name = "metadata_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# source = "SFTP"

# table_name = "COST_TRACKER"
# delimiter = ","
# full_file_pattern = ""
# full_load_landing_location = "dbfs:/mnt/bi_datalake/prod/lnd/sharepoint/COST_TRACKER/"
# curated_location = "dbfs:/mnt/bi_datalake/prod/cur/COST_TRACKER"
# column_meta_table = "column_meta"
# schema_name = "metadata_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# source = "SHAREPOINT"

# table_name = "cms_register_complaints"
# delimiter = "u0001"
# full_file_pattern = ""
# full_load_landing_location = "dbfs:/mnt/bi_datalake/prod/arc/dims/cms_register_complaints/"
# curated_location = "dbfs:/mnt/bi_datalake/prod/cur/cms_register_complaints/"
# column_meta_table = "column_meta"
# schema_name = "metadata_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# source = "DIMS"

# table_name = "ZBSEG"
# delimiter = "u0001"
# full_file_pattern = ""
# full_load_landing_location = "dbfs:/mnt/bi_datalake/prod/lnd/sap/ZBSEG/"
# curated_location = "dbfs:/mnt/bi_datalake/prod/cur/ZBSEG/"
# column_meta_table = "column_meta"
# schema_name = "metadata_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# source = "SAP"

# COMMAND ----------

# Initializing the object for ProcessMetadataUtility class
pmu = ProcessMetadataUtility()

# COMMAND ----------

# Establshing the connection to SQL DB and fetching the data from column_meta table
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
column_metadata = pmu.get_data(cursor=db_cursor, col_lookup=column_meta_table, value=table_name, column="TABLE_NAME",schema_name = schema_name)
column_metadata = column_metadata.filter("SOURCE_COLUMN_NAME != '{dummy_value}'".format(dummy_value = "DUMMY"))
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# Function used to convert the data types of the landing layer data to desired data types based on column_meta table
def hil_convert_dataframe_datatype(table_name, column_metadata, timestamp_format, date_format,source="NA"):
    """
    Utility to convert the source datatype to target type (configured through table_column_metadata)
    """
    source_table = spark.table(table_name)
    source_table_new = source_table
    try:
        filter_table_list = column_metadata.toJSON().collect()
        for source_table_item in filter_table_list:
            source_table_item_dict = json.loads(source_table_item)
            column_name = source_table_item_dict['DESTINATION_COLUMN_NAME']
            data_type = source_table_item_dict['DATA_TYPE']
            
            if data_type.lower() == 'timestamp':
                source_table_new = source_table_new.withColumn(column_name,
                                                               unix_timestamp(column_name, timestamp_format).cast(
                                                                   "timestamp"))
                
            if data_type.lower() == 'date' and source.lower() == "sharepoint":
              if source_table_item_dict["TABLE_NAME"] not in ('WC_ROCE','MIS_SUMMARY','COST_TRACKER'):
                source_table_new = source_table_new.withColumn(column_name, expr("date_add(to_date('1899-12-30'), cast({column_name} as int))".format(column_name = column_name)).cast(DateType()))
              else:
                expression = "to_date({column_name}, '{date_format}')".format(column_name=column_name,
                                                                              date_format=date_format)
                source_table_new = source_table_new.withColumn(column_name, expr(expression))
              
            if data_type.lower() == 'date':
                expression = "to_date({column_name}, '{date_format}')".format(column_name=column_name,
                                                                              date_format=date_format)
                source_table_new = source_table_new.withColumn(column_name, expr(expression))
            else:
                if data_type.lower() == 'string' and source.lower() == 'sharepoint' and (source_table_new.where(" {} like '%.0'".format(column_name)).count())>0: # any column that contains as string data type and by having one cell value that ends with '.0' will fall into the block
                  source_table_new = source_table_new.withColumn(column_name,
                                                             (source_table_new["{}".format(column_name)].cast(IntegerType())).cast(StringType()))
                else:
                  expression = "CAST({column_name} AS {data_type})".format(column_name=column_name, data_type=data_type)
                  source_table_new = source_table_new.withColumn(column_name, expr(expression))
        return source_table_new
    except Exception as e:
        print(e)
        return source_table_new

# COMMAND ----------

# Function used to convert the column names to appropriate column names in curated layer
def hil_convert_dataframe_columns(table_name, column_metadata):
    """
    Utility to convert the source datatype to target type (configured through table_column_metadata)
    """
    source_table = table_name
    source_table_new = source_table
    try:
      alias = []
      filter_table_list = column_metadata.toJSON().collect()
      for source_table_item in filter_table_list:
          source_table_item_dict = json.loads(source_table_item)
          column_name = source_table_item_dict['SOURCE_COLUMN_NAME']
          new_column_name = source_table_item_dict['DESTINATION_COLUMN_NAME']
          alias.append(col(column_name).alias(new_column_name))
      source_table_new = source_table_new.select(alias)
      return source_table_new
    except Exception as e:
        print(e)
        return source_table_new

# COMMAND ----------

# DBTITLE 0, #1
#FIX ADDED FOR HANDLING BLANK VALUES#1
def blank_as_null(x):
    return when(col(x) != "", col(x)).otherwise(None)

# COMMAND ----------

# Fetcing the available files available in landing location
landing_files = []
try:
  for file in dbutils.fs.ls(full_load_landing_location):
    landing_files.append(file.path)
except Exception as e:
  print(e)
  dbutils.notebook.exit(1)

# COMMAND ----------

landing_files

# COMMAND ----------

# reading the SAP data and other sources data seperately
if delimiter == 'u0001':
#   landing_df = spark.read.format('csv').options(header='true').option('delimiter', '\u0001').option("escape", "\"").load(landing_files)
  landing_df = spark.read.format('parquet').load(landing_files)
else:
  landing_df = spark.read.format('csv').options(header='true').option('delimiter', delimiter).load(landing_files)

# COMMAND ----------

# DBTITLE 0, #2
# FIX ADDED FOR HANDLING BLANK VALUES #2
string_fields = []
for i, f in enumerate(landing_df.schema.fields):
    if isinstance(f.dataType, StringType):
        string_fields.append(f.name)

# COMMAND ----------

# DBTITLE 0,Untitled
# function call to remove the unwanted columns from landing layer dataframe
for each in string_fields:
  landing_df = landing_df.withColumn(each,blank_as_null(each))

# COMMAND ----------

# function call to convert name of the columns
landing_df = hil_convert_dataframe_columns(table_name=landing_df, column_metadata=column_metadata)

# COMMAND ----------

 # script to add LAST_UPDATED_DT_TS column for the landing layer dataframe
date_now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
if table_name == "SIP_SALES":
  landing_df = landing_df.withColumn("FILE_NAME", reverse(split(input_file_name(), "/"))[0])
  landing_df = landing_df.withColumn("LAST_UPDATED_DT_TS", lit(date_now))
elif table_name == "RESB":
  landing_df = landing_df.withColumn("RSART",when(((col("RSART").isNull()) | (trim(col("RSART")) == '')),"<DEFAULT>").otherwise(col("RSART")))
  landing_df = landing_df.withColumn("LAST_UPDATED_DT_TS", lit(date_now))
else:
  landing_df = landing_df.withColumn("LAST_UPDATED_DT_TS", lit(date_now))
landing_df.registerTempTable("landing_table")

# COMMAND ----------

# Function call to convert the data type of the columns
if table_name in ['LOGISTICS_DAYWISE_SALE_SPLIT','PLANT_DAILY_TARGETS']:
  temp_df = landing_df.select("*")
elif source == 'SFTP':
  temp_df = hil_convert_dataframe_datatype(table_name="landing_table", timestamp_format="HHmmss", date_format="dd-MM-yyyy", column_metadata=column_metadata)
elif source == 'SHAREPOINT':
  temp_df = hil_convert_dataframe_datatype(table_name="landing_table", timestamp_format="HHmmss", date_format="dd-MM-yyyy", column_metadata=column_metadata,source = source)
else: 
  temp_df = hil_convert_dataframe_datatype(table_name="landing_table", timestamp_format="HHmmss", date_format="yyyyMMdd", column_metadata=column_metadata)

# COMMAND ----------

# Transformation logic for LOGISTICS_DAYWISE_SALE_SPLIT dataset
if table_name =='LOGISTICS_DAYWISE_SALE_SPLIT':
  pd_df = temp_df.drop("LAST_UPDATED_DT_TS").toPandas()
  df1 = (pd_df.set_index(["PLANT_DEPOT_ID","MATERIAL_CODE","DAYS_IN_MONTH","IS_PLANT"])
         .stack()
         .reset_index(name='VALUE')
         .rename(columns={'level_4':'DAY'}))
  df1['VALUE'] = df1['VALUE'].astype("int64")
  df1['DAY'] = df1['DAY'].astype("int64")
  df_sp =spark.createDataFrame(df1)
  plnt_30 = df_sp.select("PLANT_DEPOT_ID","MATERIAL_CODE","DAY","VALUE").where("IS_PLANT ='PLANT' and DAYS_IN_MONTH = 30").withColumnRenamed("VALUE","PLANT_WITH_30_DAYS")
  plnt_31 = df_sp.select("PLANT_DEPOT_ID","MATERIAL_CODE","DAY","VALUE").where("IS_PLANT ='PLANT' and DAYS_IN_MONTH = 31").withColumnRenamed("VALUE","PLANT_WITH_31_DAYS")
  fin_plnt = plnt_30.alias("a").join(plnt_31.alias("b"),on =[col("a.PLANT_DEPOT_ID") ==col("b.PLANT_DEPOT_ID"),col("a.MATERIAL_CODE") == col("b.MATERIAL_CODE"),col("a.DAY") == col("b.DAY")],how ='inner').select([col("a." + cols) for cols in plnt_30.columns] + [col("b.PLANT_WITH_31_DAYS")])
  dpt_30 = df_sp.select("PLANT_DEPOT_ID","MATERIAL_CODE","DAY","VALUE").where("IS_PLANT ='DEPOT' and DAYS_IN_MONTH = 30").withColumnRenamed("VALUE","DEPOT_WITH_30_DAYS")
  dpt_31 = df_sp.select("PLANT_DEPOT_ID","MATERIAL_CODE","DAY","VALUE").where("IS_PLANT ='DEPOT' and DAYS_IN_MONTH = 31").withColumnRenamed("VALUE","DEPOT_WITH_31_DAYS")
  fin_dpt = dpt_30.alias("a").join(dpt_31.alias("b"),on =[col("a.PLANT_DEPOT_ID") ==col("b.PLANT_DEPOT_ID"),col("a.MATERIAL_CODE") == col("b.MATERIAL_CODE"),col("a.DAY") == col("b.DAY")],how ='inner').select([col("a." + cols) for cols in dpt_30.columns] + [col("b.DEPOT_WITH_31_DAYS")])
  sale_split = fin_plnt.alias("a").join(fin_dpt.alias("b"),on =[col("a.PLANT_DEPOT_ID") ==col("b.PLANT_DEPOT_ID"),col("a.MATERIAL_CODE") == col("b.MATERIAL_CODE"),col("a.DAY") == col("b.DAY")],how ='inner').select([col("a." + cols) for cols in fin_plnt.columns] + [col("b.DEPOT_WITH_31_DAYS"),col("b.DEPOT_WITH_31_DAYS")])
  temp_df = sale_split.withColumn("LAST_UPDATED_DT_TS", lit(date_now))

# COMMAND ----------

# Transformation logic for PLANT_DAILY_TARGETS dataset 
if table_name=='PLANT_DAILY_TARGETS':
  pd_df = temp_df.drop("LAST_UPDATED_DT_TS").toPandas()
  df1 = (pd_df.set_index(["PLANT_ID","MATERIAL_ID","MONTH","YEAR"])
         .stack()
         .reset_index(name='VALUE')
         .rename(columns={'level_4':'DAY'}))
  df1['VALUE'] = df1['VALUE'].astype("int64")
  df1['DAY'] = df1['DAY'].astype("int64")
  df_sp =spark.createDataFrame(df1)
  target_df = df_sp.withColumn("target_date",to_date(unix_timestamp(concat(lpad(col("DAY"),2,'0'),lit("-"),lpad(col("MONTH"),2,'0'),lit("-"),col("YEAR")),'dd-MM-yyyy').cast('timestamp'))).withColumnRenamed("VALUE","PRODUCTION_PLAN").withColumnRenamed("TARGET_DATE","DATE").select("PLANT_ID","MATERIAL_ID","PRODUCTION_PLAN","DATE").where("DATE is not null")
  temp_df = target_df.withColumn("LAST_UPDATED_DT_TS", lit(date_now))

# COMMAND ----------

# Eliminating null records  in cost tracker data set
if table_name =='COST_TRACKER':
  temp_df = temp_df.where("PARTICULARS is not NULL")

# COMMAND ----------

# Transformation logic for SALES_TARGETS dataset
if table_name == "SALES_TARGETS":
  temp_df = temp_df.withColumn("FILE_DATE",date_now)

# COMMAND ----------

# Writing the data to curated location in delta format
temp_df.write.format("delta").mode("overwrite").save(curated_location)

# COMMAND ----------

# Exit command
dbutils.notebook.exit({'landing_file_names':landing_files})
