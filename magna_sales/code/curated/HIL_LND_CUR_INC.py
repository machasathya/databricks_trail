# Databricks notebook source
# Import statements
import json
from datetime import datetime
from pyspark.sql.functions import *
import glob
import pyodbc
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, IntegerType, FloatType, LongType, \
    DecimalType, DateType, TimestampType
from ProcessMetadataUtility import ProcessMetadataUtility

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
primary_keys_list = dbutils.widgets.get("primary_keys_list")
curated_db = dbutils.widgets.get("curated_db")
last_updated_column = dbutils.widgets.get("last_updated_column")
incr_column_meta_table = dbutils.widgets.get("incr_column_meta_table")

# COMMAND ----------

# Variables used to run the notebook manually
# table_name = "WC_ROCE"
# source = "SHAREPOINT"
# delimiter = ","
# full_file_pattern = ""
# full_load_landing_location = "dbfs:/mnt/bi_datalake/prod/lnd/sharepoint/WC_ROCE/"
# curated_location = "dbfs:/mnt/bi_datalake/prod/cur/WC_ROCE"
# curated_db = "cur_prod"
# column_meta_table = "column_meta"
# schema_name = "metadata_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# primary_keys_list = "HIL,SBU,PRODUCT_LINE,DATE,ACTUAL_OR_BUDGET"
# incr_column_meta_table = "incr_column_meta"

# COMMAND ----------

# list that stores table names for which the data type of the dynamic columns will be integers
integer_columns_tables = ["PRODUCT_BENCHMARKING_SBU1,PRODUCT_BENCHMARKING_SBU2","PRODUCT_BENCHMARKING_SBU3","FIELD_COMPETITOR_SURVEY_AC_SHEETS","FIELD_COMPETITOR_SURVEY_CC_SHEETS","FIELD_COMPETITOR_SURVEY_NON_AC_SHEETS","FIELD_COMPETITOR_SURVEY_TILE_ADHESIVE","FIELD_COMPETITOR_SURVEY_SMART_PLASTER","FIELD_COMPETITOR_SURVEY_SMART_FIX","FIELD_COMPETITOR_SURVEY_PANELS","FIELD_COMPETITOR_SURVEY_BOARDS","FIELD_COMPETITOR_SURVEY_BLOCKS","FIELD_COMPETITOR_SURVEY_PIPES_FITTINGS","FIELD_COMPETITOR_SURVEY_WALL_PUTTY"]

# list that stores table names for which the data type of the dynamic columns will be decimal
decimal_columns_tables = ["PLANT_RATING_SBU1","PLANT_RATING_SBU2","PLANT_RATING_SBU3","TRIAL_MATERIAL_DISPATCH_SBU1","TRIAL_MATERIAL_DISPATCH_SBU2","TRIAL_MATERIAL_DISPATCH_SBU3"]

# COMMAND ----------

# Object initialization for ProcessMetadataUtility class
pmu = ProcessMetadataUtility()

# COMMAND ----------

# Establshing the connection to SQL DB and fetching the data from column_meta table
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
column_metadata = pmu.get_data(cursor=db_cursor, col_lookup=column_meta_table, value=table_name, column="TABLE_NAME",schema_name = schema_name)
column_metadata = column_metadata.filter("SOURCE_COLUMN_NAME != '{dummy_value}'".format(dummy_value = "DUMMY"))
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# Establshing the connection to SQL DB and fetching the data from incr_column_meta table
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
incr_column_metadata = pmu.get_data(cursor=db_cursor, col_lookup=incr_column_meta_table, value=table_name, column="TABLE_NAME",schema_name = schema_name)
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

# Function used to get the incremental record count
def inc_record_count(method,table_name,last_updated_column,curated_db,db_url,user_name,password,db,schema_name):
  if method == 'insert':
    final_record_count = '-1'
    current_rec_count = '-1'
    incr_rec_count = str(spark.sql("select count(*) as incr_count from source_table").first()['incr_count'])
    date_now = datetime.utcnow()
    vals = "('" + table_name + "_" + last_updated_column + "_"+date_now.strftime("%Y-%m-%d")+"', '" + table_name + "', '"+current_rec_count+"', '"+ incr_rec_count + "','" +final_record_count+"','"+ date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
    db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
    pmu.insert(db_connector, db_cursor, schema_name, "RECORD_COUNTS", "(UNIQUE_ID, TABLE_NAME, INITIAL_RECORD_COUNT, INCR_RECORD_COUNT,FINAL_RECORD_COUNT, DATE)", vals)
    pmu.close(db_connector, db_cursor)
  elif method == 'update':
    merged_rec_count = '-1'
    date_now = datetime.utcnow()
    condition = "UNIQUE_ID="+"'"+ table_name + "_" + last_updated_column + "_"+date_now.strftime("%Y-%m-%d")+"'"
    val = "FINAL_RECORD_COUNT="+"'"+merged_rec_count+"',END_DATE='"+date_now.strftime("%Y-%m-%d %H:%M:%S")+"'"
#     vals = "('" + table_name + "_" + last_updated_column + "_"+date_now.strftime("%Y-%m-%d")+"', '" + table_name + "', '"+current_rec_count+"', '"+ incr_rec_count + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
    db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
    pmu.update(db_connector, db_cursor, schema_name, "RECORD_COUNTS",condition, val)
    pmu.close(db_connector, db_cursor)

# COMMAND ----------

# Function used to insert the data into incr_metadata data table
def insert_incr_metadata(values):
  db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
  pmu.insert(db_connector, db_cursor, schema_name,incr_column_meta_table, "(TABLE_NAME, SOURCE_COLUMN_NAME, DATA_TYPE,DESTINATION_COLUMN_NAME,PROCESSED_COLUMN_NAME)", values)
  pmu.close(db_connector, db_cursor)

# COMMAND ----------

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

# reading the SAP data and other sources data seperately and replacing the RSART column with "<DEFAULT>" string if it is  null since it's one of the PK
if delimiter == 'u0001':
  landing_df = spark.read.format('parquet').load(landing_files)
  if table_name == "RESB":
    landing_df = landing_df.withColumn("RSART",when(((col("RSART").isNull()) | (trim(col("RSART")) == '')),"<DEFAULT>").otherwise(col("RSART")))
else:
  landing_df = spark.read.format('csv').options(header='true').option('delimiter', delimiter).load(landing_files)

# COMMAND ----------

# script used to handle dynamic columns in quality SFTP files.
column_df = column_metadata.select("SOURCE_COLUMN_NAME").collect()
table_columns = [row["SOURCE_COLUMN_NAME"] for row in column_df]
if len(landing_df.columns) - len(table_columns) >0: #checking of additional columns
  addl_cols_list = [i for i in landing_df.columns + table_columns if i not in landing_df.columns or i not in table_columns] #fetching additional columns
  if table_name in integer_columns_tables: # checking integer data type tables
    for each_addl_col in addl_cols_list:
      vals = "('" + table_name + "','" + each_addl_col + "','" + "INTEGER" + "','" + each_addl_col + "','" +"')"
      insert_incr_metadata(vals) # inserting column meta in incr_column_metadata table
      spark.sql("alter table {}.{} add columns({} {})".format(curated_db,table_name,each_addl_col,"INTEGER"))
  elif table_name in decimal_columns_tables: # checking decimal data type tables
    for each_addl_col in addl_cols_list:
      vals = "('" + table_name + "','" + each_addl_col + "','" + "DECIMAL(13,2)" + "','" + each_addl_col + "','" +"')"
      spark.sql("alter table {}.{} add columns({} {})".format(curated_db,table_name,each_addl_col,"DECIMAL(13,2)"))
      insert_incr_metadata(vals)

# COMMAND ----------

# Fetching the data from incr_column_metadata table
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
incr_column_metadata = pmu.get_data(cursor=db_cursor, col_lookup=incr_column_meta_table, value=table_name, column="TABLE_NAME",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# performing union operation on column and incr_column metadata dataframes
column_metadata = column_metadata.unionAll(incr_column_metadata)

# COMMAND ----------

# FIX ADDED FOR HANDLING BLANK VALUES #2
string_fields = []
for i, f in enumerate(landing_df.schema.fields):
    if isinstance(f.dataType, StringType):
        string_fields.append(f.name)
# print(string_fields)

# COMMAND ----------

# DBTITLE 1,FIX ADDED FOR HANDLING BLANK VALUES #3
# function call to remove the unwanted columns from landing layer dataframe
for each in string_fields:
  landing_df = landing_df.withColumn(each,blank_as_null(each))

# COMMAND ----------

# function call to convert name of the columns
landing_df = hil_convert_dataframe_columns(table_name=landing_df, column_metadata=column_metadata)

# COMMAND ----------

# Creating temptable out of dataframe
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

# function used to create merge query based on primary key columns
def create_merge_query(source_table, target_table, primary_keys):
    merge_condition = """"""
    primary_keys = primary_keys.split(',')
    for index, primary_key in enumerate(primary_keys):
        if index == len(primary_keys) - 1:
            merge_condition += """source.{id} == target.{id}""".format(source=source_table, target=target_table, id=primary_key)
        else:
            merge_condition += """source.{id} == target.{id} AND """.format(source=source_table, target=target_table, id=primary_key)
    merge_query = """
MERGE INTO {target} as target
USING {source} as source
ON {merge_condition}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *""".format(source=source_table,
                                         target=target_table,
                                         merge_condition=merge_condition)

    return merge_query

# COMMAND ----------

# Function used to create the merge query for BSID and BSAD table
def create_merge_query_nullable(source_table, target_table, primary_keys):
    merge_condition = """"""
    primary_keys = primary_keys.split(',')
    for index, primary_key in enumerate(primary_keys):
        if index == len(primary_keys) - 1:
            merge_condition += """source.{id} == target.{id}""".format(source=source_table, target=target_table, id=primary_key)
        elif index == len(primary_keys) - 1 and primary_key in ['UMSKS','UMSKZ','AUGDT','AUGBL']:
            merge_condition += """source.{id} == target.{id}""".format(source=source_table, target=target_table, id=primary_key)
        elif primary_key in ['UMSKS','UMSKZ','AUGDT','AUGBL'] :
            merge_condition += """((source.{id} == target.{id}) OR (source.{id} is null AND  target.{id} is null)) AND """.format(source=source_table, target=target_table, id=primary_key)
        else:
            merge_condition += """source.{id} == target.{id} AND """.format(source=source_table, target=target_table, id=primary_key)
    merge_query = """
MERGE INTO {target} as target
USING {source} as source
ON {merge_condition}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *""".format(source=source_table,
                                         target=target_table,
                                         merge_condition=merge_condition)

    return merge_query

# COMMAND ----------

# Fetching columns from the column_metadata
column_df = column_metadata.select("SOURCE_COLUMN_NAME").collect()
table_columns = [row["SOURCE_COLUMN_NAME"] for row in column_df]

# COMMAND ----------

# removing Z from table names
if table_name in ['ZKONV','ZBSEG','ZCDPOS']:
  table_name = table_name[1:]

# COMMAND ----------

#refreshing table
spark.sql("""fsck repair table {}.{}""".format(curated_db,table_name))
spark.sql("""refresh table {}.{}""".format(curated_db,table_name))

# COMMAND ----------

# Fetching the incremeental data
if table_name == "SIP_SALES":
  latest_inc_df = temp_df.withColumn("FILE_NAME", reverse(split(input_file_name(), "/"))[0])
  latest_inc_df = latest_inc_df.withColumn("LAST_UPDATED_DT_TS", current_timestamp().cast("string"))
# Eliminating null records  in cost tracker data set
elif table_name =='COST_TRACKER':
  latest_inc_df = temp_df.where("PARTICULARS is not NULL")
  latest_inc_df = latest_inc_df.withColumn("LAST_UPDATED_DT_TS", current_timestamp().cast("string"))
elif table_name == "SALES_TARGETS":
  latest_inc_df = temp_df.withColumn("LAST_UPDATED_DT_TS", current_timestamp().cast("string"))
  latest_inc_df = latest_inc_df.withColumn("FILE_DATE",to_timestamp(rpad(concat(split(reverse(split(input_file_name(), "/"))[0],"_")[2],split(split(reverse(split(input_file_name(), "/"))[0],"_")[3],".")[0]),14,'0'),"yyyyMMddHHmmss"))
elif table_name == "PLANT_DAILY_TARGETS":
  pd_df = temp_df.toPandas()
  df1 = (pd_df.set_index(["PLANT_ID","MATERIAL_ID","MONTH","YEAR"])
         .stack()
         .reset_index(name='VALUE')
         .rename(columns={'level_4':'DAY'}))
  df1['VALUE'] = df1['VALUE'].astype("int64")
  df1['DAY'] = df1['DAY'].astype("int64")
  df_sp =spark.createDataFrame(df1)
  latest_inc_df = df_sp.withColumn("target_date",to_date(unix_timestamp(concat(lpad(col("DAY"),2,'0'),lit("-"),lpad(col("MONTH"),2,'0'),lit("-"),col("YEAR")),'dd-MM-yyyy').cast('timestamp'))).withColumnRenamed("VALUE","PRODUCTION_PLAN").withColumnRenamed("TARGET_DATE","DATE").select("PLANT_ID","MATERIAL_ID","PRODUCTION_PLAN","DATE").where("DATE is not null")
  latest_inc_df = latest_inc_df.withColumn("LAST_UPDATED_DT_TS", current_timestamp().cast("string"))
else:
  latest_inc_df = temp_df.withColumn("LAST_UPDATED_DT_TS", current_timestamp().cast("string"))
latest_inc_df.registerTempTable("source_table")

# COMMAND ----------

# transformation logic for SIP_SALES Dataset
if table_name == "SIP_SALES":
  spark.sql("fsck repair table {}.{}".format(curated_db,table_name))
  spark.sql("refresh table {}.{}".format(curated_db,table_name))
  filename_df = spark.sql("select distinct(FILE_NAME) from {}.{}".format(curated_db,table_name))
  filname_list = filename_df.select("FILE_NAME").rdd.flatMap(lambda x: x).collect()
  for each in filname_list:
    latest_inc_df = latest_inc_df.filter(latest_inc_df["FILE_NAME"]!=each)
  latest_inc_df.registerTempTable("source_table")

# COMMAND ----------

# inserting incremental record count in RECORD_COUNTS table
try:
  inc_record_count("insert",table_name,last_updated_column,curated_db,db_url,user_name,password,db,schema_name)
except Exception as e:
  print(e)

# COMMAND ----------

# function call to create merge query
if table_name in ['BSID','BSAD']:
  merge_query = create_merge_query_nullable("source_table", "{}.{}".format(curated_db,table_name), primary_keys_list)
else:
  merge_query = create_merge_query("source_table", "{}.{}".format(curated_db,table_name), primary_keys_list)

# COMMAND ----------

print(merge_query)

# COMMAND ----------

# executing merge query
spark.sql(merge_query)

# COMMAND ----------

#refreshing table
spark.sql("""fsck repair table {}.{}""".format(curated_db,table_name))
spark.sql("""refresh table {}.{}""".format(curated_db,table_name))

# COMMAND ----------

# Updating the RECORD_COUNTS table
try:
  inc_record_count("update",table_name,last_updated_column,curated_db,db_url,user_name,password,db,schema_name)
except Exception as e:
  print(e)

# COMMAND ----------

# Exit command
dbutils.notebook.exit({'landing_file_names':landing_files})
