# Databricks notebook source
# import statements
import time
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType
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
table_name = "FACT_QUALITY_FIELD_COMPETITOR_SURVEY"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the code manually
# spark.conf.set("spark.sql.crossJoin.enabled", "true")
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
# table_name = "FACT_QUALITY_FIELD_COMPETITOR_SURVEY"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# fetching surrogate meta from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
ac_sheets_df = spark.sql("select * from {}.{}".format(curated_db_name, "FIELD_COMPETITOR_SURVEY_AC_SHEETS")).drop("LAST_UPDATED_DT_TS")
cc_sheets_df = spark.sql("select * from {}.{}".format(curated_db_name, "FIELD_COMPETITOR_SURVEY_CC_SHEETS")).drop("LAST_UPDATED_DT_TS")
non_ac_sheets_df = spark.sql("select * from {}.{}".format(curated_db_name, "FIELD_COMPETITOR_SURVEY_NON_AC_SHEETS")).drop("LAST_UPDATED_DT_TS")
tile_adhesive_df = spark.sql("select * from {}.{}".format(curated_db_name, "FIELD_COMPETITOR_SURVEY_TILE_ADHESIVE")).drop("LAST_UPDATED_DT_TS")
smart_plaster_df = spark.sql("select * from {}.{}".format(curated_db_name, "FIELD_COMPETITOR_SURVEY_SMART_PLASTER")).drop("LAST_UPDATED_DT_TS")
smart_fix_df = spark.sql("select * from {}.{}".format(curated_db_name, "FIELD_COMPETITOR_SURVEY_SMART_FIX")).drop("LAST_UPDATED_DT_TS")
panels_df = spark.sql("select * from {}.{}".format(curated_db_name, "FIELD_COMPETITOR_SURVEY_PANELS")).drop("LAST_UPDATED_DT_TS")
boards_df = spark.sql("select * from {}.{}".format(curated_db_name, "FIELD_COMPETITOR_SURVEY_BOARDS")).drop("LAST_UPDATED_DT_TS")
blocks_df = spark.sql("select * from {}.{}".format(curated_db_name, "FIELD_COMPETITOR_SURVEY_BLOCKS")).drop("LAST_UPDATED_DT_TS")
pipes_fittings_df = spark.sql("select * from {}.{}".format(curated_db_name, "FIELD_COMPETITOR_SURVEY_PIPES_FITTINGS")).drop("LAST_UPDATED_DT_TS")
wall_putty_df = spark.sql("select * from {}.{}".format(curated_db_name, "FIELD_COMPETITOR_SURVEY_WALL_PUTTY")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# extracting column names other than months
ac_sheets_col = [c for c in ac_sheets_df.columns if c not in {'DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS'}]
cc_sheets_col = [c for c in cc_sheets_df.columns if c not in {'DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS'}]
non_ac_sheets_col = [c for c in non_ac_sheets_df.columns if c not in {'DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS'}]
tile_adhesive_col = [c for c in tile_adhesive_df.columns if c not in {'DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS'}]
smart_plaster_col = [c for c in smart_plaster_df.columns if c not in {'DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS'}]
smart_fix_col = [c for c in smart_fix_df.columns if c not in {'DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS'}]
panels_col = [c for c in panels_df.columns if c not in {'DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS'}]
boards_col = [c for c in boards_df.columns if c not in {'DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS'}]
blocks_col = [c for c in blocks_df.columns if c not in {'DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS'}]
pipes_fittings_col = [c for c in pipes_fittings_df.columns if c not in {'DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS'}]
wall_putty_col = [c for c in wall_putty_df.columns if c not in {'DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS'}]

# COMMAND ----------

# handling tables which doesn't have survey data
if len(ac_sheets_col) == 0:
  ac_sheets = ac_sheets_df.withColumn("COMPANY",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  ac_sheets = ac_sheets_df.selectExpr('DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS', "stack({}, {})".format(len(ac_sheets_col), ', '.join(("'{}', {}".format(i, i) for i in ac_sheets_col)))).withColumnRenamed("col0","COMPANY").withColumnRenamed("col1","RATING")

# COMMAND ----------

# handling tables which doesn't have survey data
if len(cc_sheets_col) == 0:
  cc_sheets = ccc_sheets_df.withColumn("COMPANY",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  cc_sheets = cc_sheets_df.selectExpr('DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS', "stack({}, {})".format(len(cc_sheets_col), ', '.join(("'{}', {}".format(i, i) for i in cc_sheets_col)))).withColumnRenamed("col0","COMPANY").withColumnRenamed("col1","RATING")

# COMMAND ----------

# handling tables which doesn't have survey data
if len(non_ac_sheets_col) == 0:
  non_ac_sheets = non_ac_sheets_df.withColumn("COMPANY",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  non_ac_sheets = non_ac_sheets_df.selectExpr('DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS', "stack({}, {})".format(len(non_ac_sheets_col), ', '.join(("'{}', {}".format(i, i) for i in non_ac_sheets_col)))).withColumnRenamed("col0","COMPANY").withColumnRenamed("col1","RATING")

# COMMAND ----------

# handling tables which doesn't have survey data
if len(tile_adhesive_col) == 0:
  tile_adhesive = tile_adhesive_df.withColumn("COMPANY",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  tile_adhesive = tile_adhesive_df.selectExpr('DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS', "stack({}, {})".format(len(tile_adhesive_col), ', '.join(("'{}', {}".format(i, i) for i in tile_adhesive_col)))).withColumnRenamed("col0","COMPANY").withColumnRenamed("col1","RATING")

# COMMAND ----------

# handling tables which doesn't have survey data
if len(smart_plaster_col) == 0:
  smart_plaster = smart_plaster_df.withColumn("COMPANY",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  smart_plaster = smart_plaster_df.selectExpr('DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS', "stack({}, {})".format(len(smart_plaster_col), ', '.join(("'{}', {}".format(i, i) for i in smart_plaster_col)))).withColumnRenamed("col0","COMPANY").withColumnRenamed("col1","RATING")

# COMMAND ----------

# handling tables which doesn't have survey data
if len(smart_fix_col) == 0:
  smart_fix = smart_fix_df.withColumn("COMPANY",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  smart_fix = smart_fix_df.selectExpr('DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS', "stack({}, {})".format(len(smart_fix_col), ', '.join(("'{}', {}".format(i, i) for i in smart_fix_col)))).withColumnRenamed("col0","COMPANY").withColumnRenamed("col1","RATING")

# COMMAND ----------

# handling tables which doesn't have survey data
if len(panels_col) == 0:
  panels = panels_df.withColumn("COMPANY",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  panels = panels_df.selectExpr('DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS', "stack({}, {})".format(len(panels_col), ', '.join(("'{}', {}".format(i, i) for i in panels_col)))).withColumnRenamed("col0","COMPANY").withColumnRenamed("col1","RATING")

# COMMAND ----------

# handling tables which doesn't have survey data
if len(boards_col) == 0:
  boards = boards_df.withColumn("COMPANY",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  boards = boards_df.selectExpr('DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS', "stack({}, {})".format(len(boards_col), ', '.join(("'{}', {}".format(i, i) for i in boards_col)))).withColumnRenamed("col0","COMPANY").withColumnRenamed("col1","RATING")

# COMMAND ----------

# handling tables which doesn't have survey data
if len(blocks_col) == 0:
  blocks = blocks_df.withColumn("COMPANY",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  blocks = blocks_df.selectExpr('DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS', "stack({}, {})".format(len(blocks_col), ', '.join(("'{}', {}".format(i, i) for i in blocks_col)))).withColumnRenamed("col0","COMPANY").withColumnRenamed("col1","RATING")

# COMMAND ----------

# handling tables which doesn't have survey data
if len(pipes_fittings_col) == 0:
  pipes_fittings = pipes_fittings_df.withColumn("COMPANY",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  pipes_fittings = pipes_fittings_df.selectExpr('DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS', "stack({}, {})".format(len(pipes_fittings_col), ', '.join(("'{}', {}".format(i, i) for i in pipes_fittings_col)))).withColumnRenamed("col0","COMPANY").withColumnRenamed("col1","RATING")

# COMMAND ----------

# handling tables which doesn't have survey data
if len(wall_putty_col) == 0:
  wall_putty = wall_putty_df.withColumn("COMPANY",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  wall_putty = wall_putty_df.selectExpr('DATE', 'SBU', 'ZONE','CONFORMANCE','PARAMETER','PARAMETER_DESCRIPTION','PRODUCT_LINE','REMARKS', "stack({}, {})".format(len(wall_putty_col), ', '.join(("'{}', {}".format(i, i) for i in wall_putty_col)))).withColumnRenamed("col0","COMPANY").withColumnRenamed("col1","RATING")

# COMMAND ----------

# merging all dataframes into one dataframe
final_result = ac_sheets.union(cc_sheets).union(non_ac_sheets).union(tile_adhesive).union(smart_plaster).union(smart_fix).union(panels).union(boards).union(blocks).union(pipes_fittings).union(wall_putty)

# COMMAND ----------

# writing data to processed location before generating surrogate keys
final_result.write.parquet(processed_location+'FIELD_COMPETITOR_SURVEY', mode='overwrite')

# COMMAND ----------

#--------------------SURROGATE IMPLEMENTATION------------------------

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
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
  query = """select SBU,
ZONE,
CONFORMANCE,
PARAMETER,
PARAMETER_DESCRIPTION,
PRODUCT_LINE,
REMARKS,
Company,
Rating,{select_condition}  from {fact_name} {join_condition}""".format(
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
d = surrogate_mapping_hil(csv_data, table_name,"FIELD_COMPETITOR_SURVEY", processed_db_name)

# COMMAND ----------

# writing data to processed layer location after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+"FACT_QUALITY_FIELD_COMPETITOR_SURVEY")

# COMMAND ----------

# insert log data to last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
