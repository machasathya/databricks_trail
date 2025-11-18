# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,upper,concat,trim,count,sum
from pyspark.sql import functions as sf
from pyspark.sql import types as T
from ProcessMetadataUtility import ProcessMetadataUtility
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
table_name = "FACT_MATERIAL_MASTER_LOG"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the notebook manualy
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
# table_name = "FACT_MATERIAL_MASTER_LOG"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading change header and item tables data from curated layer
cdhdr = spark.sql("Select * from {}.{} WHERE OBJECTCLAS LIKE '%MATERIAL%'".format(curated_db_name,"CDHDR"))

cdpos = spark.sql("Select OBJECTCLAS,OBJECTID,CHANGENR,TABNAME,FNAME,VALUE_OLD,VALUE_NEW,UDATE,ERDAT from {}.{}".format(curated_db_name,"CDPOS"))

# COMMAND ----------

# defining count of changes and seperating active inactive changes and block changes
final_df = cdhdr.alias("hdr").join(cdpos.alias("pos"), on =[trim(col("hdr.OBJECTCLAS")) == trim(col("pos.OBJECTCLAS")),
                                                            trim(col("hdr.OBJECTID")) == trim(col("pos.OBJECTID")),
trim(col("hdr.CHANGENR"))==trim(col("pos.CHANGENR"))],how='inner').select(["pos." + cols for cols in cdpos.columns] + [col("hdr.UDATE").alias("CHANGED_DATE")]).where("tabname like '%MARA'").distinct()

nrml_changes = final_df.where("fname not in ('LVORM','MSTAE')").groupBy("CHANGED_DATE").agg(count("*").alias("CHANGES_COUNT"))

active_changes = final_df.where("fname == 'LVORM'")

blckng_changes = final_df.where("fname == 'MSTAE'")

# COMMAND ----------

# defining unblock and active inactive changes count
temp1 = blckng_changes.withColumn("UNBLOCK_CHANGES",when(((trim(col("VALUE_OLD")) == '01') & (trim(col("VALUE_NEW")).isNull())),lit(1)).otherwise(lit(0))).withColumn("BLOCK_CHANGES",when(((trim(col("VALUE_OLD")).isNull()) & (trim(col("VALUE_NEW")) == '01')),lit(1)).otherwise(lit(0)))

temp2 = active_changes.withColumn("ACTIVE_CHANGES",when(((trim(col("VALUE_OLD")) == 'X') & (trim(col("VALUE_NEW")).isNull())),lit(1)).otherwise(lit(0))).withColumn("INACTIVE_CHANGES",when(((trim(col("VALUE_OLD")).isNull()) & (trim(col("VALUE_NEW")) == 'X')),lit(1)).otherwise(lit(0)))

temp1 = temp1.groupBy("CHANGED_DATE").agg(sum("UNBLOCK_CHANGES").alias("UNBLOCK_CHANGES"),sum("BLOCK_CHANGES").alias("BLOCK_CHANGES"))

temp2 = temp2.groupBy("CHANGED_DATE").agg(sum("INACTIVE_CHANGES").alias("INACTIVE_CHANGES"),sum("ACTIVE_CHANGES").alias("ACTIVE_CHANGES"))

# COMMAND ----------

# creating final dataframe
final_df1 = nrml_changes.alias("l_df").join(temp1.alias("r_df"), on = [col("l_df.CHANGED_DATE") == col("r_df.CHANGED_DATE")],how='left').select([col("l_df.CHANGED_DATE"),col("l_df.CHANGES_COUNT"),col("r_df.UNBLOCK_CHANGES"),col("r_df.BLOCK_CHANGES")])

final_df = final_df1.alias("l_df").join(temp2.alias("r_df"), on = [col("l_df.CHANGED_DATE") == col("r_df.CHANGED_DATE")],how='left').select([col("l_df." + cols) for cols in final_df1.columns]+ [col("r_df.INACTIVE_CHANGES"),col("r_df.ACTIVE_CHANGES")])

# COMMAND ----------

# writing data to processed layer before generating surrogate key
final_df.write.mode('overwrite').parquet(processed_location+"MATERIAL_MASTER_LOG")

# COMMAND ----------

#SURROGATE IMPLEMENTATION

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
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
   CHANGES_COUNT,
   UNBLOCK_CHANGES,
   BLOCK_CHANGES,
   INACTIVE_CHANGES,
   ACTIVE_CHANGES,
   {select_condition}  from {fact_name} {join_condition}
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
d = surrogate_mapping_hil(csv_data,table_name,"MATERIAL_MASTER_LOG",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# inserting log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
