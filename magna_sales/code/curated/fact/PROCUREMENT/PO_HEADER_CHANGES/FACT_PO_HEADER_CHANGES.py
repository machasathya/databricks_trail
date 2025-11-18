# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,max as _max,substring
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

# enabling cross join
spark.conf.set("spark.sql.crossJoin.enabled", "true")

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
table_name = "FACT_PO_HEADER_CHANGES"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

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
# table_name = "FACT_PO_HEADER_CHANGES"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# class object definition
pmu = ProcessMetadataUtility()

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
cdhdr = spark.sql("select OBJECTCLAS,OBJECTID,CHANGENR,USERNAME,UDATE from {}.{} where OBJECTCLAS='EINKBELEG'".format(curated_db_name,"CDHDR"))
cdpos = spark.sql("Select OBJECTCLAS,OBJECTID,CHANGENR,TABNAME,TABKEY,FNAME,CHNGIND,CUKY_OLD,CUKY_NEW,VALUE_NEW,VALUE_OLD from {}.{} where OBJECTCLAS = 'EINKBELEG' AND TABNAME='EKPO' AND FNAME IN ('MENGE','NETPR') AND CHNGIND = 'U'".format(curated_db_name,"CDPOS"))

ekko = spark.sql("select * from {}.{}".format(curated_db_name,"EKKO"))

T161T = spark.sql("select * from {}.{}".format(curated_db_name,"T161T")).where("SPRAS = 'E'")

# COMMAND ----------

# adding po type and purchase org columns
df1 = cdhdr.alias("df1").join(ekko.alias("df2"),on=[col('df1.objectid') == col("df2.ebeln")],how='left').select([col("" + cols) for cols in cdhdr.columns]+[col("df2.BSART"),col("df2.EKORG")])

# COMMAND ----------

# defining purchase doc number and item
df2 = cdpos.withColumn("PURCHASE_DOC_NUMBER",cdpos["TABKEY"].substr(lit(4),lit(10))).withColumn("ITEM",cdpos["TABKEY"].substr(lit(14),lit(5))).drop("TABKEY")

# COMMAND ----------

# joining header and item table and selecting required columns
df3 = df1.alias("hdr").join(df2.alias("pos"), on =[col("hdr.OBJECTCLAS") == col("pos.OBJECTCLAS"),
                                                            col("hdr.OBJECTID") == col("pos.OBJECTID"),
col("hdr.CHANGENR")==col("pos.CHANGENR")],how='inner').select([col("pos.PURCHASE_DOC_NUMBER"),col("pos.ITEM"),col("hdr.BSART"),col("hdr.EKORG").alias("PLANT_ID"),col("hdr.UDATE").alias("CHANGED_DATE"),col("pos.FNAME").alias("COLUMN_NAME"),col("pos.VALUE_NEW"),col("pos.VALUE_OLD")])

# COMMAND ----------

# adding po doc type
final_df = df3.alias("df1").join(T161T.alias("df2"),on=[col("df1.BSART") == col("df2.BSART")],how='left').select([col("df1." + cols) for cols in df3.columns]+[col("df2.BATXT").alias("DOC_TYPE")]).drop("BSART")

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'PO_HEADER_CHANGES', mode='overwrite')

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
PURCHASE_DOC_NUMBER,
ITEM,
COLUMN_NAME,
VALUE_NEW,
VALUE_OLD,
DOC_TYPE,
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
d = surrogate_mapping_hil(csv_data,table_name,"PO_HEADER_CHANGES",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surroagte keys
d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# insert log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
