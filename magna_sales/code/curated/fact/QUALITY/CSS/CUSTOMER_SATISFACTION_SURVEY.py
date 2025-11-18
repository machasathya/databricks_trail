# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,max as _max
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility

# COMMAND ----------

# enabling cross join
spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

# parameters to fetch the data from ADF
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
table_name = "FACT_QUALITY_CUSTOMER_SATISFACTION_SURVEY"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variable to run the code manually
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
# table_name = "FACT_QUALITY_CUSTOMER_SATISFACTION_SURVEY"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# defining class object
pmu = ProcessMetadataUtility()

# COMMAND ----------

# fetching surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
parameter_df = spark.sql("select * from {}.{}".format(curated_db_name, "CSS_PARAMETER_DESCRIPTION")).drop("LAST_UPDATED_DT_TS")
sbu1_df = spark.sql("select * from {}.{}".format(curated_db_name, "CSS_SBU1")).drop("LAST_UPDATED_DT_TS")
sbu2_df = spark.sql("select * from {}.{}".format(curated_db_name, "CSS_SBU2")).drop("LAST_UPDATED_DT_TS")
sbu3_df = spark.sql("select * from {}.{}".format(curated_db_name, "CSS_SBU3")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# fetching all columns other than month columns
sbu1_col = [c for c in sbu1_df.columns if c not in {'DATE', 'SBU', 'PRODUCT_LINE','DEALER_NAME','SUB_DEALER_NAME','CUSTOMER_ID','SURVEY_TAKEN_BY','ZONE','STATE','REMARKS'}]
sbu2_col = [c for c in sbu2_df.columns if c not in {'DATE', 'SBU', 'PRODUCT_LINE','DEALER_NAME','SUB_DEALER_NAME','CUSTOMER_ID','SURVEY_TAKEN_BY','ZONE','STATE','REMARKS'}]
sbu3_col = [c for c in sbu3_df.columns if c not in {'DATE', 'SBU', 'PRODUCT_LINE','DEALER_NAME','SUB_DEALER_NAME','CUSTOMER_ID','SURVEY_TAKEN_BY','ZONE','STATE','REMARKS'}]

# COMMAND ----------

# handling empty datasets which doesn't have survery values for sbu1 dataset
if len(sbu1_col) ==0:
  sbu1 = sbu1_df.withColumn("QUESTION_ID",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  sbu1 = sbu1_df.selectExpr('DATE', 'SBU', 'PRODUCT_LINE','DEALER_NAME','SUB_DEALER_NAME','CUSTOMER_ID','SURVEY_TAKEN_BY','ZONE','STATE','REMARKS', "stack({}, {})".format(len(sbu1_col), ', '.join(("'{}', {}".format(i, i) for i in sbu1_col)))).withColumnRenamed("col0","QUESTION_ID").withColumnRenamed("col1","RATING")

# COMMAND ----------

# handling empty datasets which doesn't have survery values for sbu2 dataset
if len(sbu2_col) ==0:
  sbu2 = sbu2_df.withColumn("QUESTION_ID",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  sbu2 = sbu2_df.selectExpr('DATE', 'SBU', 'PRODUCT_LINE','DEALER_NAME','SUB_DEALER_NAME','CUSTOMER_ID','SURVEY_TAKEN_BY','ZONE','STATE','REMARKS', "stack({}, {})".format(len(sbu2_col), ','.join(("'{}', {}".format(i, i) for i in sbu2_col)))).withColumnRenamed("col0","QUESTION_ID").withColumnRenamed("col1","RATING")

# COMMAND ----------

# handling empty datasets which doesn't have survery values for sbu3 dataset
if len(sbu3_col) ==0:
  sbu3 = sbu3_df.withColumn("QUESTION_ID",lit('')).withColumn("RATING",lit('').cast("Integer"))
else:
  sbu3 = sbu3_df.selectExpr('DATE', 'SBU', 'PRODUCT_LINE','DEALER_NAME','SUB_DEALER_NAME','CUSTOMER_ID','SURVEY_TAKEN_BY','ZONE','STATE','REMARKS', "stack({}, {})".format(len(sbu3_col), ', '.join(("'{}', {}".format(i, i) for i in sbu3_col)))).withColumnRenamed("col0","QUESTION_ID").withColumnRenamed("col1","RATING")

# COMMAND ----------

# merging all SBU's data
sbu = sbu1.union(sbu2).union(sbu3)

# COMMAND ----------

# adding category and question description
final_df = sbu.alias('df').join(parameter_df.alias('pa'),on=[col('df.QUESTION_ID') == col('pa.QUESTION_ID')],how = 'inner').select(["df." + cols for cols in sbu.columns] + ["pa.CATEGORY"] + ["pa.QUESTION"]).drop(col("QUESTION_ID"))

# adding trailing 0's to customer id 
final_df=final_df.withColumn("CUSTOMER_ID",lpad(col("CUSTOMER_ID"),10,'0'))

# COMMAND ----------

# writing data to processed layer data before generating surrogate keys
final_df.write.parquet(processed_location+'CUSTOMER_SATISFACTION_SURVEY', mode='overwrite')

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
                        " THEN -2 ELSE ifnull({dim_table}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} on {fact_table}.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)    
  select_condition = select_condition[:-2]      
  query = """select
  
SBU,
DEALER_NAME,
SUB_DEALER_NAME,
SURVEY_TAKEN_BY,
ZONE,
STATE,
REMARKS,
RATING,
CATEGORY,
QUESTION,
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
d = surrogate_mapping_hil(csv_data,table_name,"CUSTOMER_SATISFACTION_SURVEY",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+"FACT_QUALITY_CUSTOMER_SATISFACTION_SURVEY")

# COMMAND ----------

# inserting log record to last execution details in SQL
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
