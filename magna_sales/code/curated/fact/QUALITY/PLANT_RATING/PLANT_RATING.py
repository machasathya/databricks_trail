# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,max as _max, to_date
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType,IntegerType

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
table_name = "FACT_PLANT_QUALITY_RATING"
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
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_PLANT_QUALITY_RATING"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# class definition
pmu = ProcessMetadataUtility()

# COMMAND ----------

# fetching the surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
df_sbu1 = spark.sql("select * from {}.{}".format(curated_db_name, "PLANT_RATING_SBU1")).drop("LAST_UPDATED_DT_TS")
df_sbu2 = spark.sql("select * from {}.{}".format(curated_db_name, "PLANT_RATING_SBU2")).drop("LAST_UPDATED_DT_TS")
df_sbu3 = spark.sql("select * from {}.{}".format(curated_db_name, "PLANT_RATING_SBU3")).drop("LAST_UPDATED_DT_TS")
target1_df =  spark.sql("SELECT * from {}.{}".format(curated_db_name,"PLANT_RATING_TARGETS_SBU1"))
target2_df =  spark.sql("SELECT * from {}.{}".format(curated_db_name,"PLANT_RATING_TARGETS_SBU2"))
target3_df =  spark.sql("SELECT * from {}.{}".format(curated_db_name,"PLANT_RATING_TARGETS_SBU3"))

# COMMAND ----------

# pivot month cols data to rows
month_col = ['APRIL','MAY','JUNE','JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER','JANUARY','FEBRUARY','MARCH']
pivot_df1 = target1_df.selectExpr("SBU","PRODUCT_LINE","GROUP_NAME","PARAMETER","FINANCIAL_YEAR","RATING_TARGET","LAST_UPDATED_DT_TS", "stack({}, {})".format(len(month_col), ', '.join(("'{}', {}".format(i, i) for i in month_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","VALUE_TARGET")
pivot_df2 = target2_df.selectExpr("SBU","PRODUCT_LINE","GROUP_NAME","PARAMETER","FINANCIAL_YEAR","RATING_TARGET","LAST_UPDATED_DT_TS", "stack({}, {})".format(len(month_col), ', '.join(("'{}', {}".format(i, i) for i in month_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","VALUE_TARGET")
pivot_df3 = target3_df.selectExpr("SBU","PRODUCT_LINE","GROUP_NAME","PARAMETER","FINANCIAL_YEAR","RATING_TARGET","LAST_UPDATED_DT_TS", "stack({}, {})".format(len(month_col), ', '.join(("'{}', {}".format(i, i) for i in month_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","VALUE_TARGET")

# COMMAND ----------

# merging all dataframes to one
pivot_df = pivot_df1.union(pivot_df2).union(pivot_df3)

# COMMAND ----------

# converting FY and month to calendar year
final_targets = pivot_df.withColumn('MONTH_NUMBER', sf.date_format(to_date(col('MONTH'), 'MMMMM'), 'MM')).withColumn('YEAR',((pivot_df["FINANCIAL_YEAR"].cast(IntegerType())) - 1))

# COMMAND ----------

# defining SBU Column
sbu1_df = df_sbu1.withColumn("SBU", lit('SBU 1'))
sbu2_df = df_sbu2.withColumn("SBU", lit('SBU 2'))
sbu3_df = df_sbu3.withColumn("SBU", lit('SBU 3'))

# COMMAND ----------

# seggregating rating and values columns seperately
sbu1_col = [s for s in sbu1_df.columns if '_RATING' in s]+[s for s in sbu1_df.columns if '_VALUE' in s]
sbu2_col = [s for s in sbu2_df.columns if '_RATING' in s]+[s for s in sbu2_df.columns if '_VALUE' in s]
sbu3_col = [s for s in sbu3_df.columns if '_RATING' in s]+[s for s in sbu3_df.columns if '_VALUE' in s]

# COMMAND ----------

# handling data which doesn't contain rating values 
if len(sbu1_col) == 0:
  sbu1 = sbu1_df.withColumn("col0",lit('')).withColumn("col1",lit('').cast("decimal(13,2)"))
else:
  sbu1 = sbu1_df.selectExpr('DATE','PLANT_ID','PRODUCT_LINE','SBU', "stack({}, {})".format(len(sbu1_col), ', '.join(("'{}', {}".format(i, i) for i in sbu1_col))))

# COMMAND ----------

# handling data which doesn't contain rating values 
if len(sbu2_col) == 0:
  sbu2 = sbu2_df.withColumn("col0",lit('')).withColumn("col1",lit('').cast("decimal(13,2)"))
else:
  sbu2 = sbu2_df.selectExpr('DATE','PLANT_ID','PRODUCT_LINE','SBU', "stack({}, {})".format(len(sbu2_col), ', '.join(("'{}', {}".format(i, i) for i in sbu2_col))))

# COMMAND ----------

# handling data which doesn't contain rating values 
if len(sbu3_col) == 0:
  sbu3 = sbu3_df.withColumn("col0",lit('')).withColumn("col1",lit('').cast("decimal(13,2)"))
else:
  sbu3 = sbu3_df.selectExpr('DATE','PLANT_ID','PRODUCT_LINE','SBU', "stack({}, {})".format(len(sbu3_col), ', '.join(("'{}', {}".format(i, i) for i in sbu3_col))))
sbu = sbu1.union(sbu2).union(sbu3)

# COMMAND ----------

# defining quality parameter and removing rating and value string from column name
sbu_rating = sbu.withColumn("QUALITY_PARAMETER", sf.regexp_replace(col("col0"), "_RATING", "")).drop("col0").where(sbu1.col0.contains('_RATING'))
sbu_value = sbu.withColumn("QUALITY_PARAMETER", sf.regexp_replace(col("col0"), "_VALUE", "")).drop("col0").where(sbu1.col0.contains('_VALUE'))

# COMMAND ----------

#defining rating and actual value
quality_df = sbu_rating.alias('ra').join(sbu_value.alias('va'),on = [
  col('ra.DATE') == col("va.DATE"),col('ra.PLANT_ID') == col("va.PLANT_ID"),col('ra.PRODUCT_LINE') == col("va.PRODUCT_LINE"),col('ra.SBU') == col("va.SBU"),col('ra.QUALITY_PARAMETER') == col("va.QUALITY_PARAMETER")], how = 'left').select(["ra.SBU"] + ["ra.DATE"] + ["ra.PLANT_ID"] + ["ra.PRODUCT_LINE"] + ["ra.QUALITY_PARAMETER"] + [col("ra.col1").alias("RATING_ACTUAL_VALUE")] + [col("va.col1").alias("ACTUAL_VALUE")])
qu_df = quality_df.alias('qu').select(["qu." + cols for cols in quality_df.columns] + [sf.month(quality_df.DATE).alias('MONTH')])

# COMMAND ----------

# defining target value
target_quality_df = qu_df.alias('qu').join(final_targets.alias('ta'),on=[col('qu.SBU') == col('ta.SBU'),col('qu.PRODUCT_LINE') == col('ta.PRODUCT_LINE'),col('qu.QUALITY_PARAMETER') == col('ta.PARAMETER'),col('qu.MONTH') == col('ta.MONTH_NUMBER')],how='left').select(["qu." + cols for cols in quality_df.columns] + ["ta.GROUP_NAME"] + [col("ta.RATING_TARGET").alias("RATING_TARGET_VALUE")] + [col("ta.VALUE_TARGET")])

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
target_quality_df.write.parquet(processed_location+'PLANT_QUALITY_RATING', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
  target_quality_df.createOrReplaceTempView("{}".format(fact_name))
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
  GROUP_NAME,
QUALITY_PARAMETER,
RATING_ACTUAL_VALUE,
RATING_TARGET_VALUE,
ACTUAL_VALUE,
VALUE_TARGET
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
d = surrogate_mapping_hil(csv_data,table_name,"PLANT_QUALITY_RATING",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+"FACT_PLANT_QUALITY_RATING")

# COMMAND ----------

# insert log data into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
