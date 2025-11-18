# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,upper,concat, to_date, date_format
from pyspark.sql import functions as sf
from pyspark.sql.types import DateType,IntegerType,DecimalType
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
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
table_name = "FACT_PUTTY_MIS_KPI_TARGETS"
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
# table_name = "FACT_PUTTY_MIS_KPI_TARGETS"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
df=spark.sql("select * from {}.{}".format(curated_db_name, "PUTTY_MIS_KPI_TARGETS")).drop(*["LAST_UPDATED_DT_TS","PLANT"])

# COMMAND ----------

# Summing up the records for each month and filtering the months that doesn't have values

sum1_df = df.agg(sf.sum('APRIL').alias('APRIL'),sf.sum('MAY').alias('MAY'),sf.sum('JUNE').alias('JUNE'),sf.sum('JULY').alias('JULY'),sf.sum('AUGUST').alias('AUGUST'),sf.sum('SEPTEMBER').alias('SEPTEMBER'),sf.sum('OCTOBER').alias('OCTOBER'),sf.sum('NOVEMBER').alias('NOVEMBER'),sf.sum('DECEMBER').alias('DECEMBER'),sf.sum('JANUARY').alias('JANUARY'),sf.sum('FEBRUARY').alias('FEBRUARY'),sf.sum('MARCH').alias('MARCH'))

# Collecting all the months values with non-zero values

num1_df = sum1_df.withColumn("MONTH",when(col('MARCH').isNotNull(),lit(16)).when(col('FEBRUARY').isNotNull(),lit(15)).when(col('JANUARY').isNotNull(),lit(14)).when(col('DECEMBER').isNotNull(),lit(13)).when(col('NOVEMBER').isNotNull(),lit(12)).when(col('OCTOBER').isNotNull(),lit(11)).when(col('SEPTEMBER').isNotNull(),lit(10)).when(col('AUGUST').isNotNull(),lit(9)).when(col('JULY').isNotNull(),lit(8)).when(col('JUNE').isNotNull(),lit(7)).when(col('MAY').isNotNull(),lit(6)).when(col('APRIL').isNotNull(),lit(5)))
rm_col = num1_df.select('MONTH').collect()[0][0]

trans_df = df.select(df.columns[:rm_col])

# COMMAND ----------

# selecting columns other than months
pivot_col = [c for c in trans_df.columns if c not in {"PLANT_ID","PARTICULARS","UOM","FINANCIAL_YEAR"}]

# COMMAND ----------

# pivot the data based on month
pivot_df = trans_df.selectExpr("PLANT_ID","PARTICULARS","UOM","FINANCIAL_YEAR", "stack({}, {})".format(len(pivot_col), ', '.join(("'{}', {}".format(i, i) for i in pivot_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","TARGET_VALUE")

# COMMAND ----------

# changing FY and month to calendar year and adding start date of the month

df2 = pivot_df.withColumn('MONTH_NUMBER', sf.date_format(to_date(col('MONTH'), 'MMMMM'), 'MM'))
df_yr = df2.withColumn('YEAR',when((col("MONTH_NUMBER")  == '01') | (col("MONTH_NUMBER")  == '02') |(col("MONTH_NUMBER")  == '03'),df2["FINANCIAL_YEAR"]).otherwise((df2["FINANCIAL_YEAR"].cast(IntegerType())) - 1))
df3 = df_yr.withColumn('DATE_STR',sf.concat(sf.col('YEAR'),sf.lit('-'), sf.col('MONTH_NUMBER'),sf.lit('-01')))
final_df = df3.withColumn("DATE",df3['DATE_STR'].cast(DateType())).drop('FINANCIAL_YEAR').drop('LAST_UPDATED_DT_TS').drop('MONTH').drop('MONTH_NUMBER').drop('YEAR').drop('DATE_STR')
# display(final_df)

# COMMAND ----------

# replacing spl characters with NULL
df_trans = final_df.withColumn("PARTICULARS", sf.regexp_replace(col("PARTICULARS"), "[^A-Z a-z 0-9 % _]", "")).withColumn("UOM", sf.regexp_replace(col("UOM"), "[^A-Z a-z 0-9 / % _]", ""))
df_trans_2 = df_trans.withColumn("PARTICULARS", upper(col('PARTICULARS'))).withColumn("UOM", upper(col('UOM')))
df_trans_3 = df_trans_2.withColumn("PARTICULARS", sf.regexp_replace(col("PARTICULARS"), " ", "_")).withColumn("UOM", sf.regexp_replace(col("UOM"), " ", "_"))
df_trans_4 = df_trans_3.withColumn("PARTICULARS", sf.regexp_replace(col("PARTICULARS"), "%", "PERCEN")).withColumn("UOM", sf.regexp_replace(col("UOM"), "%", "PERCEN"))
df_trans_5 = df_trans_4.withColumn("PARTICULARS", sf.regexp_replace(col("PARTICULARS"), "__", "_")).withColumn("UOM", sf.regexp_replace(col("UOM"), "__", "_"))
# display(df_trans_5)

# COMMAND ----------

# adding particulars new column
df_trans_6 = df_trans_5.withColumn("PARTICULARS_NEW", sf.concat(sf.col('PARTICULARS'),sf.lit('_'), sf.col('UOM')))

# COMMAND ----------

# pivoting data
PARTICULARS_LIST=["LABOUR_COST_INR_PER_MT","OTHER_FIXED_COST_INR_PER_MT","POWER_FUEL_INR_PER_MT","PRE_PACKING_COST_INR_PER_MT","PRODUCTION_MT","PRODUCT_COST_INR_PER_MT","SALES_MT","SECONDARY_PACKING_COST_INR_PER_MT","STORES_SPARES_INR_PER_MT"]
pivot_df = df_trans_6.groupby('PLANT_ID','DATE') \
                        .pivot('PARTICULARS_NEW',PARTICULARS_LIST) \
                        .max('TARGET_VALUE')

# COMMAND ----------

# adding month column
data_final = pivot_df.withColumn('MONTH_OF_YEAR', concat(date_format(to_date(col('DATE'), '%Y-%m-%d'), 'MMMMM'), lit(" "), date_format(to_date(col('DATE'), '%Y-%m-%d'), 'Y')).cast('string'))

# COMMAND ----------

# writing data to processed location before generating surrogate keys
data_final.write.parquet(processed_location+'PUTTY_MIS_KPI_TARGETS', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
  data_final.createOrReplaceTempView("{}".format(fact_name))
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
  MONTH_OF_YEAR,
LABOUR_COST_INR_PER_MT,
OTHER_FIXED_COST_INR_PER_MT,
POWER_FUEL_INR_PER_MT,
PRE_PACKING_COST_INR_PER_MT,
PRODUCTION_MT,
PRODUCT_COST_INR_PER_MT,
SALES_MT,
SECONDARY_PACKING_COST_INR_PER_MT,
STORES_SPARES_INR_PER_MT
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
d = surrogate_mapping_hil(csv_data,table_name,"PUTTY_MIS_KPI_TARGETS",processed_db_name)

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
