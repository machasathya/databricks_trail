# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,upper,concat, to_date
from pyspark.sql import functions as sf
from pyspark.sql.types import DateType,IntegerType,DecimalType
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
pmu = ProcessMetadataUtility()

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
table_name = "FACT_QUALITY_COPQ"
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
# table_name = "FACT_QUALITY_COPQ"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
copq_sbu1 = spark.sql("""select * from {}.{} """.format(curated_db_name, "COPQ_SBU1"))
copq_sbu2 = spark.sql("""select * from {}.{} """.format(curated_db_name, "COPQ_SBU2"))
copq_sbu3 = spark.sql("""select * from {}.{} """.format(curated_db_name, "COPQ_SBU3"))

# COMMAND ----------

#adding column values
sum1_df = copq_sbu1.agg(sf.sum('APRIL').alias('APRIL'),sf.sum('MAY').alias('MAY'),sf.sum('JUNE').alias('JUNE'),sf.sum('JULY').alias('JULY'),sf.sum('AUGUST').alias('AUGUST'),sf.sum('SEPTEMBER').alias('SEPTEMBER'),sf.sum('OCTOBER').alias('OCTOBER'),sf.sum('NOVEMBER').alias('NOVEMBER'),sf.sum('DECEMBER').alias('DECEMBER'),sf.sum('JANUARY').alias('JANUARY'),sf.sum('FEBRUARY').alias('FEBRUARY'),sf.sum('MARCH').alias('MARCH'))

sum2_df = copq_sbu2.agg(sf.sum('APRIL').alias('APRIL'),sf.sum('MAY').alias('MAY'),sf.sum('JUNE').alias('JUNE'),sf.sum('JULY').alias('JULY'),sf.sum('AUGUST').alias('AUGUST'),sf.sum('SEPTEMBER').alias('SEPTEMBER'),sf.sum('OCTOBER').alias('OCTOBER'),sf.sum('NOVEMBER').alias('NOVEMBER'),sf.sum('DECEMBER').alias('DECEMBER'),sf.sum('JANUARY').alias('JANUARY'),sf.sum('FEBRUARY').alias('FEBRUARY'),sf.sum('MARCH').alias('MARCH'))

sum3_df = copq_sbu3.agg(sf.sum('APRIL').alias('APRIL'),sf.sum('MAY').alias('MAY'),sf.sum('JUNE').alias('JUNE'),sf.sum('JULY').alias('JULY'),sf.sum('AUGUST').alias('AUGUST'),sf.sum('SEPTEMBER').alias('SEPTEMBER'),sf.sum('OCTOBER').alias('OCTOBER'),sf.sum('NOVEMBER').alias('NOVEMBER'),sf.sum('DECEMBER').alias('DECEMBER'),sf.sum('JANUARY').alias('JANUARY'),sf.sum('FEBRUARY').alias('FEBRUARY'),sf.sum('MARCH').alias('MARCH'))

# COMMAND ----------

# finding the columns which are not null and removing the null columns
num1_df = sum1_df.withColumn("MONTH",when(col('MARCH').isNotNull(),lit(21)).when(col('FEBRUARY').isNotNull(),lit(20)).when(col('JANUARY').isNotNull(),lit(19)).when(col('DECEMBER').isNotNull(),lit(18)).when(col('NOVEMBER').isNotNull(),lit(17)).when(col('OCTOBER').isNotNull(),lit(16)).when(col('SEPTEMBER').isNotNull(),lit(15)).when(col('AUGUST').isNotNull(),lit(14)).when(col('JULY').isNotNull(),lit(13)).when(col('JUNE').isNotNull(),lit(12)).when(col('MAY').isNotNull(),lit(11)).when(col('APRIL').isNotNull(),lit(10)))
sbu1_col = num1_df.select('MONTH').collect()[0][0]

num2_df = sum2_df.withColumn("MONTH",when(col('MARCH').isNotNull(),lit(21)).when(col('FEBRUARY').isNotNull(),lit(20)).when(col('JANUARY').isNotNull(),lit(19)).when(col('DECEMBER').isNotNull(),lit(18)).when(col('NOVEMBER').isNotNull(),lit(17)).when(col('OCTOBER').isNotNull(),lit(16)).when(col('SEPTEMBER').isNotNull(),lit(15)).when(col('AUGUST').isNotNull(),lit(14)).when(col('JULY').isNotNull(),lit(13)).when(col('JUNE').isNotNull(),lit(12)).when(col('MAY').isNotNull(),lit(11)).when(col('APRIL').isNotNull(),lit(10)))
sbu2_col = num2_df.select('MONTH').collect()[0][0]

num3_df = sum3_df.withColumn("MONTH",when(col('MARCH').isNotNull(),lit(21)).when(col('FEBRUARY').isNotNull(),lit(20)).when(col('JANUARY').isNotNull(),lit(19)).when(col('DECEMBER').isNotNull(),lit(18)).when(col('NOVEMBER').isNotNull(),lit(17)).when(col('OCTOBER').isNotNull(),lit(16)).when(col('SEPTEMBER').isNotNull(),lit(15)).when(col('AUGUST').isNotNull(),lit(14)).when(col('JULY').isNotNull(),lit(13)).when(col('JUNE').isNotNull(),lit(12)).when(col('MAY').isNotNull(),lit(11)).when(col('APRIL').isNotNull(),lit(10)))
sbu3_col = num3_df.select('MONTH').collect()[0][0]

# COMMAND ----------

# selecting only not null columns
sbu1_df = copq_sbu1.select(copq_sbu1.columns[:sbu1_col])
sbu2_df = copq_sbu2.select(copq_sbu2.columns[:sbu2_col])
sbu3_df = copq_sbu3.select(copq_sbu3.columns[:sbu3_col])

# COMMAND ----------

# selecting the month columns which have values
pivot_sbu1_col = [c for c in sbu1_df.columns if c not in {"SBU","PRODUCT_LINE","PLANT_ID","COPQ_TYPE","DESCRIPTION","S_NO","COST_TO_BE_CONSIDERED","UOM","FINANCIAL_YEAR"}]
pivot_sbu2_col = [c for c in sbu2_df.columns if c not in {"SBU","PRODUCT_LINE","PLANT_ID","COPQ_TYPE","DESCRIPTION","S_NO","COST_TO_BE_CONSIDERED","UOM","FINANCIAL_YEAR"}]
pivot_sbu3_col = [c for c in sbu3_df.columns if c not in {"SBU","PRODUCT_LINE","PLANT_ID","COPQ_TYPE","DESCRIPTION","S_NO","COST_TO_BE_CONSIDERED","UOM","FINANCIAL_YEAR"}]

# COMMAND ----------

# pivot month columns to rows
pivot_df1 = sbu1_df.selectExpr("SBU","PRODUCT_LINE","PLANT_ID","COPQ_TYPE","DESCRIPTION","S_NO","COST_TO_BE_CONSIDERED","UOM","FINANCIAL_YEAR", "stack({}, {})".format(len(pivot_sbu1_col), ', '.join(("'{}', {}".format(i, i) for i in pivot_sbu1_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","COST")

pivot_df2 = sbu2_df.selectExpr("SBU","PRODUCT_LINE","PLANT_ID","COPQ_TYPE","DESCRIPTION","S_NO","COST_TO_BE_CONSIDERED","UOM","FINANCIAL_YEAR", "stack({}, {})".format(len(pivot_sbu2_col), ', '.join(("'{}', {}".format(i, i) for i in pivot_sbu2_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","COST")

pivot_df3 = sbu3_df.selectExpr("SBU","PRODUCT_LINE","PLANT_ID","COPQ_TYPE","DESCRIPTION","S_NO","COST_TO_BE_CONSIDERED","UOM","FINANCIAL_YEAR", "stack({}, {})".format(len(pivot_sbu3_col), ', '.join(("'{}', {}".format(i, i) for i in pivot_sbu3_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","COST")

# COMMAND ----------

# changing FY and month to calendar date of start day of month
df1 = pivot_df1.union(pivot_df2).union(pivot_df3)
df2 = df1.withColumn('MONTH_NUMBER', sf.date_format(to_date(col('MONTH'), 'MMMMM'), 'MM'))
df_yr = df2.withColumn('YEAR',when((col("MONTH_NUMBER")  == '01') | (col("MONTH_NUMBER")  == '02') |(col("MONTH_NUMBER")  == '03'),df2["FINANCIAL_YEAR"]).otherwise((df2["FINANCIAL_YEAR"].cast(IntegerType())) - 1))
df3 = df_yr.withColumn('DATE_STR',sf.concat(sf.col('YEAR'),sf.lit('-'), sf.col('MONTH_NUMBER'),sf.lit('-01')))
final_df = df3.withColumn("DATE",df3['DATE_STR'].cast(DateType())).drop('FINANCIAL_YEAR').drop('LAST_UPDATED_DT_TS').drop('MONTH').drop('MONTH_NUMBER').drop('YEAR').drop('DATE_STR')

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'COPQ', mode='overwrite')

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
  {fact_name}.SBU,
  {fact_name}.COPQ_TYPE,
  {fact_name}.DESCRIPTION,
  {fact_name}.S_NO,
  {fact_name}.COST_TO_BE_CONSIDERED,
  {fact_name}.UOM,
  {fact_name}.COST
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

# generating surrogate keys
d = surrogate_mapping_hil(csv_data,table_name,"COPQ",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+"FACT_QUALITY_COPQ")

# COMMAND ----------

# inserting log record to last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
