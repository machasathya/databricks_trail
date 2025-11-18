# Databricks notebook source
# import statements
import time
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad, lower
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql.functions import from_unixtime, unix_timestamp, to_date, first, count,concat
from pyspark.sql import functions as sf
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql.functions import sum
from ProcessMetadataUtility import ProcessMetadataUtility
pmu = ProcessMetadataUtility()
from pyspark.sql import Window
import sys

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
table_name = "FACT_EVA_GROWTH_DEGROWTH"
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
# table_name = "FACT_EVA_GROWTH_DEGROWTH"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
eva_data=spark.sql("select * from {}.{} where particulars = '% Growth or Degrowth'".format(curated_db_name, "EVA")).drop("LAST_UPDATED_DT_TS")
mis_data=spark.sql("select HIL,SBU,PRODUCT_LINE,DATE,SUM(REVENUE) AS REVENUE from {}.{} where actual_or_budget = 'ACTUAL' group by HIL,SBU,PRODUCT_LINE,DATE".format(curated_db_name, "MIS_SUMMARY")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# pivot the data on month field and adding starting date of the month as date
sum1_df = eva_data.agg(sf.sum('APRIL').alias('APRIL'),sf.sum('MAY').alias('MAY'),sf.sum('JUNE').alias('JUNE'),sf.sum('JULY').alias('JULY'),sf.sum('AUGUST').alias('AUGUST'),sf.sum('SEPTEMBER').alias('SEPTEMBER'),sf.sum('OCTOBER').alias('OCTOBER'),sf.sum('NOVEMBER').alias('NOVEMBER'),sf.sum('DECEMBER').alias('DECEMBER'),sf.sum('JANUARY').alias('JANUARY'),sf.sum('FEBRUARY').alias('FEBRUARY'),sf.sum('MARCH').alias('MARCH'))

num1_df = sum1_df.withColumn("MONTH",when(col('MARCH').isNotNull(),lit(18)).when(col('FEBRUARY').isNotNull(),lit(17)).when(col('JANUARY').isNotNull(),lit(16)).when(col('DECEMBER').isNotNull(),lit(15)).when(col('NOVEMBER').isNotNull(),lit(14)).when(col('OCTOBER').isNotNull(),lit(13)).when(col('SEPTEMBER').isNotNull(),lit(12)).when(col('AUGUST').isNotNull(),lit(11)).when(col('JULY').isNotNull(),lit(10)).when(col('JUNE').isNotNull(),lit(9)).when(col('MAY').isNotNull(),lit(8)).when(col('APRIL').isNotNull(),lit(7)))
sbu1_col = num1_df.select('MONTH').collect()[0][0]

eva_null = eva_data.select(eva_data.columns[:sbu1_col])

pivot_col = [c for c in eva_null.columns if c not in {"HIL","SBU","PRODUCT_LINE","PARTICULARS","ACTUAL_OR_BUDGET","FINANCIAL_YEAR"}]

pivot_eva = eva_null.selectExpr("HIL","SBU","PRODUCT_LINE","PARTICULARS","ACTUAL_OR_BUDGET","FINANCIAL_YEAR", "stack({}, {})".format(len(pivot_col), ', '.join(("'{}', {}".format(i, i) for i in pivot_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","PERCENTAGE")

df2 = pivot_eva.withColumn('MONTH_NUMBER', sf.date_format(to_date(col('MONTH'), 'MMMMM'), 'MM'))
df_yr = df2.withColumn('YEAR',when((col("MONTH_NUMBER")  == '01') | (col("MONTH_NUMBER")  == '02') |(col("MONTH_NUMBER")  == '03'),df2["FINANCIAL_YEAR"]).otherwise((df2["FINANCIAL_YEAR"].cast(IntegerType())) - 1))
df3 = df_yr.withColumn('DATE_STR',sf.concat(sf.col('YEAR'),sf.lit('-'), sf.col('MONTH_NUMBER'),sf.lit('-01')))
final_eva_df = df3.withColumn("DATE",df3['DATE_STR'].cast(DateType())).drop('FINANCIAL_YEAR').drop('LAST_UPDATED_DT_TS').drop('MONTH').drop('MONTH_NUMBER').drop('YEAR').drop('DATE_STR')

# COMMAND ----------

# joining eva dataframe with mis data frame to get the revenue
final = final_eva_df.alias("l_df").join(mis_data.alias("r_df"),on=[col('l_df.HIL') == col("r_df.HIL"),
                                                                    col('l_df.SBU') == col("r_df.SBU"),
                                                                   col("l_df.PRODUCT_LINE") == col("r_df.PRODUCT_LINE"),
                                                                   col("l_df.DATE") == col("r_df.DATE")],how='left').select([col("l_df." + cols) for cols in final_eva_df.columns] + [col("REVENUE")])

# COMMAND ----------

# partition on product line
win_spec = Window.partitionBy('PRODUCT_LINE').orderBy(["PRODUCT_LINE","DATE"]).rowsBetween(-sys.maxsize, 0)

# COMMAND ----------

# adding revenue YTD
final_df = final.withColumn('REVENUE_YTD', sf.sum(final.REVENUE).over(win_spec))

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'EVA_GROWTH_DEGROWTH', mode='overwrite')

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
  HIL,
SBU,
PRODUCT_LINE as MIS_PRODUCT_LINE,
PARTICULARS,
ACTUAL_OR_BUDGET,
PERCENTAGE,
REVENUE,
REVENUE_YTD
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
d = surrogate_mapping_hil(csv_data,table_name,"EVA_GROWTH_DEGROWTH",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+"FACT_EVA_GROWTH_DEGROWTH")

# COMMAND ----------

# insert log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)

# COMMAND ----------


