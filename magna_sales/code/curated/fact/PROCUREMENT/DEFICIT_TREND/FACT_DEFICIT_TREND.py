# Databricks notebook source
# import statements
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, split, concat, when, count, concat,upper,to_date, lpad, first,last_day, datediff,date_format,date_add,explode,row_number, sum, max as max_
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType, DecimalType
from pyspark.sql import functions as sf
import pandas as pd
from pyspark.sql.window import Window
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
spark.conf.set("spark.sql.broadcastTimeout",  36000)

# COMMAND ----------

# class object definition
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
table_name = "FACT_DEFICIT_TREND"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
check_date = (last_processed_time - timedelta(days=1)).strftime("%Y-%m-%d")

# COMMAND ----------

# variables to run the code manually
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata_prod"
# curated_db_name = "cur_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# scope = "AZR-DBR-KV-SCOPE-300"
# processed_location = "mnt/bi_datalake/prod/pro/"
# processed_schema_name = "sales_semantic_prod"
# processed_db_name = "pro_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_DEFICIT_TREND"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# check_date = (last_processed_time - timedelta(days=1)).strftime("%Y-%m-%d")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
PRODUCTION_PLAN = spark.sql("select * from {}.{}".format(curated_db_name, "PRODUCTION_PLAN"))
RM_AVERAGE_CONSUMPTION = spark.sql("select * from {}.{}".format(curated_db_name, "RM_REQUIREMENT"))
RM_GROUP_MAPPING = spark.sql("select * from {}.{}".format(curated_db_name, "RM_GROUPING"))

# COMMAND ----------

# defining columns which doesn't have values
sum1_df_PP = PRODUCTION_PLAN.agg(sf.sum('APRIL').alias('APRIL'),sf.sum('MAY').alias('MAY'),sf.sum('JUNE').alias('JUNE'),sf.sum('JULY').alias('JULY'),sf.sum('AUGUST').alias('AUGUST'),sf.sum('SEPTEMBER').alias('SEPTEMBER'),sf.sum('OCTOBER').alias('OCTOBER'),sf.sum('NOVEMBER').alias('NOVEMBER'),sf.sum('DECEMBER').alias('DECEMBER'),sf.sum('JANUARY').alias('JANUARY'),sf.sum('FEBRUARY').alias('FEBRUARY'),sf.sum('MARCH').alias('MARCH'))

num1_df_PP = sum1_df_PP.withColumn("MONTH",when(col('MARCH').isNotNull(),lit(16)).when(col('FEBRUARY').isNotNull(),lit(15)).when(col('JANUARY').isNotNull(),lit(14)).when(col('DECEMBER').isNotNull(),lit(13)).when(col('NOVEMBER').isNotNull(),lit(12)).when(col('OCTOBER').isNotNull(),lit(11)).when(col('SEPTEMBER').isNotNull(),lit(10)).when(col('AUGUST').isNotNull(),lit(9)).when(col('JULY').isNotNull(),lit(8)).when(col('JUNE').isNotNull(),lit(7)).when(col('MAY').isNotNull(),lit(6)).when(col('APRIL').isNotNull(),lit(5)))
rm_col_PP = num1_df_PP.select('MONTH').collect()[0][0]

trans_df_PP = PRODUCTION_PLAN.select(PRODUCTION_PLAN.columns[:rm_col_PP])

# COMMAND ----------

# defining columns which doesn't have values
sum1_df_RAC = RM_AVERAGE_CONSUMPTION.agg(sf.sum('APRIL').alias('APRIL'),sf.sum('MAY').alias('MAY'),sf.sum('JUNE').alias('JUNE'),sf.sum('JULY').alias('JULY'),sf.sum('AUGUST').alias('AUGUST'),sf.sum('SEPTEMBER').alias('SEPTEMBER'),sf.sum('OCTOBER').alias('OCTOBER'),sf.sum('NOVEMBER').alias('NOVEMBER'),sf.sum('DECEMBER').alias('DECEMBER'),sf.sum('JANUARY').alias('JANUARY'),sf.sum('FEBRUARY').alias('FEBRUARY'),sf.sum('MARCH').alias('MARCH'))

num1_df_RAC = sum1_df_RAC.withColumn("MONTH",when(col('MARCH').isNotNull(),lit(17)).when(col('FEBRUARY').isNotNull(),lit(16)).when(col('JANUARY').isNotNull(),lit(15)).when(col('DECEMBER').isNotNull(),lit(14)).when(col('NOVEMBER').isNotNull(),lit(13)).when(col('OCTOBER').isNotNull(),lit(12)).when(col('SEPTEMBER').isNotNull(),lit(11)).when(col('AUGUST').isNotNull(),lit(10)).when(col('JULY').isNotNull(),lit(9)).when(col('JUNE').isNotNull(),lit(8)).when(col('MAY').isNotNull(),lit(7)).when(col('APRIL').isNotNull(),lit(6)))

rm_col_RAC = num1_df_RAC.select('MONTH').collect()[0][0]

trans_df_RAC = RM_AVERAGE_CONSUMPTION.select(RM_AVERAGE_CONSUMPTION.columns[:rm_col_RAC])

# COMMAND ----------

# extracting month columns
pivot_col_PP = [c for c in trans_df_PP.columns if c not in {"PLANT_ID","PLANT_NAME","PRODUCT_LINE","FINANCIAL_YEAR"}]

# COMMAND ----------

# extracting month columns
pivot_col_RAC = [c for c in trans_df_RAC.columns if c not in {"PLANT_ID","PLANT_NAME","PRODUCT_LINE","RM_GROUP","FINANCIAL_YEAR"}]

# COMMAND ----------

# pivot month cols data to rows
pivot_df_PP = trans_df_PP.selectExpr("PLANT_ID","PLANT_NAME","PRODUCT_LINE","FINANCIAL_YEAR", "stack({}, {})".format(len(pivot_col_PP), ', '.join(("'{}', {}".format(i, i) for i in pivot_col_PP)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","TARGET")

# COMMAND ----------

# pivot month cols data to rows
pivot_df_RAC = trans_df_RAC.selectExpr("PLANT_ID","PLANT_NAME","PRODUCT_LINE","RM_GROUP","FINANCIAL_YEAR", "stack({}, {})".format(len(pivot_col_RAC), ', '.join(("'{}', {}".format(i, i) for i in pivot_col_RAC)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","TARGET")

# COMMAND ----------

# defining date column with first day of month and converting FY yo calendar year
df2_PP = pivot_df_PP.withColumn('MONTH_NUMBER', sf.date_format(to_date(col('MONTH'), 'MMMMM'), 'MM'))
df_yr_PP = df2_PP.withColumn('YEAR',df2_PP["FINANCIAL_YEAR"].cast(IntegerType()))
df3_PP = df_yr_PP.withColumn('DATE_STR',sf.concat(sf.col('YEAR'),sf.lit('-'), sf.col('MONTH_NUMBER'),sf.lit('-01')))
final_df_PP_1 = df3_PP.withColumn("DATE",df3_PP['DATE_STR'].cast(DateType())).drop('FINANCIAL_YEAR').drop('LAST_UPDATED_DT_TS').drop('MONTH').drop('MONTH_NUMBER').drop('YEAR').drop('DATE_STR')

final_df_PP_check = final_df_PP_1.withColumn('days',datediff(last_day(final_df_PP_1.DATE).alias('DATE'),col('DATE'))+1)
final_df_PP = final_df_PP_check.withColumn('PLANNED_PRODUCTION_DAY', sf.round(col('TARGET') / col('days'),2))

# COMMAND ----------

# defining date column with first day of month and converting FY yo calendar year
df2_RAC = pivot_df_RAC.withColumn('MONTH_NUMBER', sf.date_format(to_date(col('MONTH'), 'MMMMM'), 'MM'))
df_yr_RAC = df2_RAC.withColumn('YEAR',df2_RAC["FINANCIAL_YEAR"].cast(IntegerType()))
df3_RAC = df_yr_RAC.withColumn('DATE_STR',sf.concat(sf.col('YEAR'),sf.lit('-'), sf.col('MONTH_NUMBER'),sf.lit('-01')))
final_df_RAC = df3_RAC.withColumn("DATE",df3_RAC['DATE_STR'].cast(DateType())).drop('FINANCIAL_YEAR').drop('LAST_UPDATED_DT_TS').drop('MONTH').drop('MONTH_NUMBER').drop('YEAR').drop('DATE_STR')

# COMMAND ----------

# adding planned production
final_target_co = final_df_RAC.alias("rac").join(final_df_PP.alias("pp"),on=[col("rac.PLANT_ID") == col("pp.PLANT_ID"),col("rac.PRODUCT_LINE") == col("pp.PRODUCT_LINE"),col("rac.DATE") == col("pp.DATE")],how='left').select(["rac." + cols for cols in final_df_RAC.columns] + [col("pp.PLANNED_PRODUCTION_DAY").alias("PLANNED_PRODUCTION")])

# COMMAND ----------

# adding material number and adding leading 0's to material id field
final_target_rm = final_target_co.alias("raco").join(RM_GROUP_MAPPING.alias("rm"),on=[col("raco.RM_GROUP") == col("rm.RM_GROUP")],how='left').select(["raco." + cols for cols in final_target_co.columns] + [lpad(col("rm.RM_MATERIAL_ID"),18,'0').alias("RM_MATERIAL_NUMBER")])

# COMMAND ----------

# dropping columns and duplicates
columns_to_drop = ['RM_MATERIAL_NUMBER']
final_target = final_target_rm.drop(*columns_to_drop).dropDuplicates()

# COMMAND ----------

# reading dimensions data from processed layer
FACT_MARD_QUANTITY = spark.sql("select * from {}.{}".format(processed_db_name, "FACT_INVENTORY"))
DIM_PLANT_DEPOT = spark.sql("select * from {}.{}".format(processed_db_name, "DIM_PLANT_DEPOT"))
DIM_PRODUCT = spark.sql("select * from {}.{}".format(processed_db_name, "DIM_PRODUCT"))

# COMMAND ----------

# adding plant id columns
df1 = FACT_MARD_QUANTITY.alias("df").join(DIM_PLANT_DEPOT.alias("p_df"),on=[col("df.PLANT_KEY") == col("p_df.PLANT_KEY")],how='left').select(["df." + cols for cols in FACT_MARD_QUANTITY.columns] + [col("p_df.PLANT_ID").alias("PLANT_ID")])

# COMMAND ----------

# adding material number and metric ton conversion value
df2 = df1.alias("df1").join(DIM_PRODUCT.alias("m_df"),on=[col("df1.MATERIAL_KEY") == col("m_df.MATERIAL_KEY")],how='left').select(["df1." + cols for cols in df1.columns] + [col("m_df.MATERIAL_NUMBER").alias("MATERIAL_NUMBER")]).where(col("MT_CONV_VALUE").isNotNull())

# COMMAND ----------

# adding date field
df3 = df2.selectExpr("MATERIAL_NUMBER","PLANT_ID","UNREST_QTY").withColumn('DATE',lit(check_date))

# COMMAND ----------

# adding unrestricted qty
deptDF_add = final_target_rm.alias("df3").join(df3.alias("f_df"),on=[col("df3.PLANT_ID") == col("f_df.PLANT_ID"),col("df3.RM_MATERIAL_NUMBER") == col("f_df.MATERIAL_NUMBER")],how='left').select(["df3." + cols for cols in final_target_rm.columns] + [col("f_df.UNREST_QTY").alias("QTY")] + [col("f_df.DATE").alias("DATE_IN")])

# COMMAND ----------

# changing date format
deptDF_final = deptDF_add.filter(date_format(col('DATE'), 'MM-yyy') == date_format(col('DATE_IN'), 'MM-yyy')).filter(col("QTY").isNotNull())

# COMMAND ----------

# selecting columns
final_1 = deptDF_final.selectExpr("RM_GROUP","RM_MATERIAL_NUMBER as MATERIAL_ID","PRODUCT_LINE","PLANT_ID","PLANT_NAME","PLANNED_PRODUCTION","TARGET as RM_PER_UNIT","QTY","DATE_IN as STOCK_DATE")

# COMMAND ----------

# duplicating data for all days
final_2 = final_1.withColumn("add_duplicate", expr("explode(array_repeat(30,int(30)))"))
final_3 = final_2.withColumn('SENT_NUMBER',row_number().over(Window.partitionBy("RM_GROUP","MATERIAL_ID","PRODUCT_LINE","PLANT_ID","PLANT_NAME","PLANNED_PRODUCTION","RM_PER_UNIT","QTY","STOCK_DATE","add_duplicate").orderBy('STOCK_DATE')))
final_4 = final_3.withColumn("STOCK_DATE_FUTURE", expr("date_add(STOCK_DATE, SENT_NUMBER)"))
columns_to_drop = ['add_duplicate', 'SENT_NUMBER','PLANNED_PRODUCTION','RM_PER_UNIT']
# columns_to_drop = ['add_duplicate', 'SENT_NUMBER']
final_5 = final_4.drop(*columns_to_drop)

# COMMAND ----------

# aggregating data
final_5_a = final_5.groupBy("RM_GROUP","PRODUCT_LINE","PLANT_ID","PLANT_NAME","STOCK_DATE","STOCK_DATE_FUTURE").agg(sum("QTY").alias("QTY"))

# COMMAND ----------

#defining rm per unit and planned production qty
final_5_add = final_5_a.alias("df4").join(final_target.alias("f_df1"),on=[col("df4.PLANT_ID") == col("f_df1.PLANT_ID"),col("df4.PRODUCT_LINE") == col("f_df1.PRODUCT_LINE"),col("df4.RM_GROUP") == col("f_df1.RM_GROUP"),date_format(col('df4.STOCK_DATE_FUTURE'), 'MM-yyy') == date_format(col('f_df1.DATE'), 'MM-yyy')],how='left').select(["df4." + cols for cols in final_5_a.columns] + [col("f_df1.PLANNED_PRODUCTION").alias("PLANNED_PRODUCTION")] + [col("f_df1.TARGET").alias("RM_PER_UNIT")])

# COMMAND ----------

# defining req rm for prod
final_6 = final_5_add.withColumn('REQUIRED_RM_FOR_PRODUCTION',col("PLANNED_PRODUCTION") * col("RM_PER_UNIT"))

# COMMAND ----------

# partition data on rm group and product line and plant id
w = Window.partitionBy("RM_GROUP","PRODUCT_LINE","PLANT_ID").orderBy("STOCK_DATE_FUTURE").rowsBetween(
    Window.unboundedPreceding,  # Take all rows from the beginning of frame
    Window.currentRow           # To current row
)
final_7 = final_6.withColumn("SUM", sum("REQUIRED_RM_FOR_PRODUCTION").over(w)).orderBy("STOCK_DATE_FUTURE")

# COMMAND ----------

# defining EOD stock qty
final_8 = final_7.withColumn('EOD_STOCK_QTY',col("QTY") - col("SUM")).withColumn('PROCESSED_DATE',lit(check_date))

# COMMAND ----------

# selecting columns
final_df = final_8.selectExpr("RM_GROUP","PRODUCT_LINE","PLANT_ID","PLANT_NAME","PLANNED_PRODUCTION","RM_PER_UNIT","STOCK_DATE_FUTURE as STOCK_DATE","REQUIRED_RM_FOR_PRODUCTION","EOD_STOCK_QTY","PROCESSED_DATE","QTY")

# COMMAND ----------

# writing data to processed layer before generating surrogate meta
final_df.write.parquet(processed_location+'DEFICIT_TREND', mode='overwrite')

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
   RM_GROUP,
   PLANNED_PRODUCTION,
   RM_PER_UNIT,
   REQUIRED_RM_FOR_PRODUCTION,
   EOD_STOCK_QTY,
   QTY
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
final = surrogate_mapping_hil(csv_data,table_name,"DEFICIT_TREND",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate meta
final.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# insert log data into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
