# Databricks notebook source
# import statements
import os
from datetime import datetime,date,timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType, DoubleType, DecimalType
from pyspark.sql import functions as sf
from pyspark.sql import Window
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
spark.conf.set("spark.sql.broadcastTimeout",36000)

# COMMAND ----------

# Generating current date
now = datetime.now()
day_n = now.day
now_hour = now.time().hour
ist_zone = datetime.now() + timedelta(hours=5.5)
var_date = (ist_zone - timedelta(days=1)).strftime("%Y-%m-%d")
var_date

# COMMAND ----------

# defining sap cosing month and year
last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)

start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)

sap_closing_month = (start_day_of_prev_month.strftime("%B")).upper()

sap_closing_year = start_day_of_prev_month.strftime("%Y")

start_day_of_prev_month_str = start_day_of_prev_month.strftime("%Y-%m-%d")


print(sap_closing_month,sap_closing_year)

# COMMAND ----------

# class object definition
pmu = ProcessMetadataUtility()

# COMMAND ----------

# code to fetch the data from ADF Parameters
meta_table = "fact_surrogate_meta"
sap_log_table = "sap_closing_run_meta"
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
table_name = "FACT_PUTTY_MATERIAL_VARIANCE"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
sap_closing_date  = dbutils.widgets.get("sap_closing_date")
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
sap_log_data = pmu.get_data(cursor=db_cursor,col_lookup = sap_log_table,value = table_name,column = 'TABLE_NAME',schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# variables to run the notebook manually
# meta_table = "fact_surrogate_meta"
# sap_log_table = "sap_closing_run_meta"
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
# table_name = "FACT_PUTTY_MATERIAL_VARIANCE"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# sap_closing_date = '2021-05-06'
# db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
# csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
# sap_log_data = pmu.get_data(cursor=db_cursor,col_lookup = sap_log_table,value = table_name,column = 'TABLE_NAME',schema_name = schema_name)
# pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading status of the sap closing job
sap_log_data = sap_log_data.where("YEAR = '{}' and MONTH = '{}'".format(sap_closing_year,sap_closing_month)).select("STATUS").collect()

# COMMAND ----------

# checking whether the job is completed for the given sap closing month and year
if len(sap_log_data)==0:
  pass
elif len(sap_log_data[0][0])!=0 and sap_log_data[0][0] == 'COMPLETED':
  dbutils.notebook.exit(0)

# COMMAND ----------

# reading data from curated layer
mseg = spark.sql("""SELECT mblnr,mjahr,zeile,bwart,matnr,werks,lgort,waers,dmbtr,menge,meins,aufnr,AUFPL,APLZL, BUDAT_MKPF FROM {}.{}""".format(curated_db_name,"mseg")).where("aufnr is not null")

# COMMAND ----------

# reading data from curated layer
mara = spark.sql("""SELECT matnr,matkl,meins,brgew,volum FROM {}.{} """.format(curated_db_name,"mara"))

# COMMAND ----------

# reading data from curated layer
marm = spark.sql("""SELECT matnr,umren,umrez,meinh FROM {}.{} where meinh = 'KG' """.format(curated_db_name,"marm"))

# COMMAND ----------

# reading data from curated layer based on sap closing date
caufv = spark.sql("select * from {}.{} where werks in ('2031','2032') and date_format(ERDAT,'yyyyMM') = {} and IDAT2 <= '{}'".format(curated_db_name,"CAUFV",datetime.strptime(start_day_of_prev_month_str,'%Y-%m-%d').strftime('%Y%m'),datetime.strptime(sap_closing_date,'%Y-%m-%d')))

# COMMAND ----------

# reading data from curated layer
zsbu3 = spark.sql("select * from {}.{}".format(curated_db_name,"ZSBU3"))

# COMMAND ----------

# reading data from curated layer
resb = spark.sql("select * from {}.{} where bwart in ('261','262')".format(curated_db_name,"RESB"))

# COMMAND ----------

# reading data from curated layer
bom_targets = spark.sql("select plant_id,financial_year,material_id,per_production_mt_target from {}.{}".format(curated_db_name,"BOM_TARGETS"))
bom_targets = bom_targets.withColumn("MATERIAL_ID",lpad(col("MATERIAL_ID"),18,'0')).withColumn("financial_year",(col("financial_year").cast(IntegerType()))-1)

# COMMAND ----------

# converting rm disounts from FY to calendar date
df=spark.sql("select * from {}.{}".format(curated_db_name, "PUTTY_RM_DISCOUNTS")).drop("LAST_UPDATED_DT_TS")

sum1_df = df.agg(sf.sum('APRIL').alias('APRIL'),sf.sum('MAY').alias('MAY'),sf.sum('JUNE').alias('JUNE'),sf.sum('JULY').alias('JULY'),sf.sum('AUGUST').alias('AUGUST'),sf.sum('SEPTEMBER').alias('SEPTEMBER'),sf.sum('OCTOBER').alias('OCTOBER'),sf.sum('NOVEMBER').alias('NOVEMBER'),sf.sum('DECEMBER').alias('DECEMBER'),sf.sum('JANUARY').alias('JANUARY'),sf.sum('FEBRUARY').alias('FEBRUARY'),sf.sum('MARCH').alias('MARCH'))

num1_df = sum1_df.withColumn("MONTH",when(col('MARCH').isNotNull(),lit(16)).when(col('FEBRUARY').isNotNull(),lit(15)).when(col('JANUARY').isNotNull(),lit(14)).when(col('DECEMBER').isNotNull(),lit(13)).when(col('NOVEMBER').isNotNull(),lit(12)).when(col('OCTOBER').isNotNull(),lit(11)).when(col('SEPTEMBER').isNotNull(),lit(10)).when(col('AUGUST').isNotNull(),lit(9)).when(col('JULY').isNotNull(),lit(8)).when(col('JUNE').isNotNull(),lit(7)).when(col('MAY').isNotNull(),lit(6)).when(col('APRIL').isNotNull(),lit(5)))
rm_col = num1_df.select('MONTH').collect()[0][0]

trans_df = df.select(df.columns[:rm_col])

pivot_col = [c for c in trans_df.columns if c not in {"PLANT_ID","MATERIAL_ID","DISCOUNT","FINANCIAL_YEAR"}]

pivot_df = trans_df.selectExpr("PLANT_ID","MATERIAL_ID","DISCOUNT","FINANCIAL_YEAR", "stack({}, {})".format(len(pivot_col), ', '.join(("'{}', {}".format(i, i) for i in pivot_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","DISCOUNT_VALUE")

df_2 = pivot_df.withColumn('MONTH_NUMBER', sf.date_format(to_date(col('MONTH'), 'MMMMM'), 'MM'))
df_yr = df_2.withColumn('YEAR',when((col("MONTH_NUMBER")  == '01') | (col("MONTH_NUMBER")  == '02') |(col("MONTH_NUMBER")  == '03'),df_2["FINANCIAL_YEAR"]).otherwise((df_2["FINANCIAL_YEAR"].cast(IntegerType())) - 1))
df_3 = df_yr.withColumn('DATE_STR',sf.concat(sf.col('YEAR'),sf.lit('-'), sf.col('MONTH_NUMBER'),sf.lit('-01')))
df_4 = df_3.withColumn("DATE",df_3['DATE_STR'].cast(DateType())).drop('FINANCIAL_YEAR').drop('LAST_UPDATED_DT_TS').drop('MONTH').drop('MONTH_NUMBER').drop('YEAR').drop('DATE_STR')
final_disc_df = (df_3.withColumn("MATERIAL_ID",lpad(col("MATERIAL_ID"),18,'0')).select(col("PLANT_ID"),col("MATERIAL_ID"),col("DISCOUNT"),col("DISCOUNT_VALUE"),col("YEAR"),col("MONTH"),col("DATE_STR")))

# COMMAND ----------

# converting target master data from FY to calendar months
df = spark.sql("""select PLANT_ID,MATERIAL_ID,FINANCIAL_YEAR,APRIL,MAY,JUNE,JULY,AUGUST,SEPTEMBER,OCTOBER,NOVEMBER,DECEMBER,JANUARY,FEBRUARY,MARCH from {}.{} """.format(curated_db_name, "PUTTY_MIS_TARGET_MASTER")).drop(col("LAST_UPDATED_DT_TS"))

sum1_df = df.agg(sf.sum('APRIL').alias('APRIL'),sf.sum('MAY').alias('MAY'),sf.sum('JUNE').alias('JUNE'),sf.sum('JULY').alias('JULY'),sf.sum('AUGUST').alias('AUGUST'),sf.sum('SEPTEMBER').alias('SEPTEMBER'),sf.sum('OCTOBER').alias('OCTOBER'),sf.sum('NOVEMBER').alias('NOVEMBER'),sf.sum('DECEMBER').alias('DECEMBER'),sf.sum('JANUARY').alias('JANUARY'),sf.sum('FEBRUARY').alias('FEBRUARY'),sf.sum('MARCH').alias('MARCH'))

num1_df = sum1_df.withColumn("MONTH",when(col('MARCH').isNotNull(),lit(17)).when(col('FEBRUARY').isNotNull(),lit(16)).when(col('JANUARY').isNotNull(),lit(15)).when(col('DECEMBER').isNotNull(),lit(14)).when(col('NOVEMBER').isNotNull(),lit(13)).when(col('OCTOBER').isNotNull(),lit(12)).when(col('SEPTEMBER').isNotNull(),lit(11)).when(col('AUGUST').isNotNull(),lit(10)).when(col('JULY').isNotNull(),lit(9)).when(col('JUNE').isNotNull(),lit(8)).when(col('MAY').isNotNull(),lit(7)).when(col('APRIL').isNotNull(),lit(6)))
rm_col = num1_df.select('MONTH').collect()[0][0]

trans_df = df.select(df.columns[:rm_col])

pivot_col = [c for c in trans_df.columns if c not in {"PLANT_ID","MATERIAL_ID","FINANCIAL_YEAR"}]

pivot_df = trans_df.selectExpr("PLANT_ID","MATERIAL_ID","FINANCIAL_YEAR", "stack({}, {})".format(len(pivot_col), ', '.join(("'{}', {}".format(i, i) for i in pivot_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","TARGET_VALUE")


df2 = pivot_df.withColumn('MONTH_NUMBER', sf.date_format(to_date(col('MONTH'), 'MMMMM'), 'MM'))
df_yr = df2.withColumn('YEAR',when((col("MONTH_NUMBER")  == '01') | (col("MONTH_NUMBER")  == '02') |(col("MONTH_NUMBER")  == '03'),df2["FINANCIAL_YEAR"]).otherwise((df2["FINANCIAL_YEAR"].cast(IntegerType())) - 1))
df3 = df_yr.withColumn('DATE_STR',sf.concat(sf.col('YEAR'),sf.lit('-'), sf.col('MONTH_NUMBER'),sf.lit('-01')))
df4 = df3.withColumn("DATE",df3['DATE_STR'].cast(DateType())).drop('FINANCIAL_YEAR').drop(*['LAST_UPDATED_DT_TS','DATE_STR','MONTH','MATERIAL_CATEGORY'])

df_target_price = df4.withColumn("MATERIAL_ID",lpad(col("MATERIAL_ID"),18,'0'))

df_target_price.createOrReplaceTempView("material_target_price")

# COMMAND ----------

# defining FG Production qty against each production order
df1 = caufv.alias("l_df").join(mseg.where("bwart in ('101','102')").alias('r_df'),on=["AUFNR"],how='left').select([col("l_df.aufnr").alias("PRODUCTION_ORDER"),col("l_df.auart").alias("PROD_ORD_TYPE"),col("l_df.plnbez").alias("CAUFV_MATERIAL_ID"),col("l_df.erdat").alias("PROD_ORDER_CREATED_DATE"),col("r_df.MATNR").alias("MSEG_MATERIAL_ID"),col("r_df.werks").alias("PLANT_ID"),col("bwart").alias("MOVEMENT_TYPE"),col("dmbtr").alias("AMOUNT_IN_LC"),col("menge").alias("QUANTITY"),col("MEINS")])


df11 = df1.withColumn("FG_MATERIAL_ID",when(col("CAUFV_MATERIAL_ID").isNotNull(),col("CAUFV_MATERIAL_ID")).otherwise(col("MSEG_MATERIAL_ID")))

df2 = df11.withColumn("QUANTITY",when(col("MOVEMENT_TYPE") == '101',col("QUANTITY")).otherwise(-(col("QUANTITY")))).withColumn("AMOUNT_IN_LC",when(col("MOVEMENT_TYPE") == '101',col("AMOUNT_IN_LC")).otherwise(-(col("AMOUNT_IN_LC"))))

df3 = df2.groupBy("PROD_ORD_TYPE","PRODUCTION_ORDER","PROD_ORDER_CREATED_DATE","FG_MATERIAL_ID","PLANT_ID").agg(sum("QUANTITY").alias("QUANTITY"),sum("AMOUNT_IN_LC").alias("FG_AMOUNT"),first("MEINS").alias("MEINS"))

df4 = df3.alias('l_df').join(marm.alias('r_df'),on=[col('matnr') == col('FG_MATERIAl_ID')],how='left').select([col("l_df." + cols) for cols in df3.columns] + [col("umren"),col("umrez")])

df5 = df4.withColumn("production_qty_kgs", when(col("UMREN").isNotNull(),col("QUANTITY")*(col("UMREN")/col("UMREZ"))).when(col("meins") == 'KG',col("QUANTITY")))

# COMMAND ----------

# defining rm consumption against each production order
df6_1 = caufv.alias("l_df").join(mseg.where("bwart in ('261','262')").alias('r_df'),on=["AUFNR"],how='left').select([col("l_df.aufnr").alias("PRODUCTION_ORDER"),col("l_df.auart").alias("PROD_ORD_TYPE"),col("r_df.matnr").alias("RM_MATERIAL_ID"),col("r_df.werks").alias("PLANT_ID"),col("bwart").alias("MOVEMENT_TYPE"),col("dmbtr").alias("RM_AMOUNT_IN_LC"),col("menge").alias("RM_QUANTITY"),col("MEINS"),col("ERDAT").alias("PROD_ORDER_CREATED_DATE"), col("r_df.BUDAT_MKPF").alias("POSTING_DATE"),col("r_df.MBLNR"), col("MBLNR")])

df6 = df6_1.alias("l_df").join(spark.sql("select * from {}.{}".format(processed_db_name,"DIM_PRODUCT")).alias("r_df"),on = [col("l_df.RM_MATERIAL_ID") == col("r_df.MATERIAL_NUMBER")],how='left').select([col("l_df." + cols) for cols in df6_1.columns] + [col("r_df.MATERIAL_TYPE")])

df7 = df6.withColumn("RM_QUANTITY",when(col("MOVEMENT_TYPE") == '261',col("RM_QUANTITY")).otherwise(-(col("RM_QUANTITY")))).withColumn("RM_AMOUNT_IN_LC",when(col("MOVEMENT_TYPE") == '261',col("RM_AMOUNT_IN_LC")).otherwise(-(col("RM_AMOUNT_IN_LC"))))

# COMMAND ----------

# adding RM amount and quantity to dataframe
df8 = df7.groupBy("PRODUCTION_ORDER","PROD_ORD_TYPE","PROD_ORDER_CREATED_DATE","RM_MATERIAL_ID","PLANT_ID","MATERIAL_TYPE").agg(sum("RM_QUANTITY").alias("RM_QUANTITY"),sum("RM_AMOUNT_IN_LC").alias("RM_AMOUNT"),first("MEINS").alias("MEINS"))

df9 = df8.alias('l_df').join(marm.alias('r_df'),on=[col('matnr') == col('RM_MATERIAl_ID')],how='left').select([col("l_df." + cols) for cols in df8.columns] + [col("umren"),col("umrez")])

df10  = df9.withColumn("rm_actual_qty_kgs", when(col("UMREN").isNotNull(),col("RM_QUANTITY")*(col("UMREN")/col("UMREZ"))).when(col("meins") == 'KG',col("RM_QUANTITY")).when(col("MATERIAL_TYPE") == 'VERP',col("RM_QUANTITY")))

# COMMAND ----------

# adding production target to the dataframe
df11 = df10.withColumn("YEAR",sf.date_format(to_date(col('PROD_ORDER_CREATED_DATE'), 'YYYY'), 'YYYY')).withColumn("MONTH",sf.date_format(to_date(col('PROD_ORDER_CREATED_DATE'), 'MMMM'), 'MM'))

df12 = df11.withColumn('FY_YEAR',when(((col("MONTH")  == '01') | (col("MONTH")  == '02') |(col("MONTH")  == '03')),(col("YEAR").cast(IntegerType())) - 1).otherwise(col("YEAR")))

df13 = df12.alias("l_df").join(bom_targets.alias("r_df"),on=[col("l_df.plant_id") == col("r_df.plant_id"),
                                                           col("l_df.fy_year") == col("r_df.financial_year"),
                                                           col("l_df.rm_material_id") == col("r_df.material_id")],how='left').select([col("l_df." + cols) for cols in df12.columns] + [col("per_production_mt_target").alias("STD_QTY_KGS")])

# COMMAND ----------

# merging all dataframes to one final dataframe
final_11 = df5.alias("l_df").join(df13.alias("r_df"),on = ["PRODUCTION_ORDER"]).select(["l_df.PRODUCTION_ORDER","l_df.PROD_ORD_TYPE","l_df.PROD_ORDER_CREATED_DATE","l_df.FG_MATERIAL_ID",col("l_df.PLANT_ID").alias("plant_id_1"),col("r_df.plant_id").alias("plant_id_2"),"FG_AMOUNT","l_df.production_qty_kgs","RM_MATERIAL_ID","r_df.RM_ACTUAL_QTY_KGS","r_df.RM_AMOUNT","r_df.STD_QTY_KGS"])

final_1 = final_11.withColumn("PLANT_ID",when(col("plant_id_1").isNull(),col('plant_id_2')).otherwise(col("plant_id_1")))

# COMMAND ----------

# adding target value to final dataframe
final_2 = final_1.withColumn("report_month",sf.date_format(to_date(col('PROD_ORDER_CREATED_DATE'), 'MMMMM'), 'MM')).withColumn("report_year",sf.date_format(to_date(col('PROD_ORDER_CREATED_DATE'), 'YYYY'), 'YYYY'))

final_3 = final_2.withColumn('reporting_date',sf.concat(sf.col('report_year'),sf.lit('-'), sf.col('report_month'),sf.lit('-01')))

final_4 = final_3.alias("l_df").join(df_target_price.alias("r_df"),on = [col("r_df.MONTH_NUMBER") == col("l_df.report_month"),
                                                                        col("r_df.YEAR") == col("l_df.report_year"),
                                                                        col("r_df.plant_id") == col("l_df.plant_id"),
                                                                        col("r_df.material_id") == col("l_df.RM_MATERIAL_ID")],how='left').select(["l_df." + cols for cols in final_3.columns]+[col("r_df.TARGET_VALUE")])

# COMMAND ----------

final_5 = final_4.withColumn('MONTH_LOW', date_format(final_4.reporting_date,'MMMMM')).withColumn('YEAR',year(final_4.reporting_date)).select("*", upper(col('MONTH_LOW')).alias('MONTH')).drop('MONTH_LOW')

# COMMAND ----------

# adding discount values
final_6 = final_5.alias('df1').join(final_disc_df.alias('df2'),on = [col('df1.RM_MATERIAL_ID') == col('df2.MATERIAL_ID'),
                                                                     col('df1.PLANT_ID') == col('df2.PLANT_ID'),
                                                                     col('df1.YEAR') == col('df2.YEAR'),
                                                                     col('df1.MONTH') == col('df2.MONTH')], how='left').select([col("df1." + cols) for cols in final_5.columns] + [col("DISCOUNT_VALUE")]).withColumn("DISCOUNT_VALUE_KG",col("DISCOUNT_VALUE")/1000)

# COMMAND ----------

# adding net price, rm rate per kg, rm amount fields
final_7 = final_6.withColumn("NET_PRICE",(col("RM_AMOUNT")/col("RM_ACTUAL_QTY_KGS"))-col("DISCOUNT_VALUE_KG")).withColumn("NET_FINAL_PRICE",col("NET_PRICE")*col("RM_ACTUAL_QTY_KGS"))

final_8 = final_7.withColumn("RM_RATE_PER_KG",when(col('DISCOUNT_VALUE_KG').isNull(),(col("RM_AMOUNT")/col("RM_ACTUAL_QTY_KGS"))).otherwise(col("NET_PRICE")))

final_9 = final_8.withColumn("RM_AMOUNT",when(col("DISCOUNT_VALUE_KG").isNull(),col("RM_AMOUNT")).otherwise(col("NET_FINAL_PRICE")))

# COMMAND ----------

# selecting final req columns and performing appropriate calculations
final_df = final_9.selectExpr("PRODUCTION_ORDER","PROD_ORD_TYPE","plant_id as PLANT_ID","FG_MATERIAL_ID","production_qty_kgs AS PRODUCTION_QTY_KG","FG_AMOUNT","RM_MATERIAL_ID",
                              "RM_ACTUAL_QTY_KGS as RM_ACTUAL_QTY_KG",
                              "RM_AMOUNT",
                              "RM_RATE_PER_KG",
                              "STD_QTY_KGS as STD_QTY_KG",
                              "(STD_QTY_KGS*(production_qty_kgs/1000)) as STD_QTY_FOR_ACTUAL_PROD",
                              "((STD_QTY_KGS*(production_qty_kgs/1000))*TARGET_VALUE) as STD_RM_REQUIRED_VALUE",
                              "TARGET_VALUE as TARGET_PRICE",
                              "((((nvl(STD_QTY_KGS,0)*(production_qty_kgs/1000))) - (RM_ACTUAL_QTY_KGS))*(RM_RATE_PER_KG)) as QTY_VARIANCE",
                              "((nvl(TARGET_VALUE,0) - (RM_RATE_PER_KG))*RM_ACTUAL_QTY_KGS) as RATE_VARIANCE",
                              "reporting_date as REPORTING_DATE")
final_df = final_df.withColumn("LAST_EXECUTED_TIME", lit(last_processed_time_str))
final_df = final_df.withColumn("SNAPSHOT_DATE",lit(datetime.strptime(var_date, '%Y-%m-%d').date()))

# COMMAND ----------

# writing data to processed layer before geenrating surrogate keys
final_df.write.parquet(processed_location+'PUTTY_MATERIAL_VARIANCE', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
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
  PRODUCTION_ORDER,
  PROD_ORD_TYPE,
   PRODUCTION_QTY_KG,
FG_AMOUNT,
RM_ACTUAL_QTY_KG,
RM_AMOUNT,
RM_RATE_PER_KG,
STD_QTY_KG,
STD_QTY_FOR_ACTUAL_PROD,
STD_RM_REQUIRED_VALUE,
TARGET_PRICE,
QTY_VARIANCE,
RATE_VARIANCE,
LAST_EXECUTED_TIME
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
d = surrogate_mapping_hil(csv_data,table_name,"PUTTY_MATERIAL_VARIANCE",processed_db_name)
d.createOrReplaceTempView("d")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS temp_putty

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

tab = processed_db_name + "." + table_name
pro_df = spark.read.table(tab).select( "PRODUCTION_ORDER","PROD_ORD_TYPE","PRODUCTION_QTY_KG","FG_AMOUNT","RM_ACTUAL_QTY_KG","RM_AMOUNT","RM_RATE_PER_KG","STD_QTY_KG","STD_QTY_FOR_ACTUAL_PROD","STD_RM_REQUIRED_VALUE","TARGET_PRICE","QTY_VARIANCE","RATE_VARIANCE","LAST_EXECUTED_TIME","REPORTING_DATE_KEY","FG_MATERIAL_KEY","SNAPSHOT_DATE_KEY","PLANT_KEY","RM_MATERIAL_KEY").write.saveAsTable("temp_putty")

# COMMAND ----------

full_df = spark.sql("select A.PRODUCTION_ORDER,A.PROD_ORD_TYPE,A.PRODUCTION_QTY_KG,A.FG_AMOUNT,A.RM_ACTUAL_QTY_KG,A.RM_AMOUNT,A.RM_RATE_PER_KG,A.STD_QTY_KG,A.STD_QTY_FOR_ACTUAL_PROD,A.STD_RM_REQUIRED_VALUE,A.TARGET_PRICE,A.QTY_VARIANCE,A.RATE_VARIANCE,A.LAST_EXECUTED_TIME,A.REPORTING_DATE_KEY,A.FG_MATERIAL_KEY,A.SNAPSHOT_DATE_KEY,A.PLANT_KEY,A.RM_MATERIAL_KEY from temp_putty A left outer join (select distinct(SNAPSHOT_DATE_KEY)  as SNAPSHOT_DATE_KEY from d) T on A.SNAPSHOT_DATE_KEY=T.SNAPSHOT_DATE_KEY where T.SNAPSHOT_DATE_KEY is null")

# COMMAND ----------

union_df = full_df.union(d)

# COMMAND ----------

# writing data to processed layer after geenrating surrogate keys
union_df.write.mode('overwrite').parquet(processed_location+table_name)


# COMMAND ----------

# inserting log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# inserting log record into sap closing run job details table
date_now = datetime.utcnow()
vals = "('" +  table_name + "','" + sap_closing_year +  "','" +  sap_closing_month + "','" + "COMPLETED','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "SAP_CLOSING_RUN_META", "(TABLE_NAME, YEAR, MONTH, STATUS, RUN_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

dbutils.notebook.exit(1)
