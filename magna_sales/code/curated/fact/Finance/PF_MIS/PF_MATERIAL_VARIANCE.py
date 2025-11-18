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

# defining sap closing month and year
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
table_name = "FACT_PF_MATERIAL_VARIANCE"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
sap_closing_date  = dbutils.widgets.get("sap_closing_date")
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
sap_log_data = pmu.get_data(cursor=db_cursor,col_lookup = sap_log_table,value = table_name,column = 'TABLE_NAME',schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# variables to run the code manually
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
# table_name = "FACT_PF_MATERIAL_VARIANCE"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# sap_closing_date = '2021-05-06'
# db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
# csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
# sap_log_data = pmu.get_data(cursor=db_cursor,col_lookup = sap_log_table,value = table_name,column = 'TABLE_NAME',schema_name = schema_name)
# pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading status of the job runs
sap_log_data = sap_log_data.where("YEAR = '{}' and MONTH = '{}'".format(sap_closing_year,sap_closing_month)).select("STATUS").collect()

# COMMAND ----------

# checking whether the job is completed for the provided month or not
if len(sap_log_data)==0:
  pass
elif len(sap_log_data[0][0])!=0 and sap_log_data[0][0] == 'COMPLETED':
  dbutils.notebook.exit(0)

# COMMAND ----------

# DBTITLE 1,MSEG Table
# reading data from curated layer
mseg = spark.sql("""SELECT mblnr,mjahr,zeile,bwart,matnr,werks,lgort,waers,dmbtr,menge,meins,aufnr,AUFPL,APLZL FROM {}.{}""".format(curated_db_name,"mseg")).where("aufnr is not null")

# COMMAND ----------

# DBTITLE 1,MARA Table
# reading data from curated layer
mara = spark.sql("""SELECT matnr,matkl,meins,brgew,volum FROM {}.{} """.format(curated_db_name,"mara"))

# COMMAND ----------

# DBTITLE 1,MARM Table
# reading data from curated layer
marm = spark.sql("""SELECT matnr,umren,umrez,meinh FROM {}.{} where meinh = 'KG' """.format(curated_db_name,"marm"))

# COMMAND ----------

# reading data from curated layer based on sap closing date
caufv = spark.sql("select * from {}.{} where werks in ('2019','2021','2029') and date_format(ERDAT,'yyyyMM') = {} and IDAT2 <= '{}'".format(curated_db_name,"CAUFV",datetime.strptime(start_day_of_prev_month_str,'%Y-%m-%d').strftime('%Y%m'),datetime.strptime(sap_closing_date,'%Y-%m-%d')))

# COMMAND ----------

# reading data from curated layer
zsbu3 = spark.sql("select * from {}.{}".format(curated_db_name,"ZSBU3"))

# COMMAND ----------

# reading data from curated layer
resb = spark.sql("select * from {}.{} where bwart in ('261','262')".format(curated_db_name,"RESB"))

# COMMAND ----------

# converting target data from columns to rows and adding start date of the month
df = spark.sql("""select PLANT_ID,MATERIAL_CATEGORY,MATERIAL_CODE,UOM,FINANCIAL_YEAR,APRIL,MAY,JUNE,JULY,AUGUST,SEPTEMBER,OCTOBER,NOVEMBER,DECEMBER,JANUARY,FEBRUARY,MARCH from {}.{} """.format(curated_db_name, "PF_MIS_TARGET_MASTER")).drop(col("LAST_UPDATED_DT_TS"))

sum1_df = df.agg(sf.sum('APRIL').alias('APRIL'),sf.sum('MAY').alias('MAY'),sf.sum('JUNE').alias('JUNE'),sf.sum('JULY').alias('JULY'),sf.sum('AUGUST').alias('AUGUST'),sf.sum('SEPTEMBER').alias('SEPTEMBER'),sf.sum('OCTOBER').alias('OCTOBER'),sf.sum('NOVEMBER').alias('NOVEMBER'),sf.sum('DECEMBER').alias('DECEMBER'),sf.sum('JANUARY').alias('JANUARY'),sf.sum('FEBRUARY').alias('FEBRUARY'),sf.sum('MARCH').alias('MARCH'))

num1_df = sum1_df.withColumn("MONTH",when(col('MARCH').isNotNull(),lit(17)).when(col('FEBRUARY').isNotNull(),lit(16)).when(col('JANUARY').isNotNull(),lit(15)).when(col('DECEMBER').isNotNull(),lit(14)).when(col('NOVEMBER').isNotNull(),lit(13)).when(col('OCTOBER').isNotNull(),lit(12)).when(col('SEPTEMBER').isNotNull(),lit(11)).when(col('AUGUST').isNotNull(),lit(10)).when(col('JULY').isNotNull(),lit(9)).when(col('JUNE').isNotNull(),lit(8)).when(col('MAY').isNotNull(),lit(7)).when(col('APRIL').isNotNull(),lit(6)))
rm_col = num1_df.select('MONTH').collect()[0][0]

trans_df = df.select(df.columns[:rm_col])

pivot_col = [c for c in trans_df.columns if c not in {"PLANT_ID","MATERIAL_CATEGORY","MATERIAL_CODE","UOM","FINANCIAL_YEAR"}]

pivot_df = trans_df.selectExpr("PLANT_ID","MATERIAL_CATEGORY","MATERIAL_CODE","UOM","FINANCIAL_YEAR", "stack({}, {})".format(len(pivot_col), ', '.join(("'{}', {}".format(i, i) for i in pivot_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","TARGET_VALUE")


df2 = pivot_df.withColumn('MONTH_NUMBER', sf.date_format(to_date(col('MONTH'), 'MMMMM'), 'MM'))
df_yr = df2.withColumn('YEAR',when((col("MONTH_NUMBER")  == '01') | (col("MONTH_NUMBER")  == '02') |(col("MONTH_NUMBER")  == '03'),df2["FINANCIAL_YEAR"]).otherwise((df2["FINANCIAL_YEAR"].cast(IntegerType())) - 1))
df3 = df_yr.withColumn('DATE_STR',sf.concat(sf.col('YEAR'),sf.lit('-'), sf.col('MONTH_NUMBER'),sf.lit('-01')))
df4 = df3.withColumn("DATE",df3['DATE_STR'].cast(DateType())).drop('FINANCIAL_YEAR').drop(*['LAST_UPDATED_DT_TS','DATE_STR','MONTH','MATERIAL_CATEGORY'])

df_target_price = df4.withColumn("MATERIAL_CODE",lpad(col("MATERIAL_CODE"),18,'0'))

df_target_price.createOrReplaceTempView("material_target_price")

# COMMAND ----------

# defining FG production qty against each production order
df1 = caufv.alias("l_df").join(mseg.where("bwart in ('101','102')").alias('r_df'),on=["AUFNR"],how='left').select([col("l_df.aufnr").alias("PRODUCTION_ORDER"),col("l_df.auart").alias("PROD_ORD_TYPE"),col("l_df.plnbez").alias("FG_MATERIAL_ID"),col("l_df.erdat").alias("PROD_ORDER_CREATED_DATE"),col("r_df.werks").alias("PLANT_ID"),col("bwart").alias("MOVEMENT_TYPE"),col("dmbtr").alias("AMOUNT_IN_LC"),col("menge").alias("QUANTITY"),col("MEINS")])

df2 = df1.withColumn("QUANTITY",when(col("MOVEMENT_TYPE") == '101',col("QUANTITY")).otherwise(-(col("QUANTITY")))).withColumn("AMOUNT_IN_LC",when(col("MOVEMENT_TYPE") == '101',col("AMOUNT_IN_LC")).otherwise(-(col("AMOUNT_IN_LC"))))

df3 = df2.groupBy("PROD_ORD_TYPE","PRODUCTION_ORDER","PROD_ORDER_CREATED_DATE","FG_MATERIAL_ID","PLANT_ID").agg(sum("QUANTITY").alias("QUANTITY"),sum("AMOUNT_IN_LC").alias("FG_AMOUNT"),first("MEINS").alias("MEINS"))

df4 = df3.alias('l_df').join(marm.alias('r_df'),on=[col('matnr') == col('FG_MATERIAl_ID')],how='left').select([col("l_df." + cols) for cols in df3.columns] + [col("umren"),col("umrez")])

df5 = df4.alias('ls2').join(zsbu3,[df4["FG_MATERIAL_ID"] == zsbu3["matnr"],
                                                df4["plant_id"] == zsbu3["werks"]],
                                                how='left').select(
  [col("ls2." + cols) for cols in df4.columns] + [col("STD_WT").alias("ZSBU3_STD_WT"),col("VALID_FROM"),col("VALID_TO")]
).where((col("PROD_ORDER_CREATED_DATE").between(col("VALID_FROM"),col("VALID_TO"))) | ((col("VALID_FROM").isNull()) & (col("VALID_TO").isNull()))).drop("VALID_FROM","VALID_TO")

df6_1  = df5.withColumn("production_qty_kgs", when(col("UMREN").isNotNull(),col("QUANTITY")*(col("UMREN")/col("UMREZ"))).when(col("ZSBU3_STD_WT").isNotNull(),col("QUANTITY")*col("ZSBU3_STD_WT")).when(col("meins") == 'KG',col("QUANTITY")))

df6 = df6_1.withColumn("FG_STANDARD_WT",when(col("UMREN").isNotNull(),(col("UMREN")/col("UMREZ"))).when(col("ZSBU3_STD_WT").isNotNull(),col("ZSBU3_STD_WT")).when(col("meins") == 'KG',lit(1)))

# COMMAND ----------

# defining RM consumed qty against each production order
df7 = caufv.alias("l_df").join(mseg.where("bwart in ('261','262')").alias('r_df'),on=["AUFNR"],how='left').select([col("l_df.aufnr").alias("PRODUCTION_ORDER"),col("l_df.auart").alias("PROD_ORD_TYPE"),col("r_df.matnr").alias("RM_MATERIAL_ID"),col("r_df.werks").alias("PLANT_ID"),col("bwart").alias("MOVEMENT_TYPE"),col("dmbtr").alias("RM_AMOUNT_IN_LC"),col("menge").alias("RM_QUANTITY"),col("MEINS"),col("ERDAT").alias("PROD_ORDER_CREATED_DATE")])

df8 = df7.withColumn("RM_QUANTITY",when(col("MOVEMENT_TYPE") == '261',col("RM_QUANTITY")).otherwise(-(col("RM_QUANTITY")))).withColumn("RM_AMOUNT_IN_LC",when(col("MOVEMENT_TYPE") == '261',col("RM_AMOUNT_IN_LC")).otherwise(-(col("RM_AMOUNT_IN_LC"))))

df9 = df8.groupBy("PRODUCTION_ORDER","PROD_ORD_TYPE","PROD_ORDER_CREATED_DATE","RM_MATERIAL_ID","PLANT_ID").agg(sum("RM_QUANTITY").alias("RM_QUANTITY"),sum("RM_AMOUNT_IN_LC").alias("RM_AMOUNT"),first("MEINS").alias("MEINS"))

df10 = df9.alias('l_df').join(marm.alias('r_df'),on=[col('matnr') == col('RM_MATERIAl_ID')],how='left').select([col("l_df." + cols) for cols in df9.columns] + [col("umren"),col("umrez")])

df11 = df10.alias('ls2').join(zsbu3,[df10["RM_MATERIAL_ID"] == zsbu3["matnr"],
                                                df10["plant_id"] == zsbu3["werks"]],
                                                how='left').select(
  [col("ls2." + cols) for cols in df10.columns] + [col("STD_WT").alias("ZSBU3_STD_WT"),col("VALID_FROM"),col("VALID_TO")]
).where((col("PROD_ORDER_CREATED_DATE").between(col("VALID_FROM"),col("VALID_TO"))) | ((col("VALID_FROM").isNull()) & (col("VALID_TO").isNull()))).drop("VALID_FROM","VALID_TO")

df12  = df11.withColumn("rm_actual_qty_kgs", when(col("UMREN").isNotNull(),col("RM_QUANTITY")*(col("UMREN")/col("UMREZ"))).when(col("ZSBU3_STD_WT").isNotNull(),col("RM_QUANTITY")*col("ZSBU3_STD_WT")).when(col("meins") == 'KG',col("RM_QUANTITY")))

# COMMAND ----------

# defining STD BOM qty for RM against each production order
df13 = caufv.alias("l_df").join(resb.alias('r_df'),on=["AUFNR"],how='left').select([col("l_df.aufnr").alias("PRODUCTION_ORDER"),col("GAMNG").alias("TARGET_QTY"),col("r_df.matnr").alias("STD_MATERIAL_ID"),col("r_df.werks").alias("PLANT_ID"),col("bwart").alias("MOVEMENT_TYPE"),col("bdmng").alias("STD_QUANTITY"),col("ERFME"),col("ERDAT").alias("PROD_ORDER_CREATED_DATE")])

df14 = df13.withColumn("STD_QUANTITY",when((col("MOVEMENT_TYPE").isin(["261"])),col("STD_QUANTITY")).when((col("MOVEMENT_TYPE").isin(["262"])),-(col("STD_QUANTITY"))))

df15 = df14.groupBy("PRODUCTION_ORDER","PROD_ORDER_CREATED_DATE","STD_MATERIAL_ID","PLANT_ID","TARGET_QTY").agg(sum("STD_QUANTITY").alias("STD_QUANTITY"),first("ERFME").alias("ERFME"))

df16 = df15.alias('l_df').join(marm.alias('r_df'),on=[col('matnr') == col('STD_MATERIAL_ID')],how='left').select([col("l_df." + cols) for cols in df15.columns] + [col("umren"),col("umrez")])

df17 = df16.alias('ls2').join(zsbu3,[df16["STD_MATERIAL_ID"] == zsbu3["matnr"],
                                                df16["plant_id"] == zsbu3["werks"]],
                                                how='left').select(
  [col("ls2." + cols) for cols in df16.columns] + [col("STD_WT").alias("ZSBU3_STD_WT"),col("VALID_FROM"),col("VALID_TO")]
).where((col("PROD_ORDER_CREATED_DATE").between(col("VALID_FROM"),col("VALID_TO"))) | ((col("VALID_FROM").isNull()) & (col("VALID_TO").isNull()))).drop("VALID_FROM","VALID_TO")

df18  = df17.withColumn("std_qty_kgs", when(col("UMREN").isNotNull(),col("STD_QUANTITY")*(col("UMREN")/col("UMREZ"))).when(col("ZSBU3_STD_WT").isNotNull(),col("STD_QUANTITY")*col("ZSBU3_STD_WT")).when(col("erfme") == 'KG',col("STD_QUANTITY")))

df19 = df18.withColumn("STD_BOM_QTY",(col("std_qty_kgs")/col("TARGET_QTY")))

# COMMAND ----------

# merging all data frames into final dataframe
final_1 = df6.alias("l_df").join(df12.alias("r_df"),on = ["PRODUCTION_ORDER"]).select(["l_df.PRODUCTION_ORDER","l_df.PROD_ORD_TYPE","l_df.PROD_ORDER_CREATED_DATE","l_df.FG_MATERIAL_ID","l_df.PLANT_ID","FG_AMOUNT","l_df.production_qty_kgs",
"l_df.FG_STANDARD_WT","l_df.QUANTITY","RM_MATERIAL_ID","r_df.RM_ACTUAL_QTY_KGS","r_df.RM_AMOUNT","r_df.ZSBU3_STD_WT","r_df.RM_QUANTITY"])

# COMMAND ----------

final_2 = final_1.alias("l_df").join(df19.alias("r_df"),on=[col("l_df.PRODUCTION_ORDER") == col("r_df.PRODUCTION_ORDER"),
                                                           col("l_df.RM_MATERIAL_ID") == col("r_df.STD_MATERIAL_ID")],how='left').select(["l_df." + cols for cols in final_1.columns]+[col("r_df.STD_BOM_QTY")]+[col("r_df.STD_QUANTITY")]+[col("r_df.TARGET_QTY")])

# COMMAND ----------

# adding target values to the final dataframe
final_3 = final_2.withColumn("report_month",sf.date_format(to_date(col('PROD_ORDER_CREATED_DATE'), 'MMMMM'), 'MM')).withColumn("report_year",sf.date_format(to_date(col('PROD_ORDER_CREATED_DATE'), 'YYYY'), 'YYYY'))

final_4 = final_3.withColumn('reporting_date',sf.concat(sf.col('report_year'),sf.lit('-'), sf.col('report_month'),sf.lit('-01')))

final_5 = final_4.alias("l_df").join(df_target_price.alias("r_df"),on = [col("r_df.MONTH_NUMBER") == col("l_df.report_month"),
                                                                        col("r_df.YEAR") == col("l_df.report_year"),
                                                                        col("r_df.plant_id") == col("l_df.plant_id"),
                                                                        col("r_df.material_code") == col("l_df.RM_MATERIAL_ID")],how='left').select(["l_df." + cols for cols in final_4.columns]+[col("r_df.TARGET_VALUE")])

# COMMAND ----------

# selecting columns by performing appropriate calculations
final_rm_1 = final_5.selectExpr("PRODUCTION_ORDER","PROD_ORD_TYPE","plant_id as PLANT_ID","FG_MATERIAL_ID","production_qty_kgs AS PRODUCTION_QTY_KG","FG_AMOUNT","RM_MATERIAL_ID",
                              "RM_ACTUAL_QTY_KGS as RM_ACTUAL_QTY_KG",
                              "RM_AMOUNT",
                              "RM_QUANTITY",
                              "(RM_AMOUNT/RM_ACTUAL_QTY_KGS) as RM_RATE_PER_KG",
                              "STD_BOM_QTY",
                              "STD_QUANTITY",
                              "FG_STANDARD_WT",
                              "QUANTITY as FG_QUANTITY",
                              "(STD_QUANTITY/TARGET_QTY) as STD_BOM_QTY_BASE_UOM",
                              "((STD_BOM_QTY)*100) as STD_BOM_FOR_100_KG", 
                              "((((STD_BOM_QTY)*100)*production_qty_kgs)/100) as STD_RM_REQUIRED_QTY",
                              "(((((STD_BOM_QTY)*100)*production_qty_kgs)/100)*nvl(TARGET_VALUE,0)) as STD_RM_REQUIRED_VALUE",
                              "nvl(TARGET_VALUE,0) as TARGET_PRICE",
                              "((((((STD_BOM_QTY)*100)*production_qty_kgs)/100) - RM_ACTUAL_QTY_KGS)*nvl(TARGET_VALUE,0)) as QTY_VARIANCE",
                              "(RM_ACTUAL_QTY_KGS*(nvl(TARGET_VALUE,0) - (RM_AMOUNT/RM_ACTUAL_QTY_KGS))) as RATE_VARIANCE",
                              "reporting_date as REPORTING_DATE")

# COMMAND ----------

# defining qty variance
final_rm = final_rm_1.withColumn("QTY_VARIANCE", sf.when(final_rm_1["STD_BOM_FOR_100_KG"].isNull(), final_rm_1["RM_ACTUAL_QTY_KG"]*final_rm_1["TARGET_PRICE"]).otherwise(final_rm_1["QTY_VARIANCE"]))
final_rm = final_rm.withColumn("LAST_EXECUTED_TIME", lit(last_processed_time_str))
final_rm = final_rm.withColumn("SNAPSHOT_DATE",lit(datetime.strptime(var_date, '%Y-%m-%d').date()))

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_rm.write.parquet(processed_location+'PF_MATERIAL_VARIANCE', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
  final_rm.createOrReplaceTempView("{}".format(fact_name))
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
FG_STANDARD_WT,
RM_ACTUAL_QTY_KG,
RM_AMOUNT,
RM_QUANTITY,
RM_RATE_PER_KG,
STD_BOM_QTY,
STD_QUANTITY,
FG_QUANTITY,
STD_BOM_QTY_BASE_UOM,
STD_BOM_FOR_100_KG,
STD_RM_REQUIRED_QTY,
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
d = surrogate_mapping_hil(csv_data,table_name,"PF_MATERIAL_VARIANCE",processed_db_name)
d.createOrReplaceTempView("d")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS temp_pf

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

tab = processed_db_name + "." + table_name
pro_df = spark.read.table(tab).select( "PRODUCTION_ORDER","PROD_ORD_TYPE","PRODUCTION_QTY_KG","FG_AMOUNT","FG_STANDARD_WT","RM_ACTUAL_QTY_KG","RM_AMOUNT","RM_QUANTITY","RM_RATE_PER_KG","STD_BOM_QTY","STD_QUANTITY","FG_QUANTITY","STD_BOM_QTY_BASE_UOM","STD_BOM_FOR_100_KG","STD_RM_REQUIRED_QTY","STD_RM_REQUIRED_VALUE","TARGET_PRICE","QTY_VARIANCE","RATE_VARIANCE","LAST_EXECUTED_TIME","SNAPSHOT_DATE_KEY","PLANT_KEY","FG_MATERIAL_KEY","RM_MATERIAL_KEY","REPORTING_DATE_KEY").write.saveAsTable("temp_pf")

# COMMAND ----------

full_df = spark.sql("select A.PRODUCTION_ORDER,A.PROD_ORD_TYPE,A.PRODUCTION_QTY_KG,A.FG_AMOUNT,A.FG_STANDARD_WT,A.RM_ACTUAL_QTY_KG,A.RM_AMOUNT,A.RM_QUANTITY,A.RM_RATE_PER_KG,A.STD_BOM_QTY,A.STD_QUANTITY,A.FG_QUANTITY,A.STD_BOM_QTY_BASE_UOM,A.STD_BOM_FOR_100_KG,A.STD_RM_REQUIRED_QTY,A.STD_RM_REQUIRED_VALUE,A.TARGET_PRICE,A.QTY_VARIANCE,A.RATE_VARIANCE,A.LAST_EXECUTED_TIME,A.SNAPSHOT_DATE_KEY,A.PLANT_KEY,A.FG_MATERIAL_KEY,A.RM_MATERIAL_KEY,A.REPORTING_DATE_KEY from temp_pf A left outer join (select distinct(SNAPSHOT_DATE_KEY)  as SNAPSHOT_DATE_KEY from d) T on A.SNAPSHOT_DATE_KEY=T.SNAPSHOT_DATE_KEY where T.SNAPSHOT_DATE_KEY is null")

# COMMAND ----------

# Union of incremental data with existing data
union_df = full_df.union(d)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
union_df.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# insert log record into last exection details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'INC', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# insert log record into sap closing run meta table
date_now = datetime.utcnow()
vals = "('" +  table_name + "','" + sap_closing_year +  "','" +  sap_closing_month + "','" + "COMPLETED','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "SAP_CLOSING_RUN_META", "(TABLE_NAME, YEAR, MONTH, STATUS, RUN_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

dbutils.notebook.exit(1)
