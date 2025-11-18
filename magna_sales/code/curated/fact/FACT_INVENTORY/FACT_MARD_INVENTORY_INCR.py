# Databricks notebook source
# import statements
import os
from datetime import datetime, timedelta,date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, split, concat, when, count, concat,upper,to_date, lpad, first,last_day, datediff,date_format,date_add,explode,row_number, sum, max as max_,trim
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType, DecimalType
from pyspark.sql import functions as sf
import pandas as pd
from pyspark.sql.window import Window
spark.conf.set( "spark.sql.crossJoin.enabled" , "true")
spark.conf.set("spark.sql.broadcastTimeout", 36000)

# COMMAND ----------

# Generating current date
now = datetime.now()
day_n = now.day
now_hour = now.time().hour
ist_zone = datetime.now() + timedelta(hours=5.5)
var_date = (ist_zone - timedelta(days=1)).strftime("%Y-%m-%d")
var_date

# COMMAND ----------

# declaring class for ProcessMetadataUtility
pmu = ProcessMetadataUtility()

# COMMAND ----------

# parameters to get the values from adf
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
table_name = "FACT_INVENTORY"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# Variables used to run the notebook manually
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
# table_name = "FACT_INVENTORY"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# check_date = (last_processed_time - timedelta(days=1)).strftime("%Y-%m-%d")

# COMMAND ----------

# Fetching surrogate_meta from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# Fetching data from curated layer
mard = spark.sql("select MATNR,WERKS,LGORT,LABST,INSME,SPEME FROM {}.{}".format(curated_db_name, "MARD")).drop("LAST_UPDATED_DT_TS")
marm_kg = spark.sql("select * FROM {}.{} where MEINH in ('KG')".format(curated_db_name, "MARM")).drop("LAST_UPDATED_DT_TS")
marm_cum = spark.sql("select * FROM {}.{} where MEINH in ('M3')".format(curated_db_name, "MARM")).drop("LAST_UPDATED_DT_TS")
mbew = spark.sql("Select BWKEY,MATNR,VERPR,STPRS,VPRSV,LFGJA,LFMON,BWTAR from {}.{}".format(curated_db_name,"MBEW")).drop("LAST_UPDATED_DT_TS")
marc = spark.sql("select MATNR,WERKS,EKGRP,UMLMC,TRAME from {}.{} ".format(curated_db_name,"MARC"))
dim_prod = spark.sql("select * from {}.{}".format(processed_db_name,"dim_product"))
zsbu3 = spark.sql("select * from {}.{}".format(curated_db_name,"ZSBU3"))

# COMMAND ----------

# Fetching material type and product line by joining with material master table
pline_mard_df = mard.alias("md").join(dim_prod.alias("dp"),on = [col("md.matnr") == col("dp.material_number")],how='left').select([col("md." + cols) for cols in mard.columns]+[col("MATERIAL_TYPE"),col("UNIT_OF_MEASURE"),col("PRODUCT_LINE")])

# Fetching kg conversion values against each material number from marm
conv_mard_df = pline_mard_df.alias("l_df").join(marm_kg.alias("r_df"),on=[col("l_df.MATNR") == col("r_df.MATNR")],how='left').select([col("l_df."+cols) for cols in pline_mard_df.columns] + [col("UMREZ"),col("UMREN")])

# calculating MT conversion value 
kg_conv_mard_df = conv_mard_df.withColumn("MT_CONV_VAL",(col("UMREN")/col("UMREZ"))/1000).drop(*["UMREZ","UMREN"])

# Fetching Cum3 conversion value from marm against each material number
cum_mard_df = kg_conv_mard_df.alias("l_df").join(marm_cum.alias("r_df"),on=[col("l_df.MATNR") == col("r_df.MATNR")],how='left').select([col("l_df."+cols) for cols in kg_conv_mard_df.columns] + [col("UMREZ"),col("UMREN")])

# calculaitng cum3 conversion value
cum_conv_mard_df = cum_mard_df.withColumn("CUM_CONV_VAL",(col("UMREN")/col("UMREZ"))).drop(*["UMREZ","UMREN"])

# calculating mt conv value for materials which has base UoM is MT or TO
mard_fin_df = cum_conv_mard_df.withColumn("MT_CONV_VAL",when(col("UNIT_OF_MEASURE").isin(["MT","TO"]),lit(1)).otherwise(col("MT_CONV_VAL")))

# calculating MT conv value for materials which contains value in marm
mard_fin_df = mard_fin_df.withColumn("MT_CONV_VAL",when(((col("CUM_CONV_VAL").isNotNull()) | (col("CUM_CONV_VAL") > 0)),lit(None)).otherwise(col("MT_CONV_VAL")))

# calculating cum conv value for blocks and for non blocks product line making it as null
mard_fin_df = mard_fin_df.withColumn("CUM_CONV_VAL",when(col("PRODUCT_LINE") != "BLOCKS",lit(None)).otherwise(col("CUM_CONV_VAL")))

# selecting required columns
mard_fin_df = mard_fin_df.select(col("MATNR"),col("WERKS"),col("LGORT"),col("LABST"),col("INSME"),col("SPEME"),lit(0.0).cast("decimal(14,3)").alias("IN_TRANSIT_QTY"),col("MATERIAL_TYPE"),col("UNIT_OF_MEASURE"),col("PRODUCT_LINE"),col("MT_CONV_VAL"),col("CUM_CONV_VAL"))

# COMMAND ----------

# Implementing same above logic for the data coming from MARC table to find out intransit qty
pline_marc_df = marc.alias("md").join(dim_prod.alias("dp"),on = [col("md.matnr") == col("dp.material_number")],how='left').select([col("md." + cols) for cols in marc.columns]+[col("MATERIAL_TYPE"),col("UNIT_OF_MEASURE"),col("PRODUCT_LINE")])

conv_marc_df = pline_marc_df.alias("l_df").join(marm_kg.alias("r_df"),on=[col("l_df.MATNR") == col("r_df.MATNR")],how='left').select([col("l_df."+cols) for cols in pline_marc_df.columns] + [col("UMREZ"),col("UMREN")])

kg_conv_marc_df = conv_marc_df.withColumn("MT_CONV_VAL",(col("UMREN")/col("UMREZ"))/1000).drop(*["UMREZ","UMREN"])

cum_marc_df = kg_conv_marc_df.alias("l_df").join(marm_cum.alias("r_df"),on=[col("l_df.MATNR") == col("r_df.MATNR")],how='left').select([col("l_df."+cols) for cols in kg_conv_marc_df.columns] + [col("UMREZ"),col("UMREN")])

cum_conv_marc_df = cum_marc_df.withColumn("CUM_CONV_VAL",(col("UMREN")/col("UMREZ"))).drop(*["UMREZ","UMREN"])

marc_fin_df = cum_conv_marc_df.withColumn("MT_CONV_VAL",when(col("UNIT_OF_MEASURE").isin(["MT","TO"]),lit(1)).otherwise(col("MT_CONV_VAL")))

marc_fin_df = marc_fin_df.withColumn("MT_CONV_VAL",when(((col("CUM_CONV_VAL").isNotNull()) | (col("CUM_CONV_VAL") > 0)),lit(None)).otherwise(col("MT_CONV_VAL")))

marc_fin_df = marc_fin_df.withColumn("CUM_CONV_VAL",when(col("PRODUCT_LINE") != "BLOCKS",lit(None)).otherwise(col("CUM_CONV_VAL")))

marc_fin_df = marc_fin_df.select(col("MATNR"),col("WERKS"),lit(None).cast("string").alias("LGORT"),lit(0.0).cast("decimal(13,3)").alias("LABST"),lit(0.0).cast("decimal(13,3)").alias("INSME"),lit(0.0).cast("decimal(13,3)").alias("SPEME"),(col("UMLMC")+col("TRAME")).alias("IN_TRANSIT_QTY"),col("MATERIAL_TYPE"),col("UNIT_OF_MEASURE"),col("PRODUCT_LINE"),col("MT_CONV_VAL"),col("CUM_CONV_VAL"))

# COMMAND ----------

# filtering records which doesn't have qty
df1 = mard_fin_df.where("(labst+insme+speme+in_transit_qty)>0")

df2 = marc_fin_df.where("IN_TRANSIT_QTY>0")

# Union operation of mard and marc dataframes
df3 = df1.union(df2)

# adding snapshot date column to dataframe
df4 = df3.withColumn("SNAPSHOT_DATE",lit(datetime.strptime(var_date, '%Y-%m-%d').date()))

# Fetching conversion value from ZSBU3 and converting values
df5 = df4.alias('ls').join(zsbu3,[trim(df4["matnr"]) == trim(zsbu3["matnr"]),
                                                trim(df4["werks"]) == trim(zsbu3["werks"])],
                                                how='left').select(
  [col("ls." + cols) for cols in df4.columns] + [(col("STD_WT")/1000).alias("ZSBU3_STD_WT"),col("VALID_FROM"),col("VALID_TO")]
).where((col("SNAPSHOT_DATE").between(col("VALID_FROM"),col("VALID_TO"))) | ((col("VALID_FROM").isNull()) & (col("VALID_TO").isNull()))).drop("VALID_FROM","VALID_TO")

df6 = df5.withColumn("ZSBU3_STD_WT",when((((col("UNIT_OF_MEASURE") == "EA") & (col("MATERIAL_TYPE") == "ZHLB"))|(col("Product_line") == "PIPES & FITTINGS")),col("ZSBU3_STD_WT")).otherwise(lit(None)))

df7 = df6.withColumn("MT_CONV_VAL",when(col("ZSBU3_STD_WT").isNotNull(),col("ZSBU3_STD_WT")).otherwise(col("MT_CONV_VAL"))).drop(*["MATERIAL_TYPE","UNIT_OF_MEASURE","PRODUCT_LINE","ZSBU3_STD_WT"])

# COMMAND ----------

# Fetching price values from mbew
mbew_df = df7.alias('df').join(mbew.alias('mb'), on=[col('df.matnr') == col('mb.MATNR'), col('df.werks') == col('mb.BWKEY')], how = 'left').select([col("df."+cols) for cols in df7.columns] + [col("VPRSV"),col("VERPR"),col("STPRS"),col("BWTAR")])

# COMMAND ----------

# calculating price based on standard price or variable price
price_df = mbew_df.withColumn("PRICE",when(col("VPRSV") == 'S',col("STPRS")).when(col("VPRSV") == 'V',col("VERPR")))

# COMMAND ----------

# removing multiple records coming from MBEW table
chose_group = ['werks','matnr']
count_df = price_df.groupBy(chose_group).count().filter(col('count') > 1)
count_df_renamed = count_df.withColumnRenamed('werks', 'werks').withColumnRenamed('matnr', 'matnr')
duplicates_df = price_df.join(count_df_renamed, chose_group).filter(price_df.BWTAR.isNotNull()).drop(col('count'))
duplicates_df = duplicates_df.select([col("MATNR"),col("WERKS"),col("LGORT"),col("LABST"),col("INSME"),col("SPEME"),col("IN_TRANSIT_QTY"),col("MT_CONV_VAL"),col("CUM_CONV_VAL"),col("SNAPSHOT_DATE"),col("VPRSV"),col("VERPR"),col("STPRS"),col("BWTAR"),col("PRICE")])

# COMMAND ----------

df8 = price_df.subtract(duplicates_df)
df9 = df8.drop('VPRSV').drop('VERPR').drop('STPRS').drop('BWTAR')
df10 = df9.withColumn("SNAPSHOT_DATE",lit(datetime.strptime(var_date, '%Y-%m-%d').date()))
df11 = df10.withColumn("LAST_EXECUTED_TIME", lit(last_processed_time_str))

# COMMAND ----------

# selecting columns required for final dataframe
final_df = df11.selectExpr("MATNR as MATERIAL_ID","WERKS as PLANT_ID","LGORT as STORAGE_LOCATION","LABST as UNREST_QTY","INSME as QUALITY_QTY","SPEME as BLOCKED_QTY","IN_TRANSIT_QTY","MT_CONV_VAL as MT_CONV_VALUE","CUM_CONV_VAL as CUM_CONV_VALUE","SNAPSHOT_DATE","PRICE","LAST_EXECUTED_TIME")

# COMMAND ----------

# writing data to processed location before generating surrogate key
final_df.write.parquet(processed_location+'MARD_INVENTORY', mode='overwrite')

# COMMAND ----------

# --------------------SURROGATE IMPLEMENTATION------------------------

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
                        " THEN -2 ELSE ifnull({dim_table}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} on {fact_table}.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)    
  select_condition = select_condition[:-2]
  query = """select 
  STORAGE_LOCATION,
  UNREST_QTY,
QUALITY_QTY,
BLOCKED_QTY,
IN_TRANSIT_QTY,
MT_CONV_VALUE,
CUM_CONV_VALUE,
PRICE,
LAST_EXECUTED_TIME
,{select_condition}  from {fact_name} {join_condition}""".format(
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

# Generating surrogate key
final = surrogate_mapping_hil(csv_data,table_name,"MARD_INVENTORY",processed_db_name)
final.createOrReplaceTempView("d")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS pr1

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# fetching existing data present in pro layer table
tab = processed_db_name + "." + table_name
pro_df = spark.read.table(tab).select( "STORAGE_LOCATION","UNREST_QTY","QUALITY_QTY","BLOCKED_QTY","IN_TRANSIT_QTY","MT_CONV_VALUE","CUM_CONV_VALUE","PRICE","LAST_EXECUTED_TIME","MATERIAL_KEY","PLANT_KEY","SNAPSHOT_DATE_KEY").write.saveAsTable("pr1")

# COMMAND ----------

full_df = spark.sql("select A.STORAGE_LOCATION,A.UNREST_QTY,A.QUALITY_QTY,A.BLOCKED_QTY,A.IN_TRANSIT_QTY,A.MT_CONV_VALUE,A.CUM_CONV_VALUE,A.PRICE,A.LAST_EXECUTED_TIME,A.MATERIAL_KEY,A.PLANT_KEY,A.SNAPSHOT_DATE_KEY from pr1 A left outer join (select distinct(SNAPSHOT_DATE_KEY)  as SNAPSHOT_DATE_KEY from d) T on A.SNAPSHOT_DATE_KEY=T.SNAPSHOT_DATE_KEY where T.SNAPSHOT_DATE_KEY is null")

# COMMAND ----------

# Union of incremental data with existing data
union_df = full_df.union(final)

# COMMAND ----------

# writing final data to processed location
union_df.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# Inserting log record to LAST_EXECUTION_DETAILS table 
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
