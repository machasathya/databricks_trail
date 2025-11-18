# Databricks notebook source
# Import statements
import time
from datetime import datetime, date, timedelta
from dateutil import tz
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility

# COMMAND ----------

# code to extract current date 
now = datetime.now()
day_n = now.day
now_hour = now.time().hour
ist_zone = datetime.now() + timedelta(hours=5.5)
var_date = (ist_zone - timedelta(days=1)).strftime("%Y-%m-%d")

# COMMAND ----------

# enabling cross join and increasing broadcasttime out to 36000sec
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set("spark.sql.broadcastTimeout",36000)

# COMMAND ----------

# Initialising object for processmetadatautility class
pmu = ProcessMetadataUtility()

# COMMAND ----------

# parameters that fetches data from ADF
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
table_name = "FACT_LOGISTICS_INVENTORY"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# Variables used to run the notebook manually
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# processed_db_name = "pro_prod"
# processed_location = "mnt/bi_datalake/prod/pro/"
# curated_db_name = "cur_prod"
# processed_db_name = "pro_prod"
# table_name = "FACT_LOGISTICS_INVENTORY"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# curated_db_name = "cur_prod"

# COMMAND ----------

# Extracting surrogate meta from SQL
pmu = ProcessMetadataUtility()
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value= table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer from the required tables
MARM = spark.sql("select * from {db}.MARM ".format(db=curated_db_name))
T001W = spark.sql("select * from {db}.t001w ".format(db=curated_db_name))
MARA = spark.sql("select MATNR,MATKL,MTART,MEINS,ERSDA from {db}.MARA".format(db=curated_db_name))
MARC = spark.sql("select MATNR,WERKS,EKGRP,UMLMC,TRAME from {db}.MARC ".format(db=curated_db_name))
MARD = spark.sql("select MATNR,WERKS,EINME,SPEME,INSME,LABST,RETME,UMLME,LGORT from {db}.MARD ".format(db=curated_db_name))
MBEW = spark.sql("Select BWKEY,MATNR,VERPR,STPRS,VPRSV,LFGJA,LFMON,BWTAR from {}.{}".format(curated_db_name,"MBEW"))

# COMMAND ----------

# extracting material type,group and base UoM
mast_df = MARA.join(MARC, on=['MATNR'], how='inner').select([col(cols) for cols in MARC.columns] +[col("MATKL"), col("MTART"),col("MEINS")])

# COMMAND ----------

# fetching purchase group, sales org and name of the plant
df2 = mast_df.join(T001W, on=['WERKS'], how='left').select([col(cols) for cols in mast_df.columns] +[col("NAME1"), col("EKORG"), col("VKORG")])

# COMMAND ----------

# Generating is_plant flag value
df_new = df2.withColumn("IS_PLANT",when(col("WERKS") == 4404, 1).when(col("WERKS").substr(0,1).isin([2]) & col('EKORG').isNotNull() & col('VKORG').isNotNull(), 1).when(col("WERKS").substr(0,1).isin([ 3, 4, 5, 6, 7, 9 ]), 0).otherwise(3))

# COMMAND ----------

# Fetching Kg conversion value from MARM table
df = df_new.alias('mk').join(MARM.alias('ms'), on=[
                                               col('mk.MATNR') == col('ms.MATNR')],
                            how='inner').select([col("mk."+cols) for cols in df_new.columns] + [col("MEINH"),col("UMREZ"),col("UMREN")]).where(col("MEINH").isin(["KG"]))

# COMMAND ----------

# changinf column names
df3 = df.alias('dd').join(MARD.alias('md'), on=[
                                               col('dd.MATNR') == col('md.MATNR'),col('dd.WERKS') == col('md.WERKS')], how='left').select([col("dd.MATNR").alias("matl_key"), col("dd.WERKS").alias("plant_key"), col("dd.EKGRP").alias("purch_grp"), col("dd.MATKL").alias("material_group_key"), col("dd.MTART").alias("matl_type"), col("dd.MEINS").alias("unit_measure"), col("dd.NAME1").alias("name1"), col("md.SPEME").alias("blkd_qty"),col("md.INSME").alias("qlty_qty"),col("md.RETME").alias("rtrns_qty"),col("md.EINME").alias("rest_qty"),col("md.LABST").alias("unrest_qty"),col("md.UMLME").alias("trsn_qty"), col("md.LGORT").alias("storage_loc"), col("dd.IS_PLANT"), col("dd.UMREN"), col("dd.UMREZ"), col("dd.MEINH")])

# COMMAND ----------

# Selecting required columns and calculating total qty
final = df3.selectExpr("matl_key","plant_key","blkd_qty","qlty_qty","rtrns_qty","rest_qty","trsn_qty","unrest_qty","purch_grp","matl_type","material_group_key","unit_measure","name1","storage_loc","IS_PLANT","UMREZ","UMREN","(blkd_qty+qlty_qty+rtrns_qty+rest_qty+unrest_qty+trsn_qty) as total_qty")

# selecting columns and calculation mt conv value
final_1 = final.selectExpr("matl_key","plant_key","purch_grp","matl_type","unit_measure","name1","blkd_qty","qlty_qty","rtrns_qty","rest_qty","unrest_qty","trsn_qty","IS_PLANT","storage_loc","total_qty","(UMREN/UMREZ) as unit_weights", "((UMREN/UMREZ) * total_qty) as total_unit_weights", "(((UMREN/UMREZ) * total_qty)/1000) as TOTAL_MT", "(((UMREN/UMREZ) * qlty_qty)/1000) as qlty_qty_mt", "(((UMREN/UMREZ) * unrest_qty)/1000) as unrest_qty_mt")

# selecting required columns
final_2 = final_1.selectExpr("matl_key","plant_key","purch_grp","matl_type","unit_measure","name1","blkd_qty","qlty_qty","rtrns_qty","rest_qty","unrest_qty","trsn_qty","IS_PLANT","storage_loc","TOTAL_MT","qlty_qty_mt","unrest_qty_mt")

# COMMAND ----------

# calculating reclaimable qty for plants and depots based on SLoc
df4  = final_2.withColumn("reclaimable_qty", when((col("storage_loc").isin(["81","8101","8103","8112","CA01","CA03","CA12","CB01","CB03","CB12","CC01","CC12","CD01","CD12","CE01","CE12","CF01","CF12","CG01","CG12","CH01","CH12","CZ01","CZ03","CZ12","8104","8105","8109","8111","CA04","CA05","CA09","CA11","CB04","CB05","CB09","CB11","CC03","CC04","CC05","CC09","CC11","CD03","CD04","CD05","CD09","CD11","CE03","CE04","CE05","CE09","CE11","CF03","CF04","CF05","CF09","CF11","CG03","CG04","CG05","CG09","CG11","CH03","CH04","CH05","CH09","CH11","CZ04","CZ05","CZ09","CZ11","CB04","CB05","CB09","CB11","CD03","CD04","CD05","CD09","CD11","CF03","CF04","CF05","CF09","CF11","CH03","CH04","CH05","CH09","CH11","62","8106","8120","CA06","CA20","CB06","CB20","CC06","CC20","CD06","CD20","CE06","CE20","CF06","CF20","CG06","CG20","CH06","CH20","81B4","BA04","BB04","BC04","BD04","BE04","BF04","BG04","BH04","BI04","BJ04","BZ04","CI01","CI03","CI04","CI05","CI09","CI11","CI12","CJ01","CJ03","CJ04","CJ05","CJ09","CJ11","CJ12","CZ06","CZ20"])) & (col("IS_PLANT").isin([0])) , col("unrest_qty_mt")).when(col("IS_PLANT").isin([1]) & (col("storage_loc").isin(["0081"])), col("unrest_qty_mt")))

# COMMAND ----------

# calculating good qty for plants and depots based on SLoc
df5  = df4.withColumn("good_qty", when((col("storage_loc").isin(["2001","2003","2004","2005","2009","2012","2001","2003","2004","2005","2009","2012"])) & (col("IS_PLANT").isin([0])) , col("unrest_qty_mt")).when(col("IS_PLANT").isin([1]) & (col("storage_loc").isin(["0061"])), col("unrest_qty_mt")))

# COMMAND ----------

# calculating curing stock
df6  = df5.withColumn("stock_curing", when((col("storage_loc").isin(["0061"])) , col("qlty_qty_mt")))

# COMMAND ----------

# doing union of marc and mard final dataframes
marc_df = df
marc_df1 = marc_df.select(col("MATNR").alias("matl_key"),col("WERKS").alias("plant_key"),col("EKGRP").alias("purch_grp"),col("MTART").alias("matl_type"),col("MEINS").alias("unit_measure"),col("NAME1").alias("name1"),lit(0.0).cast("decimal(13,3)").alias("blkd_qty"),lit(0.0).cast("decimal(13,3)").alias("qlty_qty"),lit(0.0).cast("decimal(13,3)").alias("rtrns_qty"),lit(0.0).cast("decimal(13,3)").alias("rest_qty"),lit(0.0).cast("decimal(13,3)").alias("unrest_qty"),(col("UMLMC")+col("TRAME")).cast("decimal(13,3)").alias("trsn_qty"),"IS_PLANT",lit(None).cast("string").alias("storage_loc"),lit(0.0).cast("decimal(31,14)").alias("TOTAL_MT"),lit(0.0).cast("decimal(30,14)").alias("qlty_qty_mt"),(((col("UMREN")/col("UMREZ"))*(col("UMLMC")+col("TRAME")))/1000).alias("unrest_qty_mt"),lit(0.0).cast("decimal(30,14)").alias("reclaimable_qty"),lit(0.0).cast("decimal(30,14)").alias("good_qty"),lit(0.0).cast("decimal(30,14)").alias("stock_curing"))

df6_extended = df6.union(marc_df1)

df7  = df6_extended.withColumn("in_transit_qty", when((col("storage_loc").isNull()), col("unrest_qty_mt").cast("decimal(30,14)")).otherwise(0)).withColumn("trsn_qty",col("trsn_qty").cast("decimal(30,14)"))

# COMMAND ----------

# Replacing Null's with 0's
null = df7.fillna( { 'reclaimable_qty':0, 'stock_curing':0, 'good_qty':0 } )

# COMMAND ----------

# Selecting final required columns
null_1 = null.selectExpr("matl_key","plant_key","purch_grp","matl_type","unit_measure","name1","blkd_qty","qlty_qty","rtrns_qty","rest_qty","unrest_qty","trsn_qty","IS_PLANT","storage_loc","TOTAL_MT","reclaimable_qty","good_qty","stock_curing","in_transit_qty","(TOTAL_MT-(good_qty+reclaimable_qty+stock_curing)) as destroyable_qty_plnt")

final_4 = null_1.selectExpr("matl_key","plant_key","blkd_qty","qlty_qty","rtrns_qty","rest_qty","unrest_qty","trsn_qty","IS_PLANT","storage_loc","TOTAL_MT","in_transit_qty","reclaimable_qty","good_qty","stock_curing","destroyable_qty_plnt")

# COMMAND ----------

# calculating destroyable qty for plant and depot based on SLoc
df8  = final_4.withColumn("destroyable_qty", when((col("storage_loc").isin(["CX06","CX01","CX03","CX12","CX04","CX05","CX09","CX11","CZ04","CZ05","BX04","CX20"])) & (col("IS_PLANT").isin([0])) , col("destroyable_qty_plnt")).when(col("IS_PLANT").isin([1]), col("destroyable_qty_plnt")))

# COMMAND ----------

# replacing Null's with 0's
null_3 = df8.fillna( { 'destroyable_qty':0 } ).drop("destroyable_qty_plnt")

# COMMAND ----------

# adding stock taking date
timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
timestamp
done = null_3.withColumn("stock_taking_date",lit(datetime.strptime(var_date, '%Y-%m-%d').date())).drop("IS_PLANT").where('TOTAL_MT<>0 OR in_transit_qty <> 0 ')

# COMMAND ----------

# Adding last executed time
done_1=done.withColumn("last_executed_time", F.lit(last_processed_time_str))

# COMMAND ----------

# fetching price value from mbew table
mbew_df = done_1.alias('df').join(MBEW.alias('mb'), on=[col('df.matl_key') == col('mb.MATNR'), col('df.plant_key') == col('mb.BWKEY')], how = 'left').select([col("df."+cols) for cols in done_1.columns] + [col("VPRSV"),col("VERPR"),col("STPRS"),col("BWTAR")])

# COMMAND ----------

# creating price column based on standard price or variable price
price_df = mbew_df.withColumn("PRICE",when(col("VPRSV") == 'S',col("STPRS")).when(col("VPRSV") == 'V',col("VERPR")))

# COMMAND ----------

# finding duplicates
chose_group = ['plant_key','matl_key']
count_df = price_df.groupBy(chose_group).count().filter(col('count') > 1)
count_df_renamed = count_df.withColumnRenamed('plant_key', 'plant_key').withColumnRenamed('matl_key', 'matl_key')
duplicates_df = price_df.join(count_df_renamed, chose_group).filter(price_df.BWTAR.isNotNull()).drop(col('count'))
duplicates_df = duplicates_df.select([col("matl_key"),col("plant_key"),col("blkd_qty"),col("qlty_qty"),col("rtrns_qty"),col("rest_qty"),col("unrest_qty"),col("trsn_qty"),col("storage_loc"),col("TOTAL_MT"),col("in_transit_qty"),col("reclaimable_qty"),col("good_qty"),col("stock_curing"),col("destroyable_qty"),col("stock_taking_date"),col("last_executed_time"),col("VPRSV"),col("VERPR"),col("STPRS"),col("BWTAR"),col("PRICE")])

# COMMAND ----------

# Removing duplicates
final = price_df.subtract(duplicates_df)
final = final.drop('VPRSV').drop('VERPR').drop('STPRS').drop('BWTAR')

# COMMAND ----------

# calculating price values for various qty values
unrestricted_df = final.withColumn("UNRESTRICTED_VALUE",lit(col('unrest_qty') * col('PRICE')))
quality_df = unrestricted_df.withColumn("QUALITY_VALUE",lit(col('qlty_qty') * col('PRICE')))
blocked_df = quality_df.withColumn("BLOCKED_VALUE",lit(col('blkd_qty') * col('PRICE')))

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
blocked_df.write.parquet(processed_location+'INVENTORY', mode='overwrite')

# COMMAND ----------

#--------------------SURROGATE IMPLEMENTATION------------------------
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition_sl="\n left join {pro}.DIM_STORAGE_LOCATION on".format(pro=processed_db_name)
  join_condition=""
  count=0
  
  blocked_df.createOrReplaceTempView("{}".format(fact_name))
  
  for row_dim_fact_mapping in dim_fact_mapping:
    
    fact_table = row_dim_fact_mapping["fact_table"]
    dim_table = row_dim_fact_mapping["dim_table"]
    fact_surrogate = row_dim_fact_mapping["fact_surrogate"]
    dim_surrogate = row_dim_fact_mapping["dim_surrogate"]
    fact_column = row_dim_fact_mapping["fact_column"]
    dim_column = row_dim_fact_mapping["dim_column"]
    
    spark.sql("refresh table {processed_db_name}.{dim_table}".format(processed_db_name=processed_db_name,
                                                                   dim_table=dim_table))
  
    if( (fact_table_name==fact_table or fact_table_name.lower()==fact_table) and (dim_table!="DIM_STORAGE_LOCATION") ):
      
      count += 1
      tmp_unmapped_str = "CASE WHEN "
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull({dim_table}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} on {fact_table}.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
    
    if((fact_table_name==fact_table or fact_table_name.lower()==fact_table) and dim_table=='DIM_STORAGE_LOCATION'):
      count +=1
      fact_column_temp = fact_column.split("|")
      dim_column_temp = dim_column.split("|")
      
    #for dim_storage location  
      for frow,drow in zip(fact_column_temp,dim_column_temp):
  
        join_condition_sl += """\n {fact_name}.{frow} = {dim_table}.{drow} AND""".format(fact_name=fact_name,dim_table=dim_table,frow=frow,drow=drow)
      join_condition_sl = join_condition_sl[:-3]
      
  select_condition = select_condition[:-2]   
  join_condition += join_condition_sl
  
  select_condition = select_condition + """
    ,DIM_STORAGE_LOCATION.STORAGE_LOCATION_KEY AS STORAGE_LOCATION_KEY
    """ 
   
  query = """select 
  blkd_qty as BLKD_QTY
,qlty_qty as QLTY_QT
,rtrns_qty as RTRNS_QTY
,rest_qty as REST_QTY
,unrest_qty as UNREST_QTY
,trsn_qty as TRSN_QTY
,TOTAL_MT
,reclaimable_qty as RECLAIMABLE_QTY
,destroyable_qty as DESTROYABLE_QTY
,in_transit_qty as IN_TRANSIT_QTY
,good_qty as GOOD_QTY
,stock_curing as STOCK_CURING
,stock_taking_date as STOCK_TAKING_DATE
,last_executed_time as LAST_EXECUTED_TIME
,PRICE
,UNRESTRICTED_VALUE
,QUALITY_VALUE
,BLOCKED_VALUE
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

# Generating surrogate keys
d = surrogate_mapping_hil(csv_data, table_name, "INVENTORY", processed_db_name)
d.createOrReplaceTempView("d")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS pr1

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# Extracting current processed layer table data
tab = processed_db_name + "." + table_name
pro_df = spark.read.table(tab).select( "blkd_qty","qlty_qt","rtrns_qty","rest_qty","unrest_qty","trsn_qty","TOTAL_MT","reclaimable_qty","destroyable_qty","in_transit_qty","good_qty","stock_curing","stock_taking_date","PRICE","UNRESTRICTED_VALUE","QUALITY_VALUE","BLOCKED_VALUE","last_executed_time","plant_key","matl_key","SNAPSHOT_DATE_KEY","STORAGE_LOCATION_KEY").write.saveAsTable("pr1")

# COMMAND ----------

# extracting data in the required order
full_df = spark.sql("select A.blkd_qty,A.qlty_qt,A.rtrns_qty,A.rest_qty,A.unrest_qty,A.trsn_qty,A.TOTAL_MT,A.reclaimable_qty,A.destroyable_qty,A.in_transit_qty,A.good_qty,A.stock_curing,A.stock_taking_date,A.last_executed_time,A.PRICE,A.UNRESTRICTED_VALUE,A.QUALITY_VALUE,A.BLOCKED_VALUE,A.plant_key,A.matl_key, A.SNAPSHOT_DATE_KEY, A.STORAGE_LOCATION_KEY from pr1 A left outer join (select distinct(SNAPSHOT_DATE_KEY)  as SNAPSHOT_DATE_KEY from d) T on A.SNAPSHOT_DATE_KEY=T.SNAPSHOT_DATE_KEY where T.SNAPSHOT_DATE_KEY is null")

# COMMAND ----------

# union of incr data with existing data
union_df = full_df.union(d)

# COMMAND ----------

# writing data to processed location after generating surrogate keys
union_df.write.insertInto(processed_db_name + "." + table_name, overwrite=True)

# COMMAND ----------

# inserting log record to last_exectuion_details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'INCREMENTAL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
