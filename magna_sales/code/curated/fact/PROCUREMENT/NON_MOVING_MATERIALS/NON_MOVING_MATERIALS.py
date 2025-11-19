# Databricks notebook source
# import statements
from datetime import datetime, date, timedelta
import time
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
table_name = "FACT_NON_MOVING_MATERIALS"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

#defining run date
now = datetime.now()
day_n = now.day
now_hour = now.time().hour
ist_zone = datetime.now() + timedelta(hours=5.5)
var_date = (ist_zone - timedelta(days=1)).strftime("%Y-%m-%d")

var_date

# COMMAND ----------

# variables to run the data manually
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
# table_name = "FACT_NON_MOVING_MATERIALS"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# class object definition
pmu = ProcessMetadataUtility()

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer 
s032 = spark.sql("select WERKS,LGORT,MATNR,BASME,HWAER,MTART,GSBER,SPART,MBWBEST,WBWBEST,LETZTZUG,LETZTABG,LETZTVER,LETZTBEW,BKLAS from {}.{}".format(curated_db_name,"S032"))

s032_grp = spark.sql("select WERKS,MATNR,sum(MBWBEST),sum(WBWBEST),(sum(WBWBEST)/sum(MBWBEST)) as div_value from {}.{} group by WERKS,MATNR".format(curated_db_name,"S032"))

# COMMAND ----------

# reading conversion data from marm
marm = spark.sql("select * from {}.{} where MEINH = 'KG'".format(curated_db_name,"MARM")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# defining conv values from marm for each material
df1 = s032.alias('l_df').join(marm.alias('r_df'),on=[col('l_df.MATNR') == col('r_df.MATNR')],how = 'left').select([col("l_df." + cols) for cols in s032.columns] + [col("UMREN"),col("UMREZ")])

df2 = df1.alias("l_df").join(s032_grp.alias("r_df"),on = [col("l_df.MATNR") == col("r_df.MATNR"),
                                                          col("l_df.WERKS") == col("r_df.WERKS")],how='left').select([col("l_df." + cols) for cols in df1.columns] + [col("div_value")])

# COMMAND ----------

# calculating quantity in metric tons and and quantity
df3 = df2.withColumn("WEIGHTS_MT",when(col("UMREN").isNotNull(),((col("UMREN")/col("UMREZ"))*col("MBWBEST"))/1000))

df4 = df3.withColumn("WBWBEST",col("MBWBEST")*col("div_value"))

# COMMAND ----------

# selecting required columns
df5 = df4.select(col("WERKS").alias("PLANT_ID"),col("LGORT").alias("STORAGE_LOC"),col("MATNR").alias("MATERIAL_NUMBER"),col("BASME").alias("BASE_UOM"),col("HWAER").alias("CURRENCY"),col("MBWBEST").alias("QUANTITY"),col("WEIGHTS_MT"),col("WBWBEST").alias("VALUE"),col("LETZTZUG").alias("LAST_RECEIPT_DATE"),col("LETZTABG").alias("LAST_GOODS_ISSUE_DATE"),col("LETZTVER").alias("LAST_CONSUMPTION_DATE"),col("LETZTBEW").alias("LAST_GOODS_MOVEMENT_DATE"))

# COMMAND ----------

# adding snapshot date and filtering data on storage location
timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
final_df = df5.withColumn("SNAPSHOT_DATE",lit(datetime.strptime(var_date, '%Y-%m-%d').date()))
final_df = final_df.where("STORAGE_LOC IS NOT NULL")

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'NON_MOVING_MATERIALS', mode='overwrite')

# COMMAND ----------

#--------------------SURROGATE IMPLEMENTATION------------------------
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition_sl="\n left join {pro}.DIM_STORAGE_LOCATION on".format(pro=processed_db_name)
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
  
    if( (fact_table_name==fact_table or fact_table_name.lower()==fact_table) and (dim_table!="DIM_STORAGE_LOCATION") ):
      
      count += 1
      tmp_unmapped_str = "CASE WHEN "
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull(A{count}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table,count=count)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} A{count} on {fact_table}.{fact_column} = A{count}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name,count=count)
    
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
  query = """
  select 
  BASE_UOM,
CURRENCY,
QUANTITY,
WEIGHTS_MT,
VALUE,
LAST_RECEIPT_DATE,
LAST_GOODS_ISSUE_DATE,
LAST_CONSUMPTION_DATE,
LAST_GOODS_MOVEMENT_DATE
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

# creating surrogate keys
d = surrogate_mapping_hil(csv_data,table_name,"NON_MOVING_MATERIALS",processed_db_name)
d.createOrReplaceTempView("d")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS pr1

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# reading existing data from processed layer
tab = processed_db_name + "." + table_name
pro_df = spark.read.table(tab).select( "BASE_UOM","CURRENCY","QUANTITY","WEIGHTS_MT","VALUE","LAST_RECEIPT_DATE","LAST_GOODS_ISSUE_DATE","LAST_CONSUMPTION_DATE","LAST_GOODS_MOVEMENT_DATE","SNAPSHOT_DATE_KEY","PLANT_KEY","MATERIAL_KEY","LAST_RECEIPT_DATE_KEY","LAST_GOODS_ISSUE_DATE_KEY","LAST_CONSUMPTION_DATE_KEY","LAST_GOODS_MOVEMENT_DATE_KEY","STORAGE_LOCATION_KEY").write.saveAsTable("pr1")

# COMMAND ----------

# selecting columns in required order 
full_df = spark.sql("select A.BASE_UOM,A.CURRENCY,A.QUANTITY,A.WEIGHTS_MT,A.VALUE,A.LAST_RECEIPT_DATE,A.LAST_GOODS_ISSUE_DATE,A.LAST_CONSUMPTION_DATE,A.LAST_GOODS_MOVEMENT_DATE,A.SNAPSHOT_DATE_KEY,A.PLANT_KEY,A.MATERIAL_KEY,A.LAST_RECEIPT_DATE_KEY,A.LAST_GOODS_ISSUE_DATE_KEY,A.LAST_CONSUMPTION_DATE_KEY,A.LAST_GOODS_MOVEMENT_DATE_KEY,A.STORAGE_LOCATION_KEY from pr1 A left outer join (select distinct(SNAPSHOT_DATE_KEY)  as SNAPSHOT_DATE_KEY from d) T on A.SNAPSHOT_DATE_KEY=T.SNAPSHOT_DATE_KEY where T.SNAPSHOT_DATE_KEY is null")

# COMMAND ----------

# merging incremental data with existing data
union_df = full_df.union(d)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
union_df.write.insertInto(processed_db_name + "." + table_name, overwrite=True)

# COMMAND ----------

# inert log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
