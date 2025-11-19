# Databricks notebook source
# DBTITLE 1,Import Session
# import statements
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr , when
from ProcessMetadataUtility import ProcessMetadataUtility
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
spark.conf.set("spark.sql.broadcastTimeout",  36000)

# COMMAND ----------

# class definition
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
table_name = "FACT_QUALITY_PLANT_REJECTION"
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
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# table_name = "FACT_QUALITY_PLANT_REJECTION"

# COMMAND ----------

# reading data from curated layer
MSEG = spark.sql("select * FROM {}.{}".format(curated_db_name, "MSEG")).select("MATNR", "MANDT","AUFNR","WERKS","LGORT","BWART","MBLNR", "MJAHR", "ZEILE", "ERFMG" ,"ERFME")

# COMMAND ----------

# reading data from curated layer
AUFK = spark.sql("select * from {}.{}".format(curated_db_name,"AUFK"))

# COMMAND ----------

# reading data from curated layer
MARM = spark.sql("select * FROM {}.{}".format(curated_db_name, "MARM")).where(col("MEINH").isin(["KG"]))

# COMMAND ----------

# joining mseg with aufk
df1 = MSEG.alias('mseg').join(AUFK.alias('aufk'), on=[
                                               col("mseg.AUFNR") == col("AUFK.AUFNR")],
                            how='left').select([col("mseg."+cols) for cols in MSEG.columns] + [col("ERDAT")]).where((col("BWART").isin(["531", "532"])) & (col("LGORT").isin(["0089","0050"])) & col("WERKS").isin (["2019","2029","2021"])).drop("MANDT")

# COMMAND ----------

# fetching marm conversion values
df2 = df1.join(MARM, on=['MATNR'], how='inner').select([col(cols) for cols in df1.columns] +[col("UMREZ"),col("UMREN")])

# COMMAND ----------

# selecting req columns and chaning names
df3 = df2.alias("ms").select([col("ms.MATNR").alias("MATERIAL_NO"),col("AUFNR").alias("PRODUCTION_ORDER"),col("ms.WERKS").alias("PLANT_ID"),col("LGORT").alias("STORAGE_LOC"),col("BWART").alias("MOVEMENT_TYPE"),col("ms.MBLNR"),col("ms.MJAHR"),col("ms.ZEILE"),col("ms.ERFMG").alias("QUANTITY"),col("ms.ERFME"),col("ms.ERDAT").alias("DATE"),col("ms.UMREZ"),col("UMREN")])

# COMMAND ----------

# selecting req columns and calculating MT conv values
df4 = df3.selectExpr("MATERIAL_NO", "PRODUCTION_ORDER" ,"PLANT_ID", "STORAGE_LOC","MOVEMENT_TYPE","MBLNR","MJAHR","ZEILE","QUANTITY","ERFME","DATE", "UMREZ", "UMREN","(UMREN/UMREZ) as UNIT_WEIGHTS", "(((UMREN/UMREZ) * QUANTITY)/1000) as WEIGHTS_MT")

# COMMAND ----------

# deriving quantity values
df5 = df4.withColumn("USABLE_REJECTION_QTY_MT", when(((col("MOVEMENT_TYPE").isin(["531"])) & (col("STORAGE_LOC").isin(["0050"]))), (col("WEIGHTS_MT")))
                     .when(((col("MOVEMENT_TYPE").isin(["532"])) & (col("STORAGE_LOC").isin(["0050"]))),-(col("WEIGHTS_MT"))))

df6 = df5.withColumn("NON_USABLE_REJECTION_QTY_MT",when(((col("MOVEMENT_TYPE").isin(["531"])) & (col("STORAGE_LOC").isin(["0089"]))), (col("WEIGHTS_MT")))
            .when(((col("MOVEMENT_TYPE").isin(["532"])) & (col("STORAGE_LOC").isin(["0089"]))),-(col("WEIGHTS_MT"))))
final_df = df6.drop("UMREZ","UMREN")

# COMMAND ----------

# writing data to processed layer location before generating surrogate keys
final_df.write.parquet(processed_location+'PLANT_REJECTION', mode='overwrite')

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

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
  
  query = """select 
PRODUCTION_ORDER,
MBLNR as MATERIAL_DOC,
MJAHR as MATERIAL_DOC_YEAR,
ZEILE as DOC_ITEM,
MOVEMENT_TYPE,
QUANTITY,
ERFME as UNIT_OF_ENTRY,
UNIT_WEIGHTS,
WEIGHTS_MT,
USABLE_REJECTION_QTY_MT,
NON_USABLE_REJECTION_QTY_MT
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

# creating surrogate key
d = surrogate_mapping_hil(csv_data,table_name,"PLANT_REJECTION",processed_db_name)

# COMMAND ----------

# writing data to processed layer location after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+"FACT_QUALITY_PLANT_REJECTION")

# COMMAND ----------

# insert log data into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
