# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad
from pyspark.sql import functions as sf
from pyspark.sql.types import *
from ProcessMetadataUtility import ProcessMetadataUtility
pmu = ProcessMetadataUtility()

# COMMAND ----------

# enabling cross join and increasing broadcast timeout to 36000 seconds
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set("spark.sql.broadcastTimeout",36000)

# COMMAND ----------

# code to fetch the parameters from ADF
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
table_name = "FACT_INSPECTION_CHARS_RESULT"
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
# table_name = "FACT_INSPECTION_CHARS_RESULT"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# fetching surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
QALS = spark.sql("select PRUEFLOS,AUFNR,ART,WERK,SELMATNR,ENSTEHDAT,HERKUNFT from {}.{} where trim(ART)='04'".format(curated_db_name, "QALS"))
QAMV = spark.sql("Select PRUEFLOS,VORGLFNR,MERKNR,VERWMERKM,PMTVERSION,MASSEINHSW,TOLERANZOB,TOLERANZUN,SOLLWERT from {}.{}".format(curated_db_name,"QAMV"))
QAMR = spark.sql("Select PRUEFLOS,VORGLFNR,MERKNR,MITTELWERT from {}.{}".format(curated_db_name, "QAMR"))

# COMMAND ----------

# joining qals and qamv
df1 = QALS.alias('qals').join(QAMV.alias('qamv'), on=[col('qals.PRUEFLOS') == col('qamv.PRUEFLOS')],how='inner').select([col("qals.PRUEFLOS").alias("INSPECTION_LOT")] + [col("qals.ENSTEHDAT").alias("LOT_CREATED_DATE")] + [col("qals.WERK").alias("PLANT_ID")] + [col("qals.SELMATNR").alias("MATERIAL_ID")]+[col("qals.AUFNR").alias("PRODUCTION_ORDER")]+[col("qamv.VORGLFNR").alias('NODE_NUMBER')] + [col("qamv.MERKNR").alias('CHARACTERISTIC')] + [col("qamv.VERWMERKM").alias('MASTER_INSP_CHAR')]+ [col("PMTVERSION").alias("VERSION")] + [col("qamv.MASSEINHSW").alias('UNIT_OF_MEASUREMENT')] + [col("qamv.TOLERANZOB").alias('UPPER_LIMIT')] + [col("qamv.TOLERANZUN").alias('LOWER_LIMIT')] + [col("qamv.SOLLWERT").alias('TARGET_VALUE')])

# COMMAND ----------

# joining with qamr to fetch the actual characteristic value
df2 = df1.alias('df').join(QAMR.alias('qamr'),on=[(col('df.INSPECTION_LOT') == col('qamr.PRUEFLOS')) & (col('df.NODE_NUMBER') == col('qamr.VORGLFNR')) & (col('df.CHARACTERISTIC') == col('qamr.MERKNR'))],how='inner').select(["df." + cols for cols in df1.columns] + [col("qamr.MITTELWERT").alias('ACTUAL_VALUE')]).drop("NODE_NUMBER").drop("CHARACTERISTIC")

# COMMAND ----------

# casting value fields
df2 = df2.withColumn("UPPER_LIMIT",df2['UPPER_LIMIT'].cast(DecimalType(13,3))).withColumn("LOWER_LIMIT",df2['LOWER_LIMIT'].cast(DecimalType(13,3))).withColumn("TARGET_VALUE",df2['TARGET_VALUE'].cast(DecimalType(13,3))).withColumn("ACTUAL_VALUE",df2['ACTUAL_VALUE'].cast(DecimalType(13,3)))

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
df2.write.parquet(processed_location+'INSPECTION_CHARS_RESULT', mode='overwrite')

# COMMAND ----------

#--------------------SURROGATE IMPLEMENTATION------------------------
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition_sl="\n left join {pro}.DIM_INSPECTION_CHARS on".format(pro=processed_db_name)
  join_condition=""
  count=0
  
  df2.createOrReplaceTempView("{}".format(fact_name))
  
  for row_dim_fact_mapping in dim_fact_mapping:
    
    fact_table = row_dim_fact_mapping["fact_table"]
    dim_table = row_dim_fact_mapping["dim_table"]
    fact_surrogate = row_dim_fact_mapping["fact_surrogate"]
    dim_surrogate = row_dim_fact_mapping["dim_surrogate"]
    fact_column = row_dim_fact_mapping["fact_column"]
    dim_column = row_dim_fact_mapping["dim_column"]
    
    spark.sql("refresh table {processed_db_name}.{dim_table}".format(processed_db_name=processed_db_name,
                                                                   dim_table=dim_table))
  
    if( (fact_table_name==fact_table or fact_table_name.lower()==fact_table) and (dim_table!="DIM_INSPECTION_CHARS") ):
      
      count += 1
      tmp_unmapped_str = "CASE WHEN "
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull({dim_table}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} on {fact_table}.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
    
    if((fact_table_name==fact_table or fact_table_name.lower()==fact_table) and dim_table=='DIM_INSPECTION_CHARS'):
      count +=1
      fact_column_temp = fact_column.split("|")
      dim_column_temp = dim_column.split("|")
      
    #for dim_storage location  
      for frow,drow in zip(fact_column_temp,dim_column_temp):
  
        join_condition_sl += """\n {fact_name}.{frow} = {dim_table}.{drow} AND""".format(fact_name=fact_name,dim_table=dim_table,frow=frow,drow=drow)
      join_condition_sl = join_condition_sl[:-3]
#   print(join_condition_sl)   
  select_condition = select_condition[:-2]   
  join_condition += join_condition_sl
  
  select_condition = select_condition + """
    ,DIM_INSPECTION_CHARS.INSP_CHAR_KEY AS INSP_CHAR_KEY
    """    
  query = """select 
 INSPECTION_LOT,
PRODUCTION_ORDER,
UNIT_OF_MEASUREMENT,
UPPER_LIMIT,
LOWER_LIMIT,
TARGET_VALUE,
ACTUAL_VALUE
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
d = surrogate_mapping_hil(csv_data,table_name,"INSPECTION_CHARS_RESULT", processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location + table_name)

# COMMAND ----------

# inserting log record to last execution table in SQL
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
