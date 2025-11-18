# Databricks notebook source
# DBTITLE 1,Import Session
# import statements
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr,when
from ProcessMetadataUtility import ProcessMetadataUtility
spark.conf.set( "spark.sql.crossJoin.enabled","true")
spark.conf.set("spark.sql.broadcastTimeout",36000)

# COMMAND ----------

# code to fetch the data from adf parameters
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
table_name = "FACT_QUALITY_LOADING_BREAKAGES"
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
# table_name = "FACT_QUALITY_LOADING_BREAKAGES"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading data from curated layer
mara = spark.sql("select MATNR,MEINS,BRGEW,VOLUM,SPART from {}.{}".format(curated_db_name,"MARA"))

# COMMAND ----------

# reading data from curated layer
marm = spark.sql("select * FROM {}.{}".format(curated_db_name, "MARM")).where(col("MEINH").isin(["KG"]))

# COMMAND ----------

# reading data from curated layer
qals = spark.sql("select PRUEFLOS,ENSTEHDAT,WERK,ART,HERKUNFT,AUFNR,LOSMENGE,MATNR from {}.{} where HERKUNFT='08'".format(curated_db_name,"QALS"))

# COMMAND ----------

# reading data from curated layer
qmel = spark.sql("select QMNUM,PRUEFLOS from {}.{}".format(curated_db_name,"QMEL"))

# COMMAND ----------

# reading data from curated layer
qmfe = spark.sql("select QMNUM,FENUM,FEGRP,FECOD,ANZFEHLER from {}.{}".format(curated_db_name,"QMFE"))

# COMMAND ----------

# defining defect group, code and quantity
it_final1 = qals.join(qmel,on = [col("qals.PRUEFLOS") == col("qmel.PRUEFLOS")],how='left').select([col("qals." + cols) for cols in qals.columns] + [col("QMNUM")])

it_final2 = it_final1.alias("df1").join(qmfe, on =[col("df1.QMNUM") == col("qmfe.QMNUM")],how='left').select([col("df1." + cols) for cols in it_final1.columns] + [col("FEGRP"),col("FECOD"),col("ANZFEHLER")])

# COMMAND ----------

# joining with mara and fetching quantity
it_mara = it_final2.alias("df1").join(mara.alias('mara'), on=[col("df1.MATNR") == col("mara.MATNR")],how='left').select([col("df1." + cols) for cols in it_final2.columns]+ [col("mara.BRGEW")]+[col("mara.VOLUM")])

# COMMAND ----------

# defining breakage quantity
it_final3 = it_mara.withColumn("Breakage_QTY_MT",when((col("werk").isin(["2010","2013","2018","2022"])),(col("ANZFEHLER")*col("VOLUM"))/lit(1000)).otherwise((col("ANZFEHLER")*col("BRGEW"))/lit(1000)))

# COMMAND ----------

# selecting final columns by renaming
final_df = it_final3.select(col("AUFNR").alias("PRODUCTION_ORDER"),col("PRUEFLOS").alias("INSPECTION_LOT"),col("ENSTEHDAT").alias("LOT_CREATED_DATE"),col("FEGRP").alias("DEFECT_GROUP"),col("FECOD").alias("DEFECT_CODE"),col("WERK").alias("PLANT_ID"),col("MATNR").alias("MATERIAL_ID"),col("ANZFEHLER").alias("Breakage_QTY_EA"),col("BREAKAGE_QTY_MT"))

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'LOADING_BREAKAGES', mode='overwrite')

# COMMAND ----------

# class definition and reading surrogate metadata from SQL
pmu = ProcessMetadataUtility()
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

#--------------------SURROGATE IMPLEMENTATION------------------------
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition_sl="\n left join {pro}.DIM_DEFECT on".format(pro=processed_db_name)
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
  
    if( (fact_table_name==fact_table or fact_table_name.lower()==fact_table) and (dim_table!="DIM_DEFECT") ):
      
      count += 1
      tmp_unmapped_str = "CASE WHEN "
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull({dim_table}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} on {fact_table}.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
    
    if((fact_table_name==fact_table or fact_table_name.lower()==fact_table) and dim_table=='DIM_DEFECT'):
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
    ,DIM_DEFECT.DEFECT_KEY AS DEFECT_KEY
    """    
  query = """select 
PRODUCTION_ORDER,
INSPECTION_LOT,
Breakage_QTY_EA,
BREAKAGE_QTY_MT
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
d = surrogate_mapping_hil(csv_data,table_name,"LOADING_BREAKAGES", processed_db_name)

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
