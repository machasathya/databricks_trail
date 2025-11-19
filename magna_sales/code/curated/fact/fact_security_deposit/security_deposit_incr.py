# Databricks notebook source
# Import statements
import datetime 
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from ProcessMetadataUtility import ProcessMetadataUtility
from datetime import timedelta 

# COMMAND ----------

# fetching current date
now = datetime.datetime.now() + timedelta(seconds = 19800)
day_n = now.day
ist_zone = datetime.datetime.now() + timedelta(hours=5.5)
var_date = (ist_zone - timedelta(days=1)).strftime("%Y-%m-%d")
p_budat = F.to_date(F.lit(var_date),'yyyy-MM-dd')
p_budat_str = datetime.datetime.strptime(var_date, "%Y-%m-%d").date()
month_str = p_budat_str.strftime("%B")

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
table_name = "FACT_SECURITY_DEPOSIT"
last_processed_time = datetime.datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the code manually
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
# table_name = "FACT_SECURITY_DEPOSIT"
# last_processed_time = datetime.datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# LT_BSID
lt_bsid_df = spark.sql("SELECT VBELN, BUKRS, KUNNR, UMSKS, UMSKZ, AUGDT, AUGBL, ZUONR, GJAHR, BELNR, BUZEI, BUDAT, BLDAT, WAERS, XBLNR, BLART, SHKZG, DMBTR, WRBTR, ZFBDT, ZTERM, ZBD1T, REBZG, REBZJ FROM {db}.BSID WHERE BUDAT <= '{p_budat}' AND (BSTAT NOT IN ('S') OR BSTAT is null) ORDER BY BUKRS,KUNNR".format(p_budat=p_budat_str, db=curated_db_name))

# COMMAND ----------

# LT_BSAD

lt_bsad_df = spark.sql("SELECT VBELN, BUKRS, KUNNR, UMSKS, UMSKZ, AUGDT, AUGBL, ZUONR, GJAHR, BELNR, BUZEI, BUDAT, BLDAT, WAERS, XBLNR, BLART, SHKZG, DMBTR, WRBTR, ZFBDT, ZTERM, ZBD1T, REBZG, REBZJ FROM {db}.BSAD WHERE BUDAT <= '{p_budat}' AND AUGDT > '{p_budat}' AND (BSTAT NOT IN ('S') or BSTAT is null) ORDER BY BUKRS,KUNNR".format(p_budat=p_budat_str, db=curated_db_name))

# COMMAND ----------

# Union of bsid and bsad
lt_docs_df = lt_bsid_df.union(lt_bsad_df).where("UMSKZ='H'")

# COMMAND ----------

# Transformation | Amount sign transformation | Credit vs Debit
lt_docs_df = lt_docs_df.withColumn("DMBTR",F.when(lt_docs_df.SHKZG=="H", -1 * lt_docs_df.DMBTR).otherwise(lt_docs_df.DMBTR))

# COMMAND ----------

# Aggregating data upon customers
sec_dep = lt_docs_df.groupBy("KUNNR").agg(F.sum("DMBTR").alias("security_deposit"))
sec_dep.createOrReplaceTempView("security")

# COMMAND ----------

# extracting hysil customers data
lt_hysil_df_query = """
  SELECT lt.*
  FROM security lt
  WHERE lt.KUNNR not in(
  SELECT lpad(CUSTOMER_CODE, 10,'0')
  FROM {db}.dim_hysil_customers)
  """
lt_hysil_df = spark.sql(lt_hysil_df_query.format(db=processed_db_name))
lt_hysil_df.createOrReplaceTempView("LT_HYSIL")

# COMMAND ----------

# adding snapshot_date
security_final_df = lt_hysil_df.withColumn("snapshot_date", F.lit(var_date)).withColumn("month", F.lit(month_str)).withColumn("last_executed_time", F.lit(last_processed_time_str))

# COMMAND ----------

# extracting surrogate meta data from SQL
pmu = ProcessMetadataUtility()
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

#SURROGATE KEY IMPLEMENTATION FOR FACT_SALES_TARGETS - INCLUDING LOGIC FOR GEO-KEY--

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  sur_col_list = []
  join_condition=""
  count=0
  
  security_final_df.createOrReplaceTempView("{}".format(fact_name))
  
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
      tmp_unmapped_str += "(A.{fact_column} is NULL OR trim(A.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull({dim_table}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table)
      tmp_unmapped_str += "A.{fact_column},\n".format(fact_column=fact_column, fact_name=fact_name)
      sur_col_list.append("{fact_surrogate}".format(fact_surrogate=fact_surrogate))
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
          
      join_condition += "\n left join {pro_db}.{dim_table} on A.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
   
  select_condition = select_condition[:-2]
  query = """select 
  A.security_deposit as SECURITY_DEPOSIT  
  ,{select_condition},to_timestamp(last_executed_time, 'yyyy-MM-dd HH:mm:ss') as LAST_EXECUTED_TIME, A.month  from {fact_name} A {join_condition}""".format(
               join_condition=join_condition,fact_name=fact_name,select_condition=select_condition)
  
  print("\nFinal Query: " +query) 
  fact_final_view_surrogate = spark.sql(query)
  fact_final_view_surrogate_persist = fact_final_view_surrogate
  fact_final_view_surrogate_persist.createOrReplaceTempView("{fact_table}_busi_sur".format(fact_table=fact_table))
  
###   Where condition for unmapped records, selecting records where surrogate keys are -1
  sur_col_list = list(set(sur_col_list))
  str_where_condition = ""
  for item in sur_col_list:
    str_where_condition+= "{item} = -1 or ".format(item=item)
    
###   Select query for unmapped records
  str_where_condition = str_where_condition[:-3]
  unmapped_final_with_sur_df = spark.sql("select * from {fact_table}_busi_sur where {str_where_condition}".format(str_where_condition=str_where_condition,fact_table=fact_table))
  
  cols = []
  
  for item in fact_final_view_surrogate.columns:
    cols.append( "{} AS {}".format(item ,item.upper()))
  fact_final_view_surrogate = fact_final_view_surrogate.selectExpr(cols)
  #print(cols)
  return fact_final_view_surrogate, unmapped_final_with_sur_df

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sec_dep;

# COMMAND ----------

# Generating Surrogate keys
data_cag, unmapped_final_df = surrogate_mapping_hil(csv_data, table_name, "SECURITY_DEPOSIT", processed_db_name)

data_cag.createOrReplaceTempView("sec_dep")

# COMMAND ----------

# writing unmapped data to processed location
unmapped_final_df.write.mode('overwrite').parquet(processed_location + table_name + "_UNMAPPED")

# COMMAND ----------

# selecting columns in req order
full_df = spark.sql("select A.CUSTOMER_KEY,A.SECURITY_DEPOSIT,A.SNAPSHOT_DATE_KEY,A.LAST_EXECUTED_TIME, A.month from sec_dep A")

# COMMAND ----------

# writing incr data to processed layer table
full_df.repartition("snapshot_date_key").write.insertInto(processed_db_name + "." + table_name, overwrite=True)

# COMMAND ----------

# inserting log record to last_execution_details table
date_now = datetime.datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'INCREMENTAL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
