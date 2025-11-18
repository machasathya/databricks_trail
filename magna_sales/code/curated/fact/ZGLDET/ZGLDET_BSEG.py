# Databricks notebook source
# DBTITLE 1,Import Session
# import statements
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, split, concat, lpad
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql import functions as F
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
spark.conf.set("spark.sql.broadcastTimeout",  36000)

# COMMAND ----------

# defining class object
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
table_name = "FACT_GL_ACCOUNT"
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
# processed_schema_name = "sales_semantic_prod"
# processed_db_name = "pro_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# table_name = "FACT_GL_ACCOUNT"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surroagte metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# DBTITLE 1,Table Names Declaration
#Initialize Table Names
makt = "makt"
t001 = "t001"
skat = "skat"
skb1 = "skb1"
bsis = "bsis"
bkpf = "bkpf"
bseg = "bseg"

# COMMAND ----------

# DBTITLE 1,MAKT Table
makt_df = spark.sql("""SELECT matnr,maktx FROM {}.{} """.format(curated_db_name,makt))
makt_df.createOrReplaceTempView("makt_df_tmp")

# COMMAND ----------

# DBTITLE 1,T001 Table
t001_df = spark.sql("""SELECT * FROM {}.{} """.format(curated_db_name,t001))
t001_df.createOrReplaceTempView("t001_df_tmp")

# COMMAND ----------

# DBTITLE 1,SKAT Table
skat_df = spark.sql("""SELECT * FROM {}.{} """.format(curated_db_name,skat))
skat_df.createOrReplaceTempView("skat_df_tmp")

# COMMAND ----------

# DBTITLE 1,SKB1 Table
skb1_df = spark.sql("""SELECT * FROM {}.{} """.format(curated_db_name,skb1))
skb1_df.createOrReplaceTempView("skb1_df_tmp")

# COMMAND ----------

# DBTITLE 1,BSIS Table
bsis_df = spark.sql("""SELECT * FROM {}.{} """.format(curated_db_name,bsis))
bsis_df.createOrReplaceTempView("bsis_df_tmp")

# COMMAND ----------

# DBTITLE 1,BKPF Table
bkpf_df = spark.sql("""SELECT * FROM {}.{} """.format(curated_db_name,bkpf))
bkpf_df.createOrReplaceTempView("bkpf_df_tmp")

# COMMAND ----------

# DBTITLE 1,BSEG Table
bseg_df = spark.sql("""SELECT * FROM {}.{} """.format(curated_db_name,bseg))
bseg_df.createOrReplaceTempView("bseg_df_tmp")

# COMMAND ----------

# reading gl master data from processed layer and adding leading 0's to gl code column
dim_gl_master = spark.sql("select * from {}.{}".format(processed_db_name,"DIM_GL_MASTER"))
dim_gl_master = dim_gl_master.withColumn("GL_CODE",lpad(col("GL_CODE"),10,'0'))

# COMMAND ----------

# generating GL Numbers list
GL_Numbers = [row.GL_CODE for row in dim_gl_master.toLocalIterator()]
print(GL_Numbers)

# COMMAND ----------

# filtering the data absed on company code
it_h_t001 = t001_df.filter("bukrs='1000'")

# COMMAND ----------

# generating it_skat dataframe
it_h_skat = skat_df.join(it_h_t001, on = [trim(it_h_t001["bukrs"]) == trim(skat_df["KTOPL"]), skat_df.SAKNR.isin(GL_Numbers)], how = 'left_semi')

# COMMAND ----------

# generatinf it_skb1 dataframe by joining skat and skb1 on gl number
it_h_skb1 = skb1_df.join(it_h_skat, on = [it_h_skat["KTOPL"] == skb1_df["BUKRS"],it_h_skat["SAKNR"] == skb1_df["SAKNR"]], how = 'left_semi')

# COMMAND ----------

# joining bseg with skb1 dataframe based on gl number and company code
bsis_data = bseg_df.alias("a1").join(it_h_skb1, on = [it_h_skb1["BUKRS"] == bseg_df["BUKRS"],it_h_skb1["SAKNR"] == bseg_df["HKONT"]],
                                   how='right').select([col("a1." + cols) for cols in bseg_df.columns]+ [col("mitkz")])

# COMMAND ----------

# filtering data based on account type
bsis_data_X = bsis_data.withColumn("XOPVW", F.when(((bsis_data["mitkz"] == "D") | (bsis_data["mitkz"] == "K")) & (bsis_data["xkres"] != " "), "X").otherwise(bsis_data["XOPVW"]))

# COMMAND ----------

# joining bkpg and bseg data on doc no and item
bsis_bseg_df = bsis_data_X.alias("a2").join(bkpf_df, on = [bsis_data_X["BUKRS"] == bkpf_df["BUKRS"],bsis_data_X["BELNR"] == bkpf_df["BELNR"],bsis_data_X["GJAHR"] == bkpf_df["GJAHR"]],
                                   how='left').select([col("a2." + cols) for cols in bsis_data_X.columns]+ [col("BLDAT")] + [col("BUDAT")])

# COMMAND ----------

# filling koart field value as 'S'
bsis_bseg_df = bsis_bseg_df.withColumn("KOART", F.when(bsis_bseg_df["KOART"] != "S", "S").otherwise("S"))

# COMMAND ----------

# filtering data on account type
bsis_due = bsis_bseg_df.filter("KOART = 'D' or KOART = 'K' or zfbdt is not null")

# COMMAND ----------

# changing values in zfbdt field
bsis_due1 = bsis_due.withColumn("zfbdt", F.when(((bsis_due["KOART"] == "D") | (bsis_due["KOART"] == "K")) & (bsis_due["zfbdt"].isNull()), bsis_due["BLDAT"]).otherwise(bsis_due["zfbdt"]))

# COMMAND ----------

bsis_due2 = bsis_due1.withColumn("REFE",when((col("ZBD3T").isNotNull()),col("ZBD3T")).when((col("ZBD2T").isNotNull()),col("ZBD2T")).otherwise(bsis_due1["ZBD1T"]))

# COMMAND ----------

bsis_due2 = bsis_due2.withColumn("REFE", F.when(((bsis_due2["KOART"] == "D") | (bsis_due2["SHKZG"] == "H")) & ((bsis_due2["KOART"] == "K") | (bsis_due2["SHKZG"] == "S")) & (bsis_due2["REBZG"].isNull()), "0").otherwise(bsis_due2["REFE"]))

# COMMAND ----------

bsis_due2 = bsis_due2.withColumn("faedt", F.expr("date_add(ZFBDT, REFE)"))

# COMMAND ----------

bsis_item1 = bsis_bseg_df.withColumn("dmshb", F.when(bsis_bseg_df["shkzg"] == "H", bsis_bseg_df["dmbtr"] * -1).otherwise(bsis_bseg_df["dmbtr"]))
bsis_item = bsis_item1.withColumn("mengeshb", F.when(bsis_item1["shkzg"] == "H", bsis_item1["MENGE"] * -1).otherwise(bsis_item1["MENGE"]))

# COMMAND ----------

# renaming selected columns
final_data = bsis_item.select('HKONT','GJAHR','dmshb','BUDAT','WERKS','SGTXT','GSBER','mengeshb')
final_data = final_data.withColumnRenamed("HKONT","GL_CODE").withColumnRenamed("GJAHR","FISCAL_YEAR").withColumnRenamed("dmshb","AMOUNT").withColumnRenamed("BUDAT","DATE").withColumnRenamed("WERKS","PLANT").withColumnRenamed("SGTXT","TEXT").withColumnRenamed("GSBER","BUSINESS_AREA").withColumnRenamed("mengeshb","QUANTITY")

# COMMAND ----------

# writing data to processed location before generating surrogate keys
final_data.write.parquet(processed_location+'GL_ACCOUNT', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
  final_data.createOrReplaceTempView("{}".format(fact_name))
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
   FISCAL_YEAR,
   AMOUNT,
   TEXT,
   QUANTITY
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

# generating surrogate keys
final = surrogate_mapping_hil(csv_data,table_name,"GL_ACCOUNT",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
final.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

#inserting log record to LAST_EXECUTION_DETAILS table in SQL
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
