# Databricks notebook source
# import statements
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType, DoubleType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql import Window
spark.conf.set( "spark.sql.crossJoin.enabled","true" )
spark.conf.set("spark.sql.broadcastTimeout",36000)

# COMMAND ----------

# class object definition
pmu = ProcessMetadataUtility()

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
table_name = "FACT_MIS_SUMMARY"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# variables to run the notebook manually
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata"
# curated_db_name = "cur_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# scope = "AZR-DBR-KV-SCOPE-300"
# processed_location = "mnt/bi_datalake/prod/pro/"
# processed_schema_name = "global_semantic_prod"
# processed_db_name = "pro_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_MIS_SUMMARY"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
# csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
# pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
df=spark.sql("select * from {}.{}".format(curated_db_name, "MIS_SUMMARY")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# selecting columns in req order
final_df = df.select('HIL','SBU','PRODUCT_LINE','PLANT','DATE','ACTUAL_OR_BUDGET','PRODUCTION','SALES','GROSS_REVENUE','DISCOUNT_COMMISSION','REVENUE','FREIGHT','PACKING','TOTAL_DISTRIBUTION_EXPENSES','NET_SALES_REALISATION','MISC_RECEIPTS','SERV_TO_OTHER_DIVNS','FOB_SALE_TO_AEROCON','TOTAL_OTHER_INCOME','TOTAL_INCOME_SALES_INCOME','INC_DEC_IN_INVENTORY','PRODUCTION_VALUE','IMPORTED_RAW_MATERIAL','INDIGENOUS_RAW_MATERIAL','TRADED_GOODS','TOTAL_RAW_MATERIAL','STORES_SPARES','POWER_FUEL','WAGES_BENEFITS','GRM','CONTRACT_WAGES','TOTAL_OF_OTHER_VARIABLE_EXP_EXCLUDING_RM','TOTAL_MATERIAL_COST_VERIABLE_COST','CONTRIBUTION','REPAIRS_TO_BUILDINGS','SALARIES_BENEFITS','INSURANCE','VEHICLE_EXPENSES','COMMUNICATION_EXPENSES','G_CHARGES','TRAV_CONV','RATE_TAXES','TOTAL_OTHER_MFG_EXPENSES','R_D_EXPENSES','C_O_EXPENSES_APPORTIONED','TOTAL_MANUFACTURING_OVERHEADS','TOTAL_MANUFACTURING_EXP','RENT_RATES_TAXES','EXCISE_DUTY_ON_CLOSING_STOCK','MARKETING_STAFF_SALARIES_AND_BENEFITS','DIRECTORS_AUDITORS_FEES','SAG_VEHICLES_EXPENSES','SAG_COMMUNICATION_EXPENSES','ADVERTISEMENT','GENERAL_CHARGES','TRAVELLING_CONVEYANCE','SAG_C_O_EXPENSES_APPORTIONED','TOTAL_SELLING_ADMN_GEN_EXP','HIL_C_O_EXPENSES_APPORTIONED','TOTAL_EXPENSES','PBIDT','GRP_C_O_EXPENSES_APPORTIONED','EBITDA','INTEREST_LONG_TERM','INTEREST_SHORT_TERM','C_O_INTEREST_APPORTIONED','TOTAL_INTEREST','PBDT','DEPRECIATION','C_O_SHARE_APPORTIONED','TOTAL_DEPRECIATION','OPERATING_PROFIT','OTHER_NON_OPERATING_INCOME','NON_OPERATING_EXPENSES','PBT')

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
df.write.parquet(processed_location+'MIS_SUMMARY', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
  df.createOrReplaceTempView("{}".format(fact_name))
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
   HIL,
SBU,
PRODUCT_LINE,
PLANT,
ACTUAL_OR_BUDGET,
PRODUCTION,
SALES,
GROSS_REVENUE,
DISCOUNT_COMMISSION,
REVENUE,
FREIGHT,
PACKING,
TOTAL_DISTRIBUTION_EXPENSES,
NET_SALES_REALISATION,
MISC_RECEIPTS,
SERV_TO_OTHER_DIVNS,
FOB_SALE_TO_AEROCON,
TOTAL_OTHER_INCOME,
TOTAL_INCOME_SALES_INCOME,
INC_DEC_IN_INVENTORY,
PRODUCTION_VALUE,
IMPORTED_RAW_MATERIAL,
INDIGENOUS_RAW_MATERIAL,
TRADED_GOODS,
TOTAL_RAW_MATERIAL,
STORES_SPARES,
POWER_FUEL,
WAGES_BENEFITS,
GRM,
CONTRACT_WAGES,
TOTAL_OF_OTHER_VARIABLE_EXP_EXCLUDING_RM,
TOTAL_MATERIAL_COST_VERIABLE_COST,
CONTRIBUTION,
REPAIRS_TO_BUILDINGS,
SALARIES_BENEFITS,
INSURANCE,
VEHICLE_EXPENSES,
COMMUNICATION_EXPENSES,
G_CHARGES,
TRAV_CONV,
RATE_TAXES,
TOTAL_OTHER_MFG_EXPENSES,
R_D_EXPENSES,
C_O_EXPENSES_APPORTIONED,
TOTAL_MANUFACTURING_OVERHEADS,
TOTAL_MANUFACTURING_EXP,
SGA,
RENT_RATES_TAXES,
EXCISE_DUTY_ON_CLOSING_STOCK,
MARKETING_STAFF_SALARIES_AND_BENEFITS,
DIRECTORS_AUDITORS_FEES,
SAG_VEHICLES_EXPENSES,
SAG_COMMUNICATION_EXPENSES,
ADVERTISEMENT,
GENERAL_CHARGES,
TRAVELLING_CONVEYANCE,
SAG_C_O_EXPENSES_APPORTIONED,
TOTAL_SELLING_ADMN_GEN_EXP,
HIL_C_O_EXPENSES_APPORTIONED,
TOTAL_EXPENSES,
PBIDT,
GRP_C_O_EXPENSES_APPORTIONED,
EBITDA,
INTEREST_LONG_TERM,
INTEREST_SHORT_TERM,
C_O_INTEREST_APPORTIONED,
TOTAL_INTEREST,
PBDT,
DEPRECIATION,
C_O_SHARE_APPORTIONED,
TOTAL_DEPRECIATION,
OPERATING_PROFIT,
OTHER_NON_OPERATING_INCOME,
NON_OPERATING_EXPENSES,
PBT
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
d = surrogate_mapping_hil(csv_data,table_name,"MIS_SUMMARY",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# inserting log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
