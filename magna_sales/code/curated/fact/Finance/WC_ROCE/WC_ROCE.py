# Databricks notebook source
# Import Statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,upper,concat, to_date
from pyspark.sql import functions as sf
from pyspark.sql.types import DateType,IntegerType,DecimalType
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.functions import sum, lit, when, from_unixtime, unix_timestamp, to_date
from pyspark.sql import Window
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
pmu = ProcessMetadataUtility()

# COMMAND ----------

# Enabling Cross Join 
spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

# variables to fetch the data from ADF parameters
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
table_name = "FACT_WC_ROCE"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# varaibles to run the notebook manually
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
# table_name = "FACT_WC_ROCE"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
df=spark.sql("select * from {}.{}".format(curated_db_name, "WC_ROCE")).drop("LAST_UPDATED_DT_TS").na.fill(0)

# COMMAND ----------

# renaming columns
df1 = df.withColumnRenamed("OTHER_CURRENT_ASSETS", "OTHER_CURRENT_ASSETS_PRE") \
.withColumnRenamed("OTHER_CURRENT_LIABILITIES", "OTHER_CURRENT_LIABILITIES_PRE") \
.withColumnRenamed("NET_WORKING_CAPITAL","NCA") 

# COMMAND ----------

# deriving columns
final_df = df1.withColumn("OTHER_CURRENT_ASSETS",(col("ADVANCES")+col("DEPOSITS")+col("OTHER_CURRENT_ASSETS_PRE"))) \
.withColumn("OTHER_CURRENT_LIABILITIES",(col("DEPOSITS_AND_ADVANCES")+col("OTHER_CURRENT_LIABILITIES_PRE"))) \
.withColumn("ACCOUNTS_PAYABLE",(col("ACCOUNTS_PAYABLE_IMPORT")+col("ACCOUNTS_PAYABLE_DOMESTIC")))

# COMMAND ----------

# writing data to processed location before generating surrogate keys
final_df.write.parquet(processed_location+'WC_ROCE', mode='overwrite')

# COMMAND ----------

#--------------------SURROGATE KEY IMPLEMENTATION------------------------

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
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
   ACTUAL_OR_BUDGET,
      CASH_AND_BANK_BALANCES,
      RAW_MATERIALS_COMPONENTS_IMPORTED,
      RAW_MATERIALS_COMPONENTS_INDIGENOUS,
      STORES_SPARES,
      STOCK_IN_PROCESS,
      FINISHED_GOODS,
      STOCK_IN_TRADE,
      INVENTORIES,
      RECEIVABLES,
      ADVANCES,
      DEPOSITS,
      OTHER_CURRENT_ASSETS_PRE,
      TOTAL_CURRENT_ASSETS,
      ACCOUNTS_PAYABLE_IMPORT,
      ACCOUNTS_PAYABLE_DOMESTIC,
      DEPOSITS_AND_ADVANCES,
      OTHER_CURRENT_LIABILITIES_PRE,
      TOTAL_CURRENT_LIABILITIES,
      NCA,
      GROSS_BLOCK,
      ADDITION_DELETION_DURING_THE_YEAR,
      TOTAL_GROSS_BLOCK,
      LESS_ACCUMLATED_DEPRECIATION,
      NET_FIXED_ASSETS,
      NET_CAPITAL_EMPLOYEED,
      REVENUE_NET_OF_DISCOUNT,
      EBIT,
      ROCE,
      WORKING_CAPITAL_TO_REVENUE,
      OTHER_CURRENT_ASSETS,
      OTHER_CURRENT_LIABILITIES,
      ACCOUNTS_PAYABLE,
      PRODUCT_LINE
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
d = surrogate_mapping_hil(csv_data,table_name,"WC_ROCE",processed_db_name)

# COMMAND ----------

# writing data to processed location after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# inserting log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
