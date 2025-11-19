# Databricks notebook source
# import statements
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
table_name = "FACT_WC_ROCE_CCC"
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
# table_name = "FACT_WC_ROCE_CCC"
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
df1 = df.withColumnRenamed("OTHER_CURRENT_ASSETS", "OTHER_CURRENT_ASSETS_PRE").withColumnRenamed("OTHER_CURRENT_LIABILITIES", "OTHER_CURRENT_LIABILITIES_PRE").withColumnRenamed("NET_WORKING_CAPITAL","NCA")

# COMMAND ----------

# defining columns
df2 = df1.withColumn("OTHER_CURRENT_ASSETS",(col("ADVANCES")+col("DEPOSITS")+col("OTHER_CURRENT_ASSETS_PRE"))).withColumn("OTHER_CURRENT_LIABILITIES",(col("DEPOSITS_AND_ADVANCES")+col("OTHER_CURRENT_LIABILITIES_PRE"))).withColumn("ACCOUNTS_PAYABLE",(col("ACCOUNTS_PAYABLE_IMPORT")+col("ACCOUNTS_PAYABLE_DOMESTIC")))

# COMMAND ----------

# defining columns
df3 = df2.withColumn('OTHER_CURRENT_ASSETS',df2['OTHER_CURRENT_ASSETS'].cast(DecimalType(13,2))).withColumn('OTHER_CURRENT_LIABILITIES',df2['OTHER_CURRENT_LIABILITIES'].cast(DecimalType(13,2))).withColumn('ACCOUNTS_PAYABLE',df2['ACCOUNTS_PAYABLE'].cast(DecimalType(13,2)))

# COMMAND ----------

# filtering actual and budget data
df_actual = df3.filter(df3.ACTUAL_OR_BUDGET == "ACTUAL")
df_budget = df3.filter(df3.ACTUAL_OR_BUDGET == "BUDGET")

# COMMAND ----------

# unpivot actual data
df_a = ['CASH_AND_BANK_BALANCES','INVENTORIES','RECEIVABLES','ADVANCES','DEPOSITS','OTHER_CURRENT_ASSETS_PRE','TOTAL_CURRENT_ASSETS','ACCOUNTS_PAYABLE_IMPORT','ACCOUNTS_PAYABLE_DOMESTIC','DEPOSITS_AND_ADVANCES','OTHER_CURRENT_LIABILITIES_PRE','TOTAL_CURRENT_LIABILITIES','NCA','OTHER_CURRENT_ASSETS','OTHER_CURRENT_LIABILITIES','ACCOUNTS_PAYABLE','REVENUE_NET_OF_DISCOUNT']

df_actual_unpivot = df_actual.selectExpr('HIL','SBU','PRODUCT_LINE','DATE',"stack({}, {})".format(len(df_a), ', '.join(("'{}', {}".format(i, i) for i in df_a)))).withColumnRenamed("col0","PARTICULARS").withColumnRenamed("col1","ACTUAL")

# COMMAND ----------

# unpivot budget data
df_b = ['CASH_AND_BANK_BALANCES','INVENTORIES','RECEIVABLES','ADVANCES','DEPOSITS','OTHER_CURRENT_ASSETS_PRE','TOTAL_CURRENT_ASSETS','ACCOUNTS_PAYABLE_IMPORT','ACCOUNTS_PAYABLE_DOMESTIC','DEPOSITS_AND_ADVANCES','OTHER_CURRENT_LIABILITIES_PRE','TOTAL_CURRENT_LIABILITIES','NCA','OTHER_CURRENT_ASSETS','OTHER_CURRENT_LIABILITIES','ACCOUNTS_PAYABLE','REVENUE_NET_OF_DISCOUNT']
df_budget_unpivot = df_budget.selectExpr('HIL','SBU','PRODUCT_LINE','DATE',"stack({}, {})".format(len(df_b), ', '.join(("'{}', {}".format(i, i) for i in df_b)))).withColumnRenamed("col0","PARTICULARS").withColumnRenamed("col1","BUDGET")

# COMMAND ----------

# joining actual and budget unpivoted data
df4 = df_actual_unpivot.join(df_budget_unpivot,on=['HIL','SBU','PRODUCT_LINE','DATE','PARTICULARS'],how='inner').select(['HIL','SBU','PRODUCT_LINE','DATE','PARTICULARS','ACTUAL','BUDGET'])

# COMMAND ----------

df5 = df4.select('HIL','SBU','PRODUCT_LINE','DATE','PARTICULARS','BUDGET','ACTUAL')

df6 = df5.withColumn('PARTICULARS', sf.regexp_replace('PARTICULARS', '_', ' '))

df_final = df6.withColumn('SORT',when(df6.PARTICULARS=='CASH AND BANK BALANCES',15).when(df6.PARTICULARS=='INVENTORIES',2).when(df6.PARTICULARS=='RECEIVABLES',1).when(df6.PARTICULARS=='ADVANCES',3).when(df6.PARTICULARS=='DEPOSITS',4).when(df6.PARTICULARS=='OTHER CURRENT ASSETS PRE',5).when(df6.PARTICULARS=='TOTAL CURRENT ASSETS',7).when(df6.PARTICULARS=='ACCOUNTS PAYABLE IMPORT',8).when(df6.PARTICULARS=='ACCOUNTS PAYABLE DOMESTIC',9).when(df6.PARTICULARS=='DEPOSITS AND ADVANCES',11).when(df6.PARTICULARS=='OTHER CURRENT LIABILITIES PRE',12).when(df6.PARTICULARS=='TOTAL CURRENT LIABILITIES',14).when(df6.PARTICULARS=='NCA',16).when(df6.PARTICULARS=='OTHER CURRENT ASSETS',6).when(df6.PARTICULARS=='OTHER CURRENT LIABILITIES',13).when(df6.PARTICULARS=='ACCOUNTS PAYABLE',10))

# COMMAND ----------

# writing data to processed location before generating surrogate keys
df_final.write.parquet(processed_location+'WC_ROCE_CCC', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
  df_final.createOrReplaceTempView("{}".format(fact_name))
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
   PARTICULARS,
   BUDGET,
   ACTUAL,
   SORT,
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
d = surrogate_mapping_hil(csv_data,table_name,"WC_ROCE_CCC",processed_db_name)

# COMMAND ----------

# writing data to processed location after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# insert log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
