# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,upper,concat,to_date,collect_list
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as sf
from pyspark.sql import types as T
from ProcessMetadataUtility import ProcessMetadataUtility
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
table_name = "FACT_RM_PROCUREMENT"
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
# table_name = "FACT_RM_PROCUREMENT"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer and handling reversal movement types
mseg = spark.sql("select MBLNR,BWART,MENGE,DMBTR,MEINS,MATNR,EBELN,EBELP,LIFNR,MATNR,BUDAT_MKPF,WERKS from {}.{} where bwart in ('101','102','122','123')".format(curated_db_name,"MSEG")).withColumn("menge",when(col("bwart").isin(["101","123"]),col("MENGE")).when(col("bwart").isin(["102","122"]),-col("MENGE"))).withColumn("dmbtr",when(col("bwart").isin(["101","123"]),col("dmbtr")).when(col("bwart").isin(["102","122"]),-col("dmbtr")))

mara = spark.sql("select * from {}.{}".format(curated_db_name,"MARA"))

marm = spark.sql("select * from {}.{} where meinh='KG'".format(curated_db_name,"MARM"))

ekpo = spark.sql("Select EBELN,EBELP,NETPR,MENGE from {}.{}".format(curated_db_name,"EKPO"))

date = spark.sql("select * from {}.{}".format(curated_db_name,"dim_date"))

# COMMAND ----------

# filtering raw materials and converting values to MT
df1 = mseg.alias("df1").join(mara,on=["matnr"],how='left').select(["df1." + cols for cols in mseg.columns] + [col('mtart')]).where("mtart in ('ROH','ZROH')")

df2 = df1.alias("df1").join(marm,on=["matnr"],how='left').select(["df1." + cols for cols in df1.columns] + [col('umren'),col("umrez")])

df3 = df2.withColumn("WEIGHTS_MT",when((col("meins").isin(["MT","TO"])),col("menge")).when((col("meins") == 'KG'),(col('menge')/1000)).when((col("meins").isin(["KG","MT","TO"]) == False) & (col("umrez").isNull() == False),(col("UMREN")/col("UMREZ"))*col("MENGE")).otherwise(col("menge")))


df4 = df3.alias("df3").join(ekpo.alias("ekpo"),on=[col("df3.ebeln") == col("ekpo.ebeln"),col("df3.ebelp") == col("ekpo.ebelp")]).select(["df3." + cols for cols in df3.columns]+[col("ekpo.netpr")])

# COMMAND ----------

# adding budget values
budgets_1 = spark.sql("select * from {}.{}".format(curated_db_name,"RM_BUDGETS"))

unPivot_dF = budgets_1.selectExpr('PLANT_ID',
 'PLANT_NAME',
 'SBU',
 'MATERIAL_ID',
 'MATERIAL_NAME',
 'FINANCIAL_YEAR' ,\
         "STACK(4, 'BUDGET_Q1', BUDGET_Q1, 'BUDGET_Q2', BUDGET_Q2, 'BUDGET_Q3', BUDGET_Q3,'BUDGET_Q4',BUDGET_Q4)") \
.withColumnRenamed("col0","QUARTER") \
.withColumnRenamed("col1","BUDGET_AMOUNT") \
.where("BUDGET_AMOUNT is not null")


budgets_2 = unPivot_dF.withColumn('FINANCIAL_YEAR',when(col('QUARTER').isin(['BUDGET_Q1','BUDGET_Q2','BUDGET_Q3']),lit(col('FINANCIAL_YEAR')-1).cast(IntegerType())).otherwise(col('FINANCIAL_YEAR')))

budgets_3 = budgets_2.withColumn("Month",
       when(col("QUARTER") == "BUDGET_Q1" , "04")
      .when(col("QUARTER") == "BUDGET_Q2" , "07")
      .when(col("QUARTER") == "BUDGET_Q3" , "10")                
      .otherwise("01")).withColumn('DATE',(concat(col("FINANCIAL_YEAR"),lit("-"),col("Month"), lit("-"), lit("01"))))

budgets_4 = budgets_3.drop(*['FINANCIAL_YEAR','SBU','QUARTER','PLANT_NAME','Month','MATERIAL_NAME'])

budgets_df = budgets_4.withColumn("DATE",to_date(col("DATE"),"yyyy-MM-dd")).withColumn("MATERIAL_ID",lpad(col("MATERIAL_ID"),18,'0'))


qtr_budg = budgets_df.alias('l_df').join(date.alias('r_df'),on = [col("l_df.date") == col("r_df.date_dt")],how='left').select(["l_df." + cols for cols in budgets_df.columns]+[col("QTR_OF_YR_NUM")])

# COMMAND ----------

temp_1 = df4.alias('l_df').join(date.alias('r_df'),on = [col('l_df.BUDAT_MKPF') == col('r_df.DATE_DT')]).select(["l_df." + cols for cols in df4.columns] + [col('QTR_OF_YR_NUM')])

temp_2 = temp_1.alias('l_df').join(qtr_budg.alias('r_df'),on = [col("l_df.werks") == col("r_df.plant_id"),
                                                               col("l_df.matnr") == col("r_df.material_id"),
                                                               col("l_df.QTR_OF_YR_NUM") == col("r_df.QTR_OF_YR_NUM")],how='left').select(["l_df." + cols for cols in temp_1.columns] + [col('BUDGET_AMOUNT')]).drop(col("QTR_OF_YR_NUM"))

# COMMAND ----------

# selecting and changing column names
final_df = temp_2.selectExpr("EBELN as PURCHASE_DOC","EBELP as ITEM","MBLNR as DOC_NUMBER","MENGE as QUANTITY","MEINS as UOM","LIFNR as VENDOR_ID","MATNR AS MATERIAL_NUMBER","BUDAT_MKPF as POSTING_DATE","WERKS as PLANT_ID","WEIGHTS_MT","dmbtr as AMOUNT","BUDGET_AMOUNT")

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'RM_PROCUREMENT', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION

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
                        " THEN -2 ELSE ifnull({dim_table}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} on {fact_table}.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)    
  select_condition = select_condition[:-2]      
  query = """select
  PURCHASE_DOC,
ITEM,
DOC_NUMBER,
QUANTITY,
UOM,
WEIGHTS_MT,
AMOUNT,
BUDGET_AMOUNT
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
df = surrogate_mapping_hil(csv_data,table_name,"RM_PROCUREMENT",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
df.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# insert log records to last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
