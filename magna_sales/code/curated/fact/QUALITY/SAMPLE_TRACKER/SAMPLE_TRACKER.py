# Databricks notebook source
# import statements
from datetime import datetime 
from pyspark.sql.functions import col, lit, expr, to_date, to_timestamp, sum, min, when, count, concat,lpad, regexp_replace,collect_list,array,concat_ws
from pyspark.sql import functions as sf
from pyspark.sql.types import *
from ProcessMetadataUtility import ProcessMetadataUtility
pmu = ProcessMetadataUtility()

# COMMAND ----------

# enabling cross join
spark.conf.set("spark.sql.crossJoin.enabled", "true")

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
table_name = "FACT_QUALITY_SAMPLE_TRACKER"
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
# table_name = "FACT_QUALITY_SAMPLE_TRACKER"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
sam_det  = spark.sql("""select 
                        refno,
                        control_no,
                        samplename,
                        prodid,
                        finalrem,
                        sapvendorcode,
                        createdon,
                        sourceid from {}.{}""".format(curated_db_name,"sampledetails"))
sam_det = sam_det.withColumn("sapvendorcode",lpad(col("sapvendorcode"),18,'0'))

# COMMAND ----------

# reading data from SQL
test_res = spark.sql("""select * from {}.{}""".format(curated_db_name,"testresult"))
prod_mas = spark.sql("""select prodid,prodtype,prodname from {}.{}""".format(curated_db_name,"productmast"))
test_det = spark.sql("select * from {}.{}".format(curated_db_name,"TESTDETAILS_NEW"))

# COMMAND ----------

# defining test result field
final_1 = sam_det.alias('sd').join(test_res.alias('tr'), on=[
                                               col('sd.control_no') == col('tr.control_no')],
                            how='left').select([col("sd."+cols) for cols in sam_det.columns] + [col("testresult"),col("testval"),col("tr.testid"),col("tr.paramid")])
final_1 = final_1.distinct()
final_1 = final_1.fillna({"testresult":"In Progress"})
final_1 = final_1.withColumn("testresult",when(col('testresult') == '',"In Progress").otherwise(col('testresult')))

# COMMAND ----------

# defining test parameters field
final_2 =  final_1.alias('t1').join(prod_mas.alias('pm'), on=[
                                               col('t1.prodid') == col('pm.prodid')],
                            how='left').select([col("t1."+cols) for cols in final_1.columns] + [col("prodname")])

final_df = final_2.alias("l_df").join(test_det.alias("r_df"),on=[col("l_df.prodid") == col("r_df.prodid"),
                                                                col("l_df.testid") == col("r_df.testid"),
                                                                col("l_df.paramid") == col("r_df.paramid")],how='left').select([col("l_df."+cols) for cols in final_2.columns] + [col("TESTPARAM")])
final_df = final_df.fillna({"TESTPARAM":"NA","testval":"NA"})

# COMMAND ----------

# defining test results field
final_df = final_df.withColumn("TEST_RESULTS",concat(col("TESTPARAM"),lit(" - "),col("testval"),lit(" , "),col("testresult")))

# COMMAND ----------

# aggregating data
final_df = final_df.groupBy("control_no","refno","samplename","prodid","finalrem","createdon","sourceid","sapvendorcode","testresult","prodname").agg(collect_list(col("TEST_RESULTS")).alias("TEST_RESULTS"))

final_df = final_df.withColumn("TEST_RESULTS",concat_ws(" : ", "TEST_RESULTS"))

# COMMAND ----------

# defining created date
final_df = final_df.drop('prodid')
final_df = final_df.withColumnRenamed('sourceid','plant_id')
for col in final_df.columns:
  final_df = final_df.withColumnRenamed(col, col.upper())
final_df = final_df.withColumn("CREATEDON",final_df["createdon"].cast(DateType()))

# COMMAND ----------

# defining vendor code
final_df = final_df.withColumn('SAPVENDORCODE', sf.regexp_replace('SAPVENDORCODE', r'^[0]*', ''))
final_df = final_df.withColumn('SAPVENDORCODE', lpad(final_df.SAPVENDORCODE,10, '0'))

# COMMAND ----------

# writing data to processed layer location before generating surrogate keys
final_df.write.parquet(processed_location+'SAMPLE_TRACKER', mode='overwrite')

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
  refno as REF_NO
  ,control_no as CONTROL_NO
  ,samplename as SAMPLE_NAME
  ,finalrem as FINAL_REMARKS
  ,TESTRESULT as STATUS
  ,TEST_RESULTS as TEST_RESULT
  ,PRODNAME as PRODUCT_NAME
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
df = surrogate_mapping_hil(csv_data,table_name,"SAMPLE_TRACKER",processed_db_name)

# COMMAND ----------

# writing data to processed layer location after generating surrogate keys
df.write.mode('overwrite').parquet(processed_location+"FACT_QUALITY_SAMPLE_TRACKER")

# COMMAND ----------

# insert log data into last execution details tabl
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
