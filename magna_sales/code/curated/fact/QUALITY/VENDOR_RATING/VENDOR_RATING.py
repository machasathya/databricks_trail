# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,trim
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
pmu = ProcessMetadataUtility()

# COMMAND ----------

# enabling cross join
spark.conf.set("spark.sql.crossJoin.enabled", "true")

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
table_name = "FACT_VENDOR_QUALITY_RATING"
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
# table_name = "FACT_VENDOR_QUALITY_RATING"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading the surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading the data from curated layer
QALS = spark.sql("select PRUEFLOS,ERSTELDAT,WERK,LIFNR,MATNR,ART,STAFO,STAT34,BUDAT from {}.{}".format(curated_db_name, "QALS"))
QAVE = spark.sql("select PRUEFLOS,QKENNZAHL,VORGLFNR from {}.{}".format(curated_db_name, "QAVE"))
QAMR = spark.sql("select PRUEFLOS,first(VORGLFNR) as VORGLFNR,first(MERKNR) as MERKNR,first(MITTELWERT) as MITTELWERT from {}.{} where MBEWERTG = 'R' group by PRUEFLOS".format(curated_db_name, "QAMR"))
QAMV = spark.sql("select PRUEFLOS,VORGLFNR,MERKNR,KURZTEXT from {}.{}".format(curated_db_name,"QAMV"))

# COMMAND ----------

# joining qamr and qamv data
remarks_df = QAMR.alias("l_df").join(QAMV.alias("r_df"),on = [col("l_df.PRUEFLOS") == col("r_df.PRUEFLOS"),
                                                              col("l_df.MERKNR")==col("r_df.MERKNR"),
                                                              col("l_df.VORGLFNR") == col("r_df.VORGLFNR")],how='left').select([col("l_df.PRUEFLOS"),col("r_df.KURZTEXT"),col("l_df.MITTELWERT")])

# COMMAND ----------

# joining qals and qave data
df1 = QALS.alias('qals').join(QAVE.alias('qave'), on=[col('qals.PRUEFLOS') == col('qave.PRUEFLOS')],how='inner').select([col("qals.BUDAT").alias("POSTING_DATE")] + [col("qals.PRUEFLOS").alias("Inspection_Lot")] + [col("qals.WERK").alias("Plant")] + [col("qals.LIFNR").alias("Vendor_id")] + [col("qals.MATNR").alias("MATERIAL_ID")] + [col("qave.QKENNZAHL").alias("Quality_Score")]).where((trim(col("ART")) == "01") & (trim(col("STAFO")) != ''))

display(df1.where("Vendor_id = '0000300000' and werk = '2005' and posting_date>='2021-02-01' and posting_date<='2021-02-28' and Inspection_Lot = '010000418708'"))

# COMMAND ----------

# defining mean value
df2 = df1.alias('df').join(remarks_df.alias('r_df'), on=[(col('df.Inspection_Lot') == col('r_df.PRUEFLOS'))],how='left').select(["df." + cols for cols in df1.columns]  + [col("r_df.KURZTEXT").alias("Remarks"),col("r_df.MITTELWERT").alias("MEAN_VALUE")])

# COMMAND ----------

# changing columns to upper case
for col in df2.columns:
  df2 = df2.withColumnRenamed(col, col.upper())

# COMMAND ----------

# writing data to processed layer before generating surrogate key
df2.write.parquet(processed_location+'VENDOR_QUALITY_RATING', mode='overwrite')

# COMMAND ----------

# surrogate key mapping
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
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
    
    if((fact_table_name==fact_table or fact_table_name.lower()==fact_table)):
      count += 1
      tmp_unmapped_str = "CASE WHEN "
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull(A{count}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table, count=count)     
      
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} A{count} on {fact_table}.{fact_column} = A{count}.{dim_column} ".format(
            dim_table=dim_table, count=count, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
      
  select_condition = select_condition[:-2]
  query = """select 
Inspection_Lot,
Quality_Score,
Remarks,
MEAN_VALUE,
  {select_condition}  from {fact_name}  {join_condition}
  """.format(
              join_condition=join_condition,fact_name=fact_name,select_condition=select_condition)

  print("\nFinal Query for {fact_table}\n (Total Surrogate Keys = {count}) :\n {query}".format(count=count, query=query,fact_table=fact_table))
  fact_final_view_surrogate = spark.sql(query)
  cols = []
  
  for item in fact_final_view_surrogate.columns:
    cols.append( "{} AS {}".format(item ,item.upper()))
  fact_final_view_surrogate = fact_final_view_surrogate.selectExpr(cols)
  #print(cols)
  return fact_final_view_surrogate

# COMMAND ----------

# creating surrogate keys
d = surrogate_mapping_hil(csv_data,table_name,"VENDOR_QUALITY_RATING",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate key
d.write.mode('overwrite').parquet(processed_location+"FACT_VENDOR_QUALITY_RATING")

# COMMAND ----------

# insert log data into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
