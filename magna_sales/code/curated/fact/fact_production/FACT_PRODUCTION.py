# Databricks notebook source
# Import statements
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, when,trim
from ProcessMetadataUtility import ProcessMetadataUtility

# COMMAND ----------

# Parameters to fetch data from ADF
pmu = ProcessMetadataUtility()
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
table_name = "FACT_PRODUCTION"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# Variables to run the code manually
# from ProcessMetadataUtility import ProcessMetadataUtility
# pmu = ProcessMetadataUtility()
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
# processed_db_name = "pro_prod"
# table_name = "FACT_PRODUCTION"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
# csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
# pmu.close(db_connector, db_cursor)

# COMMAND ----------

# Reading mseg data from curated layer
mseg = spark.sql("select MATNR,WERKS,LGORT,BWART,SOBKZ,MBLNR,BUDAT_MKPF,MJAHR,ZEILE,ERFMG,ERFME,LIFNR,AUFNR,AUFPS,AUFPL,APLZL from {}.{} where aufnr is not null and ((bwart in ('101','102') and lgort = '0061' and werks in ('2001','2003','2004','2005','2009','2012')) or (bwart in ('101','102') and werks not in ('2001','2003','2004','2005','2009','2012')))".format(curated_db_name,"MSEG")).withColumn("ERFMG",when(col('BWART')=='101',col('ERFMG')).otherwise(col('ERFMG')*lit(-1)))

# COMMAND ----------

# Reading kg and cum3 conv value from marm
marm_kg = spark.sql("select * FROM {}.{}".format(curated_db_name, "MARM")).where(col("MEINH").isin(["KG"]))

marm_m3 = spark.sql("select * from {}.{}".format(curated_db_name,"MARM")).where(col("MEINH").isin(["M3"]))

# COMMAND ----------

# Fetching respective conv values from marm
df_ms_mm_kg = mseg.where("werks not in (2010,2013,2018,2022)").join(marm_kg, on=['MATNR'], how='left').select([col(cols) for cols in mseg.columns] +[col("UMREZ"),col("UMREN")])

df_ms_mm_m3 = mseg.where("werks  in (2010,2013,2018,2022)").join(marm_m3, on=['MATNR'], how='left').select([col(cols) for cols in mseg.columns] +[col("UMREZ"),col("UMREN")])

df_ms_mm = df_ms_mm_kg.union(df_ms_mm_m3)

# COMMAND ----------

# reading data from curated layer and joining afko and aufk
afko = spark.sql("select * FROM {}.{}".format(curated_db_name, "AFKO")).select('AUFNR')

aufk = spark.sql("select * FROM {}.{}".format(curated_db_name, "AUFK")).select('AUFNR').where(col('AUTYP') == 10)

af_au = afko.join(aufk, on = ['AUFNR'], how = 'inner')

# COMMAND ----------

# Reading material master data from processed layer
dim_product = spark.sql("select * from {}.{}".format(processed_db_name,"dim_product"))

# COMMAND ----------

# Reading zsbu3 data from curated layer
zsbu3 = spark.sql("select * from {}.{}".format(curated_db_name,"ZSBU3"))

# COMMAND ----------

df_ms_mm_au_af = df_ms_mm.join(af_au,on=['AUFNR'], how='left')

# COMMAND ----------

# selecting required columns and calculaing conversion values
df1 = df_ms_mm_au_af.alias('ms').select(col("ms.AUFNR").alias("PRODUCTION_ORDER"),col("ms.MATNR").alias("matl_key"), col("ms.WERKS").alias("plant_key"), col("ms.LGORT").alias("storage_loc"),  col("ms.BWART").alias("movement_type"), col("ms.SOBKZ").alias("special_stock"), col("ms.MBLNR").alias("matl_doc"), col("ms.MJAHR").alias("matl_doc_year"), col("ms.ZEILE").alias("doc_item"), col("ms.ERFMG").alias("qty_key"), col("ms.ERFME"), col("ms.LIFNR").alias("vendor_info"), col("ms.BUDAT_MKPF").alias("post_date_key"), col("ms.UMREZ"),col("ms.UMREN"))

df2 = df1.selectExpr("PRODUCTION_ORDER","matl_key", "plant_key", "storage_loc","qty_key","ERFME","matl_doc_year","post_date_key", "matl_doc","doc_item", "UMREZ", "UMREN","(UMREN/UMREZ) as unit_weights", "((UMREN/UMREZ) * qty_key) as total_unit_weights", "(((UMREN/UMREZ) * qty_key)/1000) as weights_mt")

df3 = df2.selectExpr("PRODUCTION_ORDER","matl_key", "plant_key", "matl_doc","doc_item","matl_doc_year","storage_loc","qty_key","post_date_key", "unit_weights", "total_unit_weights","weights_mt")

# COMMAND ----------

# calculating conversion value based on zsbu3 
ls_final = df3.join(dim_product.alias("prod"), on =[trim(col("matl_key")) == trim(col("material_number"))],how='left').select([col("" + cols) for cols in df3.columns]+[col("MATERIAL_TYPE"),col("UNIT_OF_MEASURE"),col("PRODUCT_LINE")])

ls_final1 = ls_final.alias('ls').join(zsbu3,[trim(df3["matl_key"]) == trim(zsbu3["matnr"]),
                                                trim(df3["plant_key"]) == trim(zsbu3["werks"])],
                                                how='left').select(
  [col("ls." + cols) for cols in ls_final.columns] + [col("STD_WT").alias("ZSBU3_STD_WT"),col("VALID_FROM"),col("VALID_TO")]
).where((col("post_date_key").between(col("VALID_FROM"),col("VALID_TO"))) | ((col("VALID_FROM").isNull()) & (col("VALID_TO").isNull()))).drop("VALID_FROM","VALID_TO")


ls_final2 = ls_final1.withColumn("unit_weights",when((col("unit_weights").isNull()) & (((col("UNIT_OF_MEASURE") == "EA") & (col("MATERIAL_TYPE") == "ZHLB"))|(col("Product_line") == "PIPES & FITTINGS")) & (col("ZSBU3_STD_WT").isNotNull()),col("ZSBU3_STD_WT")).otherwise(col("unit_weights"))).drop(col("ZSBU3_STD_WT"))

ls_final3 = ls_final2.withColumn("total_unit_weights",col("qty_key")*col("unit_weights"))

final_df = ls_final3.withColumn("weights_mt",when(col("plant_key").isin(["2010","2013","2018","2022"]),col("total_unit_weights")).otherwise(col("total_unit_weights")/1000))

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'PRODUCTION', mode='overwrite')

# COMMAND ----------

# --------------------SURROGATE IMPLEMENTATION------------------------

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
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
{fact_name}.PRODUCTION_ORDER,
matl_doc as MATL_DOC
,doc_item as DOC_ITEM
,matl_doc_year as MATL_DOC_YEAR
,qty_key as QUANTITY
,unit_weights AS UNIT_WEIGHT
,total_unit_weights as TOTAL_UNIT_WEIGHT
,WEIGHTS_MT
,storage_loc as STORAGE_LOCATION
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

# Generating surrogate keys
d = surrogate_mapping_hil(csv_data, table_name,"PRODUCTION", processed_db_name)

# COMMAND ----------

#  writing data to processed layer location after generting surrogate keys
d.write.mode('overwrite').parquet(processed_location+"FACT_LOGISTICS_PRODUCTION")

# COMMAND ----------

# inserting log record to last_execution_details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
