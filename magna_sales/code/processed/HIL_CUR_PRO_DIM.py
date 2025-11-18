# Databricks notebook source
# Import statements
import json
from pyspark.sql.functions import *
import pandas as pd
import numpy as np
from pyspark.sql import *
import pyodbc
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col,countDistinct,lpad
from pyspark.sql.functions import first
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, IntegerType, FloatType, LongType, \
    DecimalType, DateType, TimestampType
from ProcessMetadataUtility import ProcessMetadataUtility

# COMMAND ----------

# Commands used to get the input values from ADF
table_name = dbutils.widgets.get("table_name")
processed_location = dbutils.widgets.get("base_processed_location")
column_metadata_table= dbutils.widgets.get("column_meta_table")
curated_db_name = dbutils.widgets.get('curated_db_name')
processed_table_name = dbutils.widgets.get("processed_table_name")
processed_db_name = dbutils.widgets.get('processed_db_name')
primary_key = dbutils.widgets.get('primary_key')
surrogate_key = dbutils.widgets.get('surrogate_key')
schema_name = dbutils.widgets.get("schema_name")
db_url = dbutils.widgets.get("db_url")
db = dbutils.widgets.get("db")
user_name = dbutils.widgets.get("user_name")
scope = dbutils.widgets.get('scope')
password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])

# COMMAND ----------

# Variables used to run the notebook manually
# table_name = 'J_1ICHPTER'
# processed_location = 'dbfs:/mnt/bi_datalake/prod/pro/DIM_CHAPTER/'
# curated_db_name = 'cur_prod'
# processed_db_name = 'pro_prod'
# processed_table_name = 'DIM_CHAPTER'
# primary_key ='CHAPTER_ID'
# surrogate_key = 'CHAPTER_KEY'
# column_metadata_table = "column_meta"
# schema_name = "metadata_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])

# COMMAND ----------

# Initializing class object
pmu = ProcessMetadataUtility()

# COMMAND ----------

# Establishng connection to SQL and fetching the data from column_meta table
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
column_metadata_df = pmu.get_data(cursor=db_cursor, col_lookup=column_metadata_table, value=table_name, column="TABLE_NAME",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# utility function to convert the column name based on the mapping stored in column_meta table
def hil_rename_column(table_name, column_metadata):
    """
    Fun to rename the column from cur to process
    """
    try:
        filter_table_list = column_metadata.toJSON().collect()
        for source_table_item in filter_table_list:
            source_table_item_dict = json.loads(source_table_item)
            column_name = source_table_item_dict['DESTINATION_COLUMN_NAME']
            if 'PROCESSED_COLUMN_NAME' in source_table_item_dict and source_table_item_dict['PROCESSED_COLUMN_NAME'] != "":
              table_name = table_name.withColumnRenamed(column_name, source_table_item_dict['PROCESSED_COLUMN_NAME'])
            else:
              table_name = table_name.drop(column_name)
        return table_name
    except Exception as e:
        print(e)
        return table_name

# COMMAND ----------

# Function used to create the surrogate_key based on the primary Key(s)
def create_surrogate_key(renamed_df):
    merged_key_columns = ','.join(['processed_dim_table.{column} as {column}'.format(column = c) for c in primary_key.split('|')])
    merge_surrogate_columns = ''
    if surrogate_key:
      for c in surrogate_key.split('|'):
        processed_dim_table = spark.table("{processed_db_name}.{processed_table_name}".format(processed_db_name = processed_db_name, processed_table_name=processed_table_name))
        processed_dim_table.createOrReplaceTempView("processed_dim_table")
        max_surrogate_key = processed_dim_table.agg({"{c}".format(c=c): "max"}).collect()[0]
        max_surrogate_key = max_surrogate_key["max({c})".format(c=c)]
        if max_surrogate_key is None:
          max_surrogate_key = 0
        merge_surrogate_columns = ','+','.join(['ifnull(processed_dim_table.{column},monotonically_increasing_id()+1+{max_surrogate_key}) as {column}'.format(column=c,max_surrogate_key= max_surrogate_key)])
        print(merge_surrogate_columns)
    create_primary_key_join_condition = ' and '.join(['curated_df.{c} = processed_dim_table.{c}'.format(c = c) for c in primary_key.split('|')])
    curated_df = renamed_df
    curated_df.createOrReplaceTempView("curated_df")
    merge_columns = ','.join(['curated_df.{column} as {column}'.format(column = c) for c in curated_df.columns])
    query = spark.sql("select {col} {surrogate_columns} from curated_df left join processed_dim_table on {join_condition}".format(col = merge_columns, surrogate_columns = merge_surrogate_columns, join_condition =create_primary_key_join_condition))    
    return query

# COMMAND ----------

# Function used to create final processed_layer table after performing transformation(if required) and column renaming and generating surrogate keys
def create_processed_table(processed_table_name, column_metadata):
  if processed_table_name == 'DIM_CUSTOMER_GROUP':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "T151T"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "T151T"))
    dim_t151t_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = dim_t151t_df.filter("SPRAS = 'E'")
    processed_dim_table = hil_rename_column(processed_dim_table, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table )
  
  if processed_table_name == 'DIM_PURCHASE_ORG':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "T024E"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "T024E"))
    dim_pur_org_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_pur_org_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_PURCHASE_GROUP':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "T024"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "T024"))
    dim_pur_org_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_pur_org_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_VENDOR':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "LFA1"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "LFA1"))
    acc_grp = spark.sql("select * from {}.{}".format(curated_db_name,"t077y")).filter("SPRAS = 'E'")
    dim_vendor_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_vendor_df = dim_vendor_df.filter("SPRAS = 'E'")
    dim_vendor_df = dim_vendor_df.alias("df1").join(acc_grp.alias("df2"),on=[col("df1.ktokk") == col("df2.ktokk")],how='left').select([col("df1." +columns)for columns  in dim_vendor_df.columns] + [col("df2.TXT30").alias("ACCOUNT_GROUP")]).drop("ktokk")
    processed_dim_table = hil_rename_column(dim_vendor_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
  
  if processed_table_name == 'DIM_PRODUCT_GROUP':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "T023T"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "T023T"))
    dim_t023t_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_t023t_df = dim_t023t_df.filter("SPRAS = 'E'")
    processed_dim_table = hil_rename_column(dim_t023t_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_DISTRIBUTION_CHANNEL':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "TVTWT"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "TVTWT"))
    dim_tvtwt_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_tvtwt_df = dim_tvtwt_df.filter("SPRAS = 'E'")
    processed_dim_table = hil_rename_column(dim_tvtwt_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_PLANT_DEPOT':
    cur_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "T001W"))
    cur_df=cur_df.drop('LAST_UPDATED_DT_TS')
    df_fb = spark.sql("select * from {}.{}".format(curated_db_name, "LOGISTICS_BUDGETARY_UNIT"))
    df_dp_fb_o  = cur_df.alias('cur_df').join(df_fb.alias('df_fb'), on = [df_fb.DEPOT_ID == cur_df.WERKS], how                              ='outer').withColumn("REPORT_PLANT_ID",when(col("df_fb.ATTACHED_PLANT_ID").isNull(),col('cur_df.WERKS')).otherwise(col('df_fb.ATTACHED_PLANT_ID'))).drop(col('df_fb.ATTACHED_PLANT_ID'))
    df_plant_name =  spark.sql("SELECT WERKS, NAME1 FROM {}.{}".format(curated_db_name, "T001W"))
    df_plant_name= df_plant_name.drop('LAST_UPDATED_DT_TS')
    curated_df  = df_plant_name.alias('df_plant_name').join(df_dp_fb_o.alias('df_dp_fb_o'), on = [df_dp_fb_o.REPORT_PLANT_ID == df_plant_name.WERKS], how ='INNER').withColumn("REPORT_PLANT_NAME",col("df_plant_name.NAME1")).drop(col("df_plant_name.NAME1")).drop(col("df_plant_name.WERKS"))
    dim_t001w_df = curated_df.withColumn("IS_PLANT",when(col("WERKS") == 4404, 1).when(col("WERKS").substr(0,1).isin([2]) & col('EKORG').isNotNull() & col('VKORG').isNotNull(),1).when(col("WERKS").substr(0,1).isin([ 3, 4, 5, 6, 7, 9 ]), 0).otherwise(3))
    dpo_plnt_df=spark.sql("SELECT * FROM {}.{}".format(curated_db_name,"ACTIVE_PLANT_DEPOT"))
#     t001w_df=dim_t001w_df.alias("df1").join(dpo_plnt_df.alias("df2"),on=[col("df1.WERKS") == col("df2.PLANT_DEPOT_ID")],how='left').select([col("df1." +columns)for columns in dim_t001w_df.columns]+[col("df2.PRODUCT_LINE"),col("df2.PLANT_DEPOT_ID"),col("POSTAL_CODE")]).withColumn("PSTLZ",when(col("df1.WERKS")==col("df2.PLANT_DEPOT_ID"),col("POSTAL_CODE")).otherwise(col("PSTLZ"))).withColumn("IS_ACTIVE",when(((col("df1.WERKS") == col("df2.PLANT_DEPOT_ID")) | (col("df1.WERKS")!=col("df1.REPORT_PLANT_ID"))),1).otherwise(0))
    t001w_df=dim_t001w_df.alias("df1").join(dpo_plnt_df.alias("df2"),on=[col("df1.WERKS") == col("df2.PLANT_DEPOT_ID")],how='left').withColumn("PSTLZ",when(col("df1.WERKS")==col("df2.PLANT_DEPOT_ID"),col("POSTAL_CODE")).otherwise(col("PSTLZ"))).withColumn("IS_ACTIVE",when(((col("df1.WERKS") == col("df2.PLANT_DEPOT_ID")) | (col("df1.WERKS")!=col("df1.REPORT_PLANT_ID"))),1).otherwise(0))
    t001w_df= t001w_df.drop('POSTAL_CODE')
    t001w_df = t001w_df.drop('PLANT_DEPOT_ID')
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "T001W"))
    t001w_df= t001w_df.drop('LAST_UPDATED_DT_TS')
    t001w_df = t001w_df.filter("SPRAS = 'E'")
    region_df = spark.sql("Select * from {}.{}".format(curated_db_name,"T005U")).filter("SPRAS = 'E'")
    processed_dim_table = hil_rename_column(t001w_df, column_metadata)
    processed_dim_table = processed_dim_table.alias("l_df").join(region_df.alias("r_df"),on = [col("l_df.COUNTRY") == col("r_df.land1"),col("l_df.region") == col("r_df.bland")],how='left').select([col("l_df." +columns)for columns in processed_dim_table.columns]+[col("r_df.BEZEI").alias("region_desc")]).drop("region").withColumnRenamed('region_desc','REGION')
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
#     display(processed_dim_table)
    
  if processed_table_name == 'DIM_PRICING_GROUP':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "T188T"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "T188T"))
    dim_t188t_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_t188t_df = dim_t188t_df.filter("SPRAS = 'E'")
    processed_dim_table = hil_rename_column(dim_t188t_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
  
  if processed_table_name == 'DIM_CUSTOMER':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "KNA1"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "KNA1"))
    knvv_df = spark.sql('select kunnr,min(spart) as spart,min(vkbur) as vkbur from {}.{} group by kunnr'.format(curated_db_name,"KNVV"))
    tvkbt_df = spark.sql("""SELECT * FROM {}.{} where SPRAS='E'""".format(curated_db_name,"tvkbt"))
#     division_df = spark.read.format('csv').options(header='true').option('delimiter', ',').load('dbfs:/mnt/bi_datalake/temp/DIVISION_MAPPING/')
    division_df = spark.sql("select * from {}.{}".format(curated_db_name,"SBU_PRODUCT_MAPPING"))
    region_df = spark.sql("Select * from {}.{}".format(curated_db_name,"T005U")).filter("SPRAS = 'E'")
    kna1_df = curated_df.drop('LAST_UPDATED_DT_TS')
    cust_grp_df =  spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "CUSTOMER_GROUP_MAPPING")).drop('LAST_UPDATED_DT_TS')
    cust_grp_df = cust_grp_df.withColumn("CUSTOMER_ID",lpad(col("CUSTOMER_ID"),10,'0')).withColumn("GROUP_ID",lpad(col("GROUP_ID"),10,'0'))
    dim_kna1_df = kna1_df.alias("kn").join(cust_grp_df.alias("cst"),on =[col("kn.KUNNR") == col("cst.CUSTOMER_ID")],how = 'left').select([col("kn." +columns)for columns in kna1_df.columns] + [col("cst.GROUP_ID")]).withColumn("GROUP_ID",when(col("GROUP_ID").isNull(),col("KUNNR")).otherwise(col("GROUP_ID")))
#     dim_kna1_df = dim_kna1_df.filter("SPRAS = 'E'")
    processed_dim_table = hil_rename_column(dim_kna1_df, column_metadata)
    processed_dim_table = processed_dim_table.alias("l_df").join(region_df.alias("r_df"),on = [col("l_df.COUNTRY") == col("r_df.land1"),col("l_df.region") == col("r_df.bland")],how='left').select([col("l_df." +columns)for columns in processed_dim_table.columns]+[col("r_df.BEZEI").alias("region_desc")]).drop("region").withColumnRenamed('region_desc','REGION')
    processed_dim_table = processed_dim_table.alias("l_df").join(knvv_df.alias("r_df"),on = [col("l_df.cust_id") == col("r_df.kunnr")],how= 'left').select([col("l_df." +columns)for columns in processed_dim_table.columns] + [col("r_df.SPART").alias("DIVISION"),col("VKBUR")])
    processed_dim_table = processed_dim_table.alias("l_df").join(tvkbt_df.alias("r_df"),on = [col("l_df.VKBUR") == col("r_df.VKBUR")],how='left').select([col("l_df." +columns)for columns in processed_dim_table.columns] + [col("r_df.BEZEI")]).drop("VKBUR")
    processed_dim_table = processed_dim_table.withColumn("SALES_TYPE",when(col("BEZEI").like("%Export%"),"EXPORTS")
                               .when(col("BEZEI").like("%Project%"),"PROJECT")
                               .when(col("BEZEI").like("%South%"),"RETAIL")
                               .when(col("BEZEI").like("%North%"),"RETAIL")
                               .when(col("BEZEI").like("%East%"),"RETAIL")
                               .when(col("BEZEI").like("%West%"),"RETAIL").otherwise("<DEFAULT>")).drop("BEZEI")
    processed_dim_table = processed_dim_table.withColumn("DIVISION",when((col("DIVISION").isNull()),"-1").otherwise(col("DIVISION")))
    processed_dim_table = processed_dim_table.alias("l_df").join(division_df.alias("r_df"),on=[col("l_df.division") == col("r_df.division")],how='left').select([col("l_df." +columns)for columns in processed_dim_table.columns] + [col("r_df.SBU").alias("OUTSTANDING_SBU"),col("PRODUCT_LINE_FOR_OUTSTANDING").alias("OUTSTANDING_PRODUCT_LINE")])
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
                                              
  if processed_table_name == 'DIM_BUSINESS_ADDRESS':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "ADRC"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "ADRC"))
    dim_adrc_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_adrc_df = dim_adrc_df.filter("LANGU = 'E'")
    processed_dim_table = hil_rename_column(dim_adrc_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_SALES_ORG':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "TVKO"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "TVKO"))
    dim_tvko_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_tvko_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_DIVISION':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "TSPAT"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "TSPAT"))
    dim_tspat_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_tspat_df = dim_tspat_df.filter("SPRAS = 'E'")
    processed_dim_table = hil_rename_column(dim_tspat_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_SALES_OFFICE':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "TVKBT"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "TVKBT"))
    dim_tvkbt_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_tvkbt_df = dim_tvkbt_df.filter("SPRAS = 'E'")
    processed_dim_table = hil_rename_column(dim_tvkbt_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_SALES_GROUP':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "TVGRT"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "TVGRT"))
    dim_tvgrt_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_tvgrt_df = dim_tvgrt_df.filter("SPRAS = 'E'")
    processed_dim_table = hil_rename_column(dim_tvgrt_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_PRODUCT_PRICING_GROUP':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "T178T"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "T178T"))
    dim_t178t_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_t178t_df = dim_t178t_df.filter("SPRAS = 'E'")
    processed_dim_table = hil_rename_column(dim_t178t_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_FREIGHT_GROUP':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "TMFGT"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "TMFGT"))
    dim_tmfgt_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_tmfgt_df = dim_tmfgt_df.filter("SPRAS = 'E'")
    processed_dim_table = hil_rename_column(dim_tmfgt_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_ROUTE':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "TVROT"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "TVROT"))
    dim_tvrot_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_tvrot_df = dim_tvrot_df.filter("SPRAS = 'E'")
    processed_dim_table = hil_rename_column(dim_tvrot_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_PERSONNEL':
    pers_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "PA0001"))
    agg_pers_df=pers_df.select("PERNR","BEGDA").groupby("PERNR").agg(max("BEGDA").alias("b"))
    final_df=pers_df.alias("df1").join(agg_pers_df.alias("df2"),on=[col("df1.PERNR") == col("df2.PERNR"),col("df1.BEGDA") == col("df2.b")], how='inner').select([col("df1." +columns)for columns in pers_df.columns])
    curated_df=final_df
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "PA0001"))
    dim_pa0001_df = curated_df.drop('LAST_UPDATED_DT_TS')
#     display(dim_pa0001_df.filter(dim_pa0001_df['PERNR']=='50002450'))
    processed_dim_table = hil_rename_column(dim_pa0001_df, column_metadata)
#     display(processed_dim_table.filter(processed_dim_table['PER_NUMBER']=='50002450'))
  
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_PRODUCT_GROUP_DERIVED':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "PRODUCT_GROUP_DERIVED"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "PRODUCT_GROUP_DERIVED"))
    dim_product_group_derived_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_product_group_derived_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)

  if processed_table_name == 'DIM_PRODUCT':
    df= spark.sql("SELECT trim(OBJEK) as OBJEK,trim(ATINN) as ATINN,trim(ATWRT) as ATWRT,trim(KLART) as KLART FROM {}.{} where KLART = '001'".format(curated_db_name, "AUSP"))
    df1= spark.sql("SELECT trim(ATINN) as ATINN,trim(ATNAM) as ATNAM FROM {}.{}".format(curated_db_name, "CABN"))
    matl_dept_df=spark.sql("Select * from {}.{}".format(curated_db_name,"MATL_DEPT_MAPPING"))
    matl_dept_df = matl_dept_df.withColumn('MATERIAL_NUMBER', lpad(matl_dept_df.MATERIAL_NUMBER,18,'0'))
    t134t = spark.sql("SELECT MTART,MTBEZ FROM {}.{} WHERE SPRAS='E'".format(curated_db_name, "t134t"))
    t023t = spark.sql("SELECT MATKL,WGBEZ FROM {}.{} WHERE SPRAS='E'".format(curated_db_name, "t023t"))
    aspire_rm_grouping = spark.sql("select * from {}.{}".format(curated_db_name,"ASPIRE_RM_GROUPING"))
    aspire_rm_grouping = aspire_rm_grouping.withColumn('MATERIAL_ID', lpad(aspire_rm_grouping.MATERIAL_ID,18,'0'))
    lf= df.join(df1, df.ATINN == df1.ATINN,how='left').select(df["*"],df1["ATNAM"])
    lf=lf.withColumn("ATNAM",when(((trim(col("ATNAM"))=="ZSBU3_001_BU")|(trim(col("ATNAM"))=="ZSBU2_001_BU")|(trim(col("ATNAM"))=="ZSBU1_001_BU")|(trim(col("ATNAM"))== "ZSBU4_001_BU")|(trim(col("ATNAM"))== "ZSBU5_001_BU")),"SBU")
                     .when(((trim(col("ATNAM"))=="ZSBU3_002_PRODUCTLINE")|(trim(col("ATNAM"))=="ZSBU2_002_PRODUCTLINE")|(trim(col("ATNAM"))=="ZSBU1_002_PRODUCTLINE")|(trim(col("ATNAM")) == "ZSBU4_002_PRODUCTLINE")|(trim(col("ATNAM")) == "ZSBU5_002_PRODUCTLINE")),"PRODUCT_LINE")
                     .when(((trim(col("ATNAM"))=="ZSBU3_003_BRAND")|(trim(col("ATNAM"))=="ZSBU2_003_BRAND") |(trim(col("ATNAM"))=="ZSBU1_003_BRAND")|(trim(col("ATNAM")) == "ZSBU5_003_BRAND")),"BRAND")
                     .when(((trim(col("ATNAM"))=="ZSBU3_004_CHANNEL")|(trim(col("ATNAM"))=="ZSBU2_004_CHANNEL")|(trim(col("ATNAM"))=="ZSBU1_004_CHANNEL")|(trim(col("ATNAM")) == "ZSBU5_004_CHANNEL")),"CHANNEL")
                     .when(trim(col("ATNAM"))=="ZSBU1_005_COLOURTYPE","COLOUR_TYPE")
                     .when(trim(col("ATNAM"))=="ZSBU3_005_PRODUCTMIX","PRODUCT_MIX")
                     .when(((trim(col("ATNAM"))=="ZSBU2_005_CATEGORY")|(trim(col("ATNAM"))=="ZSBU1_008_CATEGORY")|(trim(col("ATNAM")) == "ZSBU5_005_CATEGORY")),"CATEGORY")
                     .when(trim(col("ATNAM"))=="ZSBU1_006_COLOUR","COLOUR")
                     .when(trim(col("ATNAM"))=="ZSBU2_006_SUBCATEGORY","SUB_CATEGORY")
                     .when(((trim(col("ATNAM"))=="ZSBU3_006_PRODUCTTYPE")|(trim(col("ATNAM"))=="ZSBU1_007_PRODUCTTYPE")|(trim(col("ATNAM"))=="ZSBU2_007_PRODUCTTYPE")|(trim(col("ATNAM")) == "ZOTHR_001_PRODTYPE")),"PRODUCT_TYPE")
                     .when(((trim(col("ATNAM"))=="ZSBU3_007_PRODUCTGROUP")|(trim(col("ATNAM")) == "ZOTHR_001_PRODGROUP")),"PRODUCT_GROUP")
                     .when(trim(col("ATNAM"))=="ZSBU3_008_ASSEMBLYTYPE","ASSEMBLY_TYPE")
                     .when(((trim(col("ATNAM"))=="ZSBU1_009_THICKNESS")|(trim(col("ATNAM"))=="ZSBU2_010_THICKNESS")),"THICKNESS")
                     .when(trim(col("ATNAM"))=="ZSBU3_009_SIZE","SIZE")
                     .when(trim(col("ATNAM"))=="ZSBU3_010_SOCKETTYPE","SOCKET_TYPE")
                     .when(trim(col("ATNAM"))=="ZSBU3_011_FITTINGSTYPE","FITTINGS_TYPE")
                     .when(trim(col("ATNAM"))=="ZSBU2_008_LENGTH","LENGTH")
                     .when(trim(col("ATNAM"))=="ZSBU2_009_WIDTH","WIDTH")
                     .when(trim(col("ATNAM"))=="ZOTHR_001_FGPRODTYPE","FG_MATERIAL_GROUP").otherwise(trim(col("ATNAM"))))
    lf= lf.groupBy("OBJEK").pivot("ATNAM").agg(first("ATWRT"))
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "MARA"))
    matl_desc = spark.sql("SELECT * FROM {}.{} WHERE SPRAS='E'".format(curated_db_name, "MAKT"))
    curated_df = curated_df.dropDuplicates(['MATNR'])
    matl_desc = matl_desc.dropDuplicates(['MATNR'])
    curated_df = curated_df.join(matl_desc,trim(curated_df.MATNR) == trim(matl_desc.MATNR),how='LEFT').select(curated_df["*"],matl_desc["MAKTX"].alias("MATERIAL_DESCRIPTION"))
    curated_df = curated_df.join(lf,trim(curated_df.MATNR) == trim(lf.OBJEK),how='left').select(curated_df["*"],lf["*"])
    curated_df = curated_df.join(matl_dept_df,trim(curated_df.MATNR) == trim(matl_dept_df.MATERIAL_NUMBER),how='left').select(curated_df["*"],matl_dept_df["DEPARTMENT"])
    curated_df=curated_df.drop("OBJEK")
    curated_df=curated_df.withColumn('PRODUCT_TYPE',upper(col('PRODUCT_TYPE'))).withColumn('CHANNEL',upper(col('CHANNEL'))).withColumn('PRODUCT_LINE',upper(col('PRODUCT_LINE')))
    curated_df=curated_df.withColumn('SKU_NUMBER', when(curated_df.PRODUCT_TYPE == '1 KG',1).when(curated_df.PRODUCT_TYPE == '5 KG',2).when(curated_df.PRODUCT_TYPE == '10 KG',3).when(curated_df.PRODUCT_TYPE == '20 KG',4).when(curated_df.PRODUCT_TYPE == '40 KG',5).when(curated_df.PRODUCT_TYPE == 'PROJECT PUTTY',6).when(curated_df.PRODUCT_TYPE == 'COARSER PUTTY',7).otherwise(0))
    clsfctn_grp = curated_df.withColumn ("CLASSIFICATION_GROUP",when(trim(col("MTART")).isin(["ROH","ZROH"]),"RAW_MATERIAL")
                                            .when(trim(col("MTART")) == "ERSA","STORES & SPARES")
                                            .when(trim(col("MTART")) == "ZHLB", "SEMI FINISHED GOODS")
                                            .when(trim(col("MTART")) == "VERP","PRINTING & PACKING")
                                            .when(trim(col("MTART")) == "ZPRT","OTHER MATERIAL")
                                            .when(trim(col("MTART")) == "ZSCR","SCRAP")
                                            .when(((trim(col("MTART")).isin(["ZHAW","ZFRT"])) & (trim(col("ASSEMBLY_TYPE")) == "Assembled") & (trim(col("SBU")) == "SBU 3")),"ASSEMBLED FINISHED GOODS")
                                            .when(((trim(col("MTART")).isin(["ZHAW","ZFRT"])) & (trim(col("SBU")) == "SBU 3")),"FINISHED GOODS"))
    clsfctn_grp = clsfctn_grp.withColumn("FG_MATERIAL_GROUP",when(((trim(col("CLASSIFICATION_GROUP")).isin(["ASSEMBLED FINISHED GOODS","FINISHED GOODS"])) & (trim(col("SBU")) == "SBU 3")),col("PRODUCT_TYPE")).otherwise(col("FG_MATERIAL_GROUP")))
    aspire_grp = clsfctn_grp.join(aspire_rm_grouping,on = [trim(clsfctn_grp.MATNR) == trim(aspire_rm_grouping.MATERIAL_ID)],how='left').select(clsfctn_grp["*"],aspire_rm_grouping["RM_GROUP"],aspire_rm_grouping["RM_GROUP2"])
    matl_type_desc = aspire_grp.join(t134t,on= [aspire_grp.MTART == t134t.MTART],how='left').select(aspire_grp["*"],t134t["MTBEZ"].alias("MATL_TYPE_DESC"))
    matl_grp_desc = matl_type_desc.join(t023t,on= [matl_type_desc.MATKL == t023t.MATKL],how='left').select(matl_type_desc["*"],t023t["WGBEZ"].alias("MATL_GROUP_DESC"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "MARA"))
    dim_product_df = matl_grp_desc.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_product_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
    
  if processed_table_name == 'DIM_SALE_TYPE':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "SALE_TYPE"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "SALE_TYPE"))
    dim_sale_type_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_sale_type_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
  
  if processed_table_name == 'DIM_DATE':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "DIM_DATE"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "DIM_DATE"))
    dim_sale_type_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_sale_type_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_FIRST_TIME_SALES':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "FIRST_TIME_SALES"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "FIRST_TIME_SALES"))
    dim_sale_type_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_sale_type_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)

  if processed_table_name == 'DIM_TERRITORY':
    ter_df =spark.sql("select * from {}.{}".format(curated_db_name, "TERRITORY"))
    ter_df =ter_df.withColumn("GEO_UNIT_TYPE",upper(col("GEO_UNIT_TYPE")))
    ter_clms = ['SBU','PRODUCT_LINE','SALE_TYPE','GEO_UNIT_TYPE','GEO_UNIT','PARENT_GEO_UNIT']
    df=ter_df[ter_clms].toPandas()
    pin_dis=df.loc[df['GEO_UNIT_TYPE']== 'PINCODE'].drop(['GEO_UNIT_TYPE'], axis=1)
    dis_sales=df.loc[df['GEO_UNIT_TYPE']== 'DISTRICT'].drop(['GEO_UNIT_TYPE'], axis=1)
    sales_state=df.loc[df['GEO_UNIT_TYPE']== 'SALES GROUP'].drop(['GEO_UNIT_TYPE'], axis=1)
    state_zone=df.loc[df['GEO_UNIT_TYPE']== 'STATE'].drop(['GEO_UNIT_TYPE'], axis=1)
    zone_country=df.loc[df['GEO_UNIT_TYPE']== 'ZONE'].drop(['GEO_UNIT_TYPE'], axis=1)
    pin_dis.columns=['SBU','PRODUCT_LINE','SALE_TYPE','PINCODE','DISTRICT']
    dis_sales.columns=['SBU','PRODUCT_LINE','SALE_TYPE','DISTRICT','SALES_GROUP']
    sales_state.columns=['SBU','PRODUCT_LINE','SALE_TYPE','SALES_GROUP','STATE']
    state_zone.columns=['SBU','PRODUCT_LINE','SALE_TYPE','STATE','ZONE']
    zone_country.columns=['SBU','PRODUCT_LINE','SALE_TYPE','ZONE','COUNTRY']
    fin_pd=pd.merge(pin_dis,dis_sales,how='left')
    fin_pd=pd.merge(fin_pd,sales_state,how='left')
    fin_pd=pd.merge(fin_pd,state_zone,how='left')
    fin_pd=pd.merge(fin_pd,zone_country,how='left')
    final_ter=spark.createDataFrame(fin_pd)
    geo_df=spark.sql("select * from {}.{}".format(curated_db_name, "GEO_STRUCTURE"))
    final_df=final_ter.alias("df1").join(geo_df.alias("df2"),on=[col("df1.PINCODE") == col("df2.PINCODE")], how='left').select([col("df1." +columns)for columns in final_ter.columns]+[col("df2.ACTUAL_DISTRICT"),col("df2.ACTUAL_STATE")])
    final_df=final_df.withColumn("SBU",when(col("SBU")=="SBU 1","1000").when(col("SBU")=="SBU 2","2000").when(col("SBU")=="SBU 3","3000").otherwise(col("SBU")))
    final_df=final_df.withColumn("PRODUCT_LINE",upper(col("PRODUCT_LINE"))).withColumn("SALE_TYPE",upper(col("SALE_TYPE")))
    final_df=final_df.withColumn("SALE_TYPE",when(col("SALE_TYPE")=="PROJECT","PROJECT").when(col("SALE_TYPE")=="RETAIL","RETAIL").otherwise("<DEFAULT>"))
    final_df=final_df.withColumn("PINCODE_TXT",final_df['PINCODE'])\
                     .withColumn("DISTRICT_TXT",final_df['DISTRICT'])\
                     .withColumn("SALES_GROUP_TXT",final_df['SALES_GROUP'])\
                     .withColumn("STATE_TXT",final_df['STATE'])\
                     .withColumn("ZONE_TXT",final_df['ZONE'])\
                     .withColumn("COUNTRY_TXT",final_df['COUNTRY'])
    final_df=final_df.fillna( { 'PINCODE':"<DEFAULT>", 'DISTRICT':"<DEFAULT>",'SALES_GROUP':"<DEFAULT>",'STATE':"<DEFAULT>",'ZONE':"<DEFAULT>",'COUNTRY':"<DEFAULT>"  } ) 
    final_df=final_df.withColumn("RLS",concat(col("PRODUCT_LINE"),lit('_'),col("STATE"),lit('_'),col("SALE_TYPE")))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "TERRITORY"))
    dim_geo_mapping_df = final_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_geo_mapping_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)  
    
  if processed_table_name == 'DIM_PRODUCT_LINE':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "PRODUCT_LINE"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "PRODUCT_LINE"))
    dim_sale_type_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_sale_type_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_STORAGE_LOCATION':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "T001L"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "T001L"))
    dim_t001l_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_t001l_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
  
  if processed_table_name == 'DIM_DEFECT':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "qpct"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "QPCT"))
    dim_qpct_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_qpct_df = dim_qpct_df.filter("SPRACHE = 'E'")
    processed_dim_table = hil_rename_column(dim_qpct_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_INSPECTION_CHARS':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "qpmt"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "QPMT"))
    dim_qpct_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_qpct_df = dim_qpct_df.filter("SPRACHE = 'E'")
    processed_dim_table = hil_rename_column(dim_qpct_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_CHAPTER':    
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "J_1ICHPTER"))        
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "J_1ICHPTER"))
    dim_chptr_df = curated_df.drop('LAST_UPDATED_DT_TS')
    dim_lang_df = spark.sql("select * from {}.{}".format(curated_db_name,"J_1ICHIDTX"))
    dim_lang_df = dim_lang_df.filter("LANGU = 'E'")
    curated_df = dim_chptr_df.join(dim_lang_df,dim_chptr_df.J_1ICHID == dim_lang_df.J_1ICHID,how='LEFT').select(dim_chptr_df["*"],dim_lang_df["J_1ICHT1"].alias("CHAPTER_DESCRIPTION"))
    processed_dim_table = hil_rename_column(curated_df, column_metadata)
    dim_chapter_created = processed_dim_table.groupBy('CHAPTER_ID').agg(min("CHANGED_ON").alias("CREATED_DATE"))
    dim_chapter_created.write.format("parquet").mode("overwrite").save("dbfs:/mnt/bi_datalake/prod/pro/DIM_CHAPTER_CREATED")
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
    
  if processed_table_name == 'DIM_SERVICE':
    asmd_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "asmd"))
    desc_df = spark.sql("SELECT * FROM {}.{} WHERE SPRAS='E'".format(curated_db_name, "asmdt"))
    t025t = spark.sql("SELECT BKLAS,BKBEZ FROM {}.{} WHERE SPRAS='E'".format(curated_db_name, "t025t"))
    t023t = spark.sql("SELECT MATKL,WGBEZ FROM {}.{} WHERE SPRAS='E'".format(curated_db_name, "t023t"))
    curated_df = asmd_df.join(desc_df,asmd_df.ASNUM == desc_df.ASNUM,how='LEFT').select(asmd_df["*"],desc_df["ASKTX"].alias("ACTIVITY_DESCRIPTION"))
    curated_df = curated_df.join(t025t,curated_df.BKLAS == t025t.BKLAS,how= 'LEFT').select(curated_df["*"],t025t["BKBEZ"].alias("VALUATION_CLASS"))
    curated_df = curated_df.join(t023t,curated_df.MATKL == t023t.MATKL,how= 'LEFT').select(curated_df["*"],t023t["WGBEZ"].alias("MATL_GROUP"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "ASMD"))
    dim_asmd_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_asmd_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)   
    
  if processed_table_name == 'DIM_FPR_DETAIL':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "FPR_DETAIL"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "T001L"))
    dim_t001l_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_t001l_df, column_metadata)

  if processed_table_name == 'DIM_CUSTOM_INCOTERMS':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "CUSTOM_INCOTERMS"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "CUSTOM_INCOTERMS"))
    dim_inco_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_inco_df, column_metadata)
    
  if processed_table_name == 'DIM_GL_MASTER':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "GL_MASTER")).withColumn("GL_CODE",lpad(col("GL_CODE"),10,'0'))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "GL_MASTER"))
    dim_gl_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_gl_df, column_metadata)
    processed_dim_table = create_surrogate_key(renamed_df = processed_dim_table)
  
  if processed_table_name == 'DIM_STATE':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "STATE_LIST"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "DIM_STATE"))
    dim_state_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_state_df, column_metadata)
  
  if processed_table_name == 'DIM_MIS_PRODUCT_LINE':
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "MIS_SBU_PLANT_PRODUCT_MAPPING"))
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "MIS_SBU_PLANT_PRODUCT_MAPPING"))
    dim_mis_df = curated_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_mis_df, column_metadata)
    
  if processed_table_name == 'DIM_PRODUCTION':
    spark.sql("refresh table {}.{}".format(processed_db_name,"FACT_LOGISTICS_PRODUCTION"))
    spark.sql("refresh table {}.{}".format(processed_db_name,"DIM_PLANT_DEPOT"))
    curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "CAUFV"))
    date_df = spark.sql("select * from  {}.{}".format(curated_db_name,"DIM_DATE"))
    plant_df = spark.sql("select * from {}.{}".format(processed_db_name,"DIM_PLANT_DEPOT"))
    prod_qty_df = spark.sql("select PRODUCTION_ORDER,first(plant_key) as plant_key,sum(QUANTITY) as ACTUAL_QUANTITY_EA, sum(WEIGHTS_MT) as ACTUAL_QUANTITY_MT from {}.{} group by production_order ".format(processed_db_name,"FACT_LOGISTICS_PRODUCTION"))
    cur_df1 =  curated_df.alias('l_df').join(plant_df.alias("r_df"),on=[col("l_df.WERKS")==col("r_df.plant_id")],how='left').select(["l_df." + cols for cols in curated_df.columns] + [col("r_df.PLANT_NAME")])
    cur_df = cur_df1.alias("df1").join(date_df.alias("df2"),on=[col("df1.ERDAT") == col("df2.DATE_DT")]).select(["df1." + cols for cols in cur_df1.columns] + [col('FY_MONTH_OF_YR'),col("FY_MONTH_ID")])
    column_metadata = column_metadata_df.filter("TABLE_NAME = '{table_name}'".format(table_name = "CAUFV"))
    dim_prod_detail_df = cur_df.drop('LAST_UPDATED_DT_TS')
    processed_dim_table = hil_rename_column(dim_prod_detail_df, column_metadata)
  return processed_dim_table

# COMMAND ----------

# Function call to create final processed layer table
final_processed_dim_table = create_processed_table(processed_table_name,column_metadata_df)

# COMMAND ----------

# writing data to pro layer location
final_processed_dim_table.write.format("parquet").mode("overwrite").save(processed_location)
