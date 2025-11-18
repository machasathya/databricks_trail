# Databricks notebook source
# import statements
from datetime import datetime 
from pyspark.sql import *
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col,lit,when,upper,concat
from ProcessMetadataUtility import ProcessMetadataUtility

# COMMAND ----------

# creating class object
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
table_name = "FACT_LOGISTICS_SALES_TARGETS"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# Variables to run the notebook manually
# from ProcessMetadataUtility import ProcessMetadataUtility
# pmu = ProcessMetadataUtility()
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# table_name = "FACT_LOGISTICS_SALES_TARGETS"
# processed_location = "mnt/bi_datalake/prod/pro/"
# curated_db_name = "cur_prod"
# processed_db_name = "pro_prod"

# COMMAND ----------

# fetching surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# Reading data from curated layer
fact_targ =spark.sql("select * from {}.{}".format(curated_db_name, "LOGISTICS_SALES_TARGETS"))
fact_targ=fact_targ.drop('LAST_UPDATED_DT_TS')

# Creating all the required columns for generating geo keys
df_fact=fact_targ.withColumn("SBU",when(col("SBU")=="SBU1","1000").when(col("SBU")=="SBU 2","2000").when(col("SBU")=="SBU 3","3000").otherwise(col("SBU")))
df_fact=df_fact.withColumn("PRODUCT_LINE",upper(col("PRODUCT_LINE"))).withColumn("GEO_UNIT_TYPE",upper(col("GEO_UNIT_TYPE")))
df_fact =df_fact.withColumn('PINCODE',when(col("GEO_UNIT_TYPE")=="PINCODE",col("GEO_UNIT")).otherwise("<DEFAULT>"))\
                .withColumn('DISTRICT',when(col("GEO_UNIT_TYPE")=="DISTRICT",col("GEO_UNIT")).when(col("GEO_UNIT_TYPE")=="PINCODE",lit(None).cast("string")).otherwise("<DEFAULT>"))\
                .withColumn('SALES_GROUP',when(((col("GEO_UNIT_TYPE")=="STATE")|(col("GEO_UNIT_TYPE")=="ZONE")|(col("GEO_UNIT_TYPE")=="COUNTRY")),"<DEFAULT>").when(((col("GEO_UNIT_TYPE")=="PINCODE")|(col("GEO_UNIT_TYPE")=="DISTRICT")),lit(None).cast("string")).otherwise(col("GEO_UNIT")))\
                .withColumn('STATE',when(((col("GEO_UNIT_TYPE")=="ZONE")|(col("GEO_UNIT_TYPE")=="COUNTRY")),"<DEFAULT>").when(((col("GEO_UNIT_TYPE")=="PINCODE")|(col("GEO_UNIT_TYPE")=="DISTRICT")|(col("GEO_UNIT_TYPE")=="SALES GROUP")),lit(None).cast("string")).otherwise(col("GEO_UNIT")))\
                .withColumn('ZONE',when(col("GEO_UNIT_TYPE")=="COUNTRY","<DEFAULT>").when(((col("GEO_UNIT_TYPE")=="PINCODE")|(col("GEO_UNIT_TYPE")=="DISTRICT")|(col("GEO_UNIT_TYPE")=="SALES GROUP")|(col("GEO_UNIT_TYPE")=="STATE")),lit(None).cast("string")).otherwise(col("GEO_UNIT")))\
                .withColumn('COUNTRY',when(col("GEO_UNIT_TYPE")=="COUNTRY",col("GEO_UNIT")).otherwise(lit(None).cast("string")))
# display(df_fact)

# COMMAND ----------

# DBTITLE 1,Territory from curated
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
    final_geo_df=final_df.withColumn("RLS",concat(col("PRODUCT_LINE"),lit('_'),col("STATE"),lit('_'),col("SALE_TYPE")))

# COMMAND ----------

# extracting pincode information and joining with final_geo_df
pin_fact=df_fact.filter(df_fact['GEO_UNIT_TYPE']=='PINCODE').select('SBU','PRODUCT_LINE','GEO_UNIT_TYPE','TARGET_DATE','SALES_TARGETS','PINCODE')
targ_f1=pin_fact.alias("df1").join(final_geo_df.alias("df2"),on=[col("df1.SBU") == col("df2.SBU"),col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.PINCODE") == col("df2.PINCODE")], how='left').select([col("df1." +columns)for columns in pin_fact.columns]+[col("df2.DISTRICT"),col("df2.SALES_GROUP"),col("df2.STATE"),col("df2.ZONE"),col("df2.COUNTRY"),col("df2.RLS")]).distinct()

# COMMAND ----------

# extracting district information and joining with final_geo_df
dis_fact=df_fact.filter(df_fact['GEO_UNIT_TYPE']=='DISTRICT').select('SBU','PRODUCT_LINE','GEO_UNIT_TYPE','TARGET_DATE','SALES_TARGETS','PINCODE','DISTRICT')
targ_f2=dis_fact.alias("df1").join(final_geo_df.alias("df2"),on=[col("df1.SBU") == col("df2.SBU"),col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.DISTRICT") == col("df2.DISTRICT")], how='left').select([col("df1." +columns)for columns in dis_fact.columns]+[col("df2.SALES_GROUP"),col("df2.STATE"),col("df2.ZONE"),col("df2.COUNTRY"),col("df2.RLS")]).distinct()

# COMMAND ----------

# extracting sales group information and joining with final_geo_df
salgrp_fact=df_fact.filter(df_fact['GEO_UNIT_TYPE']=='SALES GROUP').select('SBU','PRODUCT_LINE','GEO_UNIT_TYPE','TARGET_DATE','SALES_TARGETS','PINCODE','DISTRICT','SALES_GROUP')
targ_f3=salgrp_fact.alias("df1").join(final_geo_df.alias("df2"),on=[col("df1.SBU") == col("df2.SBU"),col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.SALES_GROUP") == col("df2.SALES_GROUP")], how='left').select([col("df1." +columns)for columns in salgrp_fact.columns]+[col("df2.STATE"),col("df2.ZONE"),col("df2.COUNTRY"),col("df2.RLS")]).distinct()

# COMMAND ----------

# extracting state information and joining with final_geo_df
ste_fact=df_fact.filter(df_fact['GEO_UNIT_TYPE']=='STATE').select('SBU','PRODUCT_LINE','GEO_UNIT_TYPE','TARGET_DATE','SALES_TARGETS','PINCODE','DISTRICT','SALES_GROUP','STATE')
targ_f4=ste_fact.alias("df1").join(final_geo_df.alias("df2"),on=[col("df1.SBU") == col("df2.SBU"),col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.STATE") == col("df2.STATE")], how='left').select([col("df1." +columns)for columns in ste_fact.columns]+[col("df2.ZONE"),col("df2.COUNTRY"),col("df2.RLS")]).distinct()

# COMMAND ----------

# extracting zone information and joining with final_geo_df
zon_fact=df_fact.filter(df_fact['GEO_UNIT_TYPE']=='ZONE').select('SBU','PRODUCT_LINE','GEO_UNIT_TYPE','TARGET_DATE','SALES_TARGETS','PINCODE','DISTRICT','SALES_GROUP','STATE','ZONE')
targ_f5=zon_fact.alias("df1").join(final_geo_df.alias("df2"),on=[col("df1.SBU") == col("df2.SBU"),col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.ZONE") == col("df2.ZONE")], how='left').select([col("df1." +columns)for columns in zon_fact.columns]+[col("df2.COUNTRY"),col("df2.RLS")]).distinct()

# COMMAND ----------

# extracting country information and joining with final_geo_df
ctry_fact=df_fact.filter(df_fact['GEO_UNIT_TYPE']=='COUNTRY').select('SBU','PRODUCT_LINE','GEO_UNIT_TYPE','TARGET_DATE','SALES_TARGETS','PINCODE','DISTRICT','SALES_GROUP','STATE','ZONE','COUNTRY',lit(None).cast("string").alias("RLS"))
targ_f6=ctry_fact.select("*")

# COMMAND ----------

# DBTITLE 1,Union -------final_trgt_df is for targets SK MAPPING
final_targ_df=targ_f1.union(targ_f2).union(targ_f3).union(targ_f4).union(targ_f5).union(targ_f6)
final_targ_df=final_targ_df.drop('GEO_UNIT_TYPE')
final_targ_df=final_targ_df.withColumnRenamed('SBU','SALES_ORG')
final_targ_df=final_targ_df.fillna( { 'PINCODE':"<DEFAULT>", 'DISTRICT':"<DEFAULT>",'SALES_GROUP':"<DEFAULT>",'STATE':"<DEFAULT>",'ZONE':"<DEFAULT>",'COUNTRY':"<DEFAULT>"  } )
final_targ_df=final_targ_df.withColumn("PINCODE_TXT",when(col("PINCODE")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("PINCODE"))).withColumn("DISTRICT_TXT",when(col("DISTRICT")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("DISTRICT"))).withColumn("SALES_GROUP_TXT",when(col("SALES_GROUP")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("SALES_GROUP"))).withColumn("STATE_TXT",when(col("STATE")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("STATE"))).withColumn("ZONE_TXT",when(col("ZONE")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("ZONE"))).withColumn("COUNTRY_TXT",when(col("COUNTRY")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("COUNTRY")))

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_targ_df.write.mode('overwrite').parquet(processed_location+"LOGISTICS_SALES_TARGETS")

# COMMAND ----------

# union of existing data with incremental data
inter_df=final_targ_df.select("SALES_ORG","PRODUCT_LINE",lit(None).cast("string").alias("SALE_TYPE"),"PINCODE","DISTRICT","SALES_GROUP","STATE","ZONE","COUNTRY",lit(None).cast("string").alias("GEO_DISTRICT"),lit(None).cast("string").alias("GEO_STATE"),"PINCODE_TXT","DISTRICT_TXT","SALES_GROUP_TXT","STATE_TXT","ZONE_TXT","COUNTRY_TXT","RLS",lit(None).cast("string").alias("GEO_KEY")).distinct()
inter_df=inter_df.fillna({'SALE_TYPE':"<DEFAULT>"})
dim_ter=spark.sql("select * from {}.{}".format(processed_db_name, "DIM_TERRITORY"))  
diff_df=inter_df.alias("df1").join(dim_ter.alias("df2"),on=[col("df1.SALES_ORG")==col("df2.SALES_ORG"),col("df1.PRODUCT_LINE")==col("df2.PRODUCT_LINE"),col("df1.SALE_TYPE")==col("df2.SALE_TYPE"),col("df1.PINCODE")==col("df2.PINCODE"),col("df1.DISTRICT")==col("df2.DISTRICT"),col("df1.SALES_GROUP")==col("df2.SALES_GROUP"),col("df1.STATE")==col("df2.STATE"),col("df1.ZONE")==col("df2.ZONE"),col("df1.COUNTRY")==col("df2.COUNTRY")],how='leftouter').where("df2.SALES_ORG is NULL").select([col("df1." +columns)for columns in inter_df.columns])
geo_temp =dim_ter.unionAll(diff_df) 

# COMMAND ----------

# DBTITLE 1,SK FOR GEO INCREMENTAL
# defining primary keys
primary_key = 'SALES_ORG|PRODUCT_LINE|SALE_TYPE|PINCODE|DISTRICT|SALES_GROUP|STATE|ZONE|COUNTRY'
processed_table_name = 'DIM_TERRITORY'
surrogate_key = 'GEO_KEY'

# COMMAND ----------

# creating surrogate keys for dim_territory
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
        merge_surrogate_columns = ','+','.join(['ifnull(processed_dim_table.{column},monotonically_increasing_id()+1+{max_surrogate_key}) as {column}'.format(column = c,max_surrogate_key = max_surrogate_key)])
    create_primary_key_join_condition = ' and '.join(['curated_df.{c} = processed_dim_table.{c}'.format(c = c) for c in primary_key.split('|')])
    curated_df = renamed_df.drop('GEO_KEY')
    curated_df.createOrReplaceTempView("curated_df")
    merge_columns = ','.join(['curated_df.{column} as {column}'.format(column = c) for c in curated_df.columns])
    query = spark.sql("select {col} {surrogate_columns} from curated_df left join processed_dim_table on {join_condition}".format(col = merge_columns, surrogate_columns = merge_surrogate_columns, join_condition =create_primary_key_join_condition))
    return query

# COMMAND ----------

# creating surrogate key
df= create_surrogate_key(geo_temp)

# COMMAND ----------

# writing updated dim_territory data to processed location
df.write.mode('overwrite').parquet(processed_location+"DIM_TERRITORY")

# COMMAND ----------

# Refresh dim_territory table
spark.catalog.refreshTable("{db_name}.{table_name}".format(db_name = processed_db_name, table_name = "DIM_TERRITORY"))

# COMMAND ----------

#SURROGATE KEY IMPLEMENTATION FOR FACT_SALES_TARGETS - INCLUDING LOGIC FOR GEO-KEY--

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
  
  final_targ_df.createOrReplaceTempView("{}".format(fact_name))
  
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
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
          
      join_condition += "\n left join {pro_db}.{dim_table} on A.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
  join_condition = join_condition + """ \n
  left join {pro_db}.dim_territory B
  ON A.PRODUCT_LINE = B.PRODUCT_LINE AND
     A.SALES_ORG = B.SALES_ORG AND
     A.PINCODE = B.PINCODE AND
     A.DISTRICT = B.DISTRICT AND
     A.SALES_GROUP = B.SALES_GROUP AND
      A.STATE = B.STATE AND
      A.ZONE = B.ZONE AND
      A.COUNTRY = B.COUNTRY
     """.format(pro_db=processed_db_name)
  
  select_condition = select_condition[:-2]
  query = """select
  A.SALES_ORG,
    A.SALES_TARGETS,
    case when( 
     A.PRODUCT_LINE is NULL OR
     A.SALES_ORG is NULL OR
     A.PINCODE is NULL OR
     A.DISTRICT is NULL OR
     A.SALES_GROUP is NULL OR
     A.STATE is NULL OR
     A.ZONE is NULL OR
     A.COUNTRY is NULL OR
     trim(A.PRODUCT_LINE)='' OR
     trim(A.SALES_ORG)='' OR
     trim(A.PINCODE)='' OR
     trim(A.DISTRICT)='' OR
     trim(A.SALES_GROUP)=''  OR
     trim(A.STATE)=''  OR
     trim(A.ZONE)='' OR
     trim(A.COUNTRY)='') THEN -2 ELSE ifnull(B.GEO_KEY, -1) END as GEO_KEY
  ,{select_condition}  from {fact_name} A {join_condition}""".format(
               join_condition=join_condition,fact_name=fact_name,select_condition=select_condition)
  
  print("\nFinal Query: " +query) 
  fact_final_view_surrogate = spark.sql(query)
  cols = []
  
  for item in fact_final_view_surrogate.columns:
    cols.append( "{} AS {}".format(item ,item.upper()))
  fact_final_view_surrogate = fact_final_view_surrogate.selectExpr(cols)
  #print(cols)
  return fact_final_view_surrogate
  
# generating surrogate keys
d = surrogate_mapping_hil(csv_data,"FACT_LOGISTICS_SALES_TARGETS","SALES_TARGETS",processed_db_name)

# COMMAND ----------

# writing data to processed layer location after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+"FACT_LOGISTICS_SALES_TARGETS")

# COMMAND ----------

# inserting log record to last_execution_details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
