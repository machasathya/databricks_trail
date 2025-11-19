# Databricks notebook source
# import statemets
from datetime import datetime 
from pyspark.sql import *
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col,lit,when,count,unix_timestamp,upper, max as max_
from pyspark.sql.functions import *
from pyspark.sql import Window
from ProcessMetadataUtility import ProcessMetadataUtility

# COMMAND ----------

# increasing broadcast timeout to 36000 seconds
spark.conf.set("spark.sql.broadcastTimeout",36000)

# COMMAND ----------

# code to fetch the data from ADF Parameters
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
table_name = "FACT_SALES_TARGETS"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the code manually
# from ProcessMetadataUtility import ProcessMetadataUtility
# pmu = ProcessMetadataUtility()
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# table_name = "FACT_SALES_TARGETS"
# curated_db_name='cur_prod'
# processed_db_name='pro_prod'
# processed_location = 'mnt/bi_datalake/prod/pro/'

# COMMAND ----------

# reading targets data from curated layer and date data from processed layer
sales_df =spark.sql("select * from {}.{}".format(curated_db_name, "SALES_TARGETS")).withColumn("TARGET_TYPE",upper(col("TARGET_TYPE"))).drop("LAST_UPDATED_DT_TS")
date_df=spark.sql("select * from {}.{}".format(processed_db_name, "DIM_DATE"))

# COMMAND ----------

# joining tatgets data with date data to fetch date, week,financial year, quarter, half year and month
temp_df= sales_df.alias("targ").join(date_df.alias("dt"),on = [col("TARGET_DATE") == col("DATE_DT")],how = 'inner').select([col("targ." + cols) for cols in sales_df.columns] + [col("dt.DATE_ID"), col("dt.WK_NAME"),col("FY_NAME"),col("FY_QTR_OF_YR"),col("FY_HF_YR_OF_YR"),col("FY_MONTH_ID")])

# COMMAND ----------

# selecting req columns and filtering the data which is at a daily level
daily_df=temp_df.select("SBU","PRODUCT_LINE","SALE_TYPE","GEO_UNIT_TYPE","GEO_UNIT","SALES_TARGETS","NSR_TARGETS","QUANTITY_TARGETS","TARGET_DATE","FILE_DATE").where("TARGET_TYPE == 'DAILY'")

# COMMAND ----------

# selecting req columns and filtering the data which is at a weekly level
weekly_df=temp_df.select("SBU","PRODUCT_LINE","SALE_TYPE","GEO_UNIT_TYPE","GEO_UNIT","SALES_TARGETS","NSR_TARGETS","QUANTITY_TARGETS","TARGET_DATE","WK_NAME","FY_MONTH_ID","FILE_DATE").where("TARGET_TYPE == 'WEEKLY'")
targ_2 = weekly_df.alias("wk").join(date_df.alias("dt"),on = [col("wk.WK_NAME") == col("dt.WK_NAME"),col("wk.FY_MONTH_ID") == col("dt.FY_MONTH_ID")],how = 'inner').select([col("wk."+cols) for cols in weekly_df.columns] + [col("dt.DATE_DT")])

# COMMAND ----------

# partition on target date
w = Window.partitionBy('TARGET_DATE')
targ_2_final = targ_2.select('SBU', 'PRODUCT_LINE','SALE_TYPE','GEO_UNIT_TYPE',"GEO_UNIT",'SALES_TARGETS','NSR_TARGETS','QUANTITY_TARGETS','DATE_DT','FILE_DATE', count('TARGET_DATE').over(w).alias('Days'))
targ_2_final = targ_2_final.withColumn("SALES_TARGETS",col("SALES_TARGETS")/col("Days")).withColumn("QUANTITY_TARGETS",col("QUANTITY_TARGETS")/col("Days")).withColumnRenamed("DATE_DT","TARGET_DATE").drop("Days")

# COMMAND ----------

# selecting req columns and filtering the data which is at a monthly level
monthly_df=temp_df.select("SBU","PRODUCT_LINE","SALE_TYPE","GEO_UNIT_TYPE","GEO_UNIT","SALES_TARGETS","NSR_TARGETS","QUANTITY_TARGETS","TARGET_DATE","FY_MONTH_ID","FILE_DATE").where("TARGET_TYPE == 'MONTHLY'")

# COMMAND ----------

# joining monthly df with date df
targ_3 = monthly_df.alias("mnth").join(date_df.alias("dt"),on = [col("mnth.FY_MONTH_ID") == col("dt.FY_MONTH_ID")],how = 'inner').select([col("mnth."+cols) for cols in monthly_df.columns] + [col("dt.DATE_DT")])

# COMMAND ----------

# partition on target date
X = Window.partitionBy('TARGET_DATE')
targ_3_final = targ_3.select('SBU', 'PRODUCT_LINE','SALE_TYPE','GEO_UNIT_TYPE','GEO_UNIT','SALES_TARGETS','NSR_TARGETS','QUANTITY_TARGETS','DATE_DT','FILE_DATE', count('TARGET_DATE').over(X).alias('Days'))
targ_3_final = targ_3_final.withColumn("SALES_TARGETS",col("SALES_TARGETS")/col("Days")).withColumn("QUANTITY_TARGETS",col("QUANTITY_TARGETS")/col("Days")).withColumnRenamed("DATE_DT","TARGET_DATE").drop("Days")

# COMMAND ----------

# selecting req columns and filtering the data which is at a quarterly level
qrtrly_df=temp_df.select("SBU","PRODUCT_LINE","SALE_TYPE","GEO_UNIT_TYPE","GEO_UNIT","SALES_TARGETS","NSR_TARGETS","QUANTITY_TARGETS","TARGET_DATE","FILE_DATE","FY_QTR_OF_YR").where("TARGET_TYPE == 'QUARTERLY'")

# COMMAND ----------

# joining quarter df with date df
targ_4 = qrtrly_df.alias("qrtr").join(date_df.alias("dt"),on = [col("qrtr.FY_QTR_OF_YR") == col("dt.FY_QTR_OF_YR")],how = 'inner').select([col("qrtr."+cols) for cols in qrtrly_df.columns] + [col("dt.DATE_DT")])

# COMMAND ----------

# partition on target date
X = Window.partitionBy('TARGET_DATE')
targ_4_final = targ_4.select('SBU', 'PRODUCT_LINE','SALE_TYPE','GEO_UNIT_TYPE','GEO_UNIT','SALES_TARGETS','NSR_TARGETS','QUANTITY_TARGETS','DATE_DT','FILE_DATE', count('TARGET_DATE').over(X).alias('Days'))
targ_4_final = targ_4_final.withColumn("SALES_TARGETS",col("SALES_TARGETS")/col("Days")).withColumn("QUANTITY_TARGETS",col("QUANTITY_TARGETS")/col("Days")).withColumnRenamed("DATE_DT","TARGET_DATE").drop("Days")

# COMMAND ----------

# selecting req columns and filtering the data which is at a Half yearly level
hfyrly_df=temp_df.select("SBU","PRODUCT_LINE","SALE_TYPE","GEO_UNIT_TYPE","GEO_UNIT","SALES_TARGETS","NSR_TARGETS","QUANTITY_TARGETS","TARGET_DATE","FILE_DATE","FY_HF_YR_OF_YR").where("TARGET_TYPE == 'HALFYEARLY'")

# COMMAND ----------

# joining half year df with date df
targ_5 = hfyrly_df.alias("hf").join(date_df.alias("dt"),on = [col("hf.FY_HF_YR_OF_YR") == col("hf.FY_HF_YR_OF_YR")],how = 'inner').select([col("hf."+cols) for cols in hfyrly_df.columns] + [col("dt.DATE_DT")])

# COMMAND ----------

# partition on target date
X = Window.partitionBy('TARGET_DATE')
targ_5_final = targ_5.select('SBU', 'PRODUCT_LINE','SALE_TYPE','GEO_UNIT_TYPE','GEO_UNIT','SALES_TARGETS','NSR_TARGETS','QUANTITY_TARGETS','DATE_DT','FILE_DATE', count('TARGET_DATE').over(X).alias('Days'))
targ_5_final = targ_5_final.withColumn("SALES_TARGETS",col("SALES_TARGETS")/col("Days")).withColumn("QUANTITY_TARGETS",col("QUANTITY_TARGETS")/col("Days")).withColumnRenamed("DATE_DT","TARGET_DATE").drop("Days")

# COMMAND ----------

# selecting req columns and filtering the data which is at a yearly level
yearly_df=temp_df.select("SBU","PRODUCT_LINE","SALE_TYPE","GEO_UNIT_TYPE","GEO_UNIT","SALES_TARGETS","NSR_TARGETS","QUANTITY_TARGETS","TARGET_DATE","FY_NAME","FILE_DATE").where("TARGET_TYPE == 'YEARLY'")

# COMMAND ----------

# joining yearly df with date df
targ_6= yearly_df.alias("yr").join(date_df.alias("dt"),on = [col("yr.FY_NAME") == col("yr.FY_NAME")],how = 'inner').select([col("yr."+cols) for cols in yearly_df.columns] + [col("dt.DATE_DT")])

# COMMAND ----------

# partition on target date
X = Window.partitionBy('TARGET_DATE')
targ_6_final = targ_6.select('SBU', 'PRODUCT_LINE','SALE_TYPE','GEO_UNIT_TYPE','GEO_UNIT','SALES_TARGETS','NSR_TARGETS','QUANTITY_TARGETS','DATE_DT','FILE_DATE', count('TARGET_DATE').over(X).alias('Days'))
targ_6_final = targ_6_final.withColumn("SALES_TARGETS",col("SALES_TARGETS")/col("Days")).withColumn("QUANTITY_TARGETS",col("QUANTITY_TARGETS")/col("Days")).withColumnRenamed("DATE_DT","TARGET_DATE").drop("Days")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS data_trg;

# COMMAND ----------

# MAGIC %py
# MAGIC spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# DBTITLE 1,Union of all target types converted to daily
union_df = daily_df.union(targ_2_final).union(targ_3_final).union(targ_4_final).union(targ_5_final).union(targ_6_final)
union_df = union_df.fillna({"SALE_TYPE":"NA"})
union_df.write.saveAsTable("data_trg")
inter_df =spark.sql("select a.SBU,a.PRODUCT_LINE,a.SALE_TYPE,a.GEO_UNIT_TYPE,a.GEO_UNIT,a.SALES_TARGETS,a.NSR_TARGETS,a.QUANTITY_TARGETS,a.TARGET_DATE,a.FILE_DATE from data_trg as a inner join(select SBU,PRODUCT_LINE,SALE_TYPE,GEO_UNIT_TYPE,GEO_UNIT,TARGET_DATE,max(FILE_DATE) as FILE_DATE from data_trg group by SBU,PRODUCT_LINE,SALE_TYPE,GEO_UNIT_TYPE,GEO_UNIT,TARGET_DATE ) as b on a.SBU=b.SBU and a.PRODUCT_LINE=b.PRODUCT_LINE and a.SALE_TYPE = b.SALE_TYPE and a.GEO_UNIT_TYPE = b.GEO_UNIT_TYPE and a.GEO_UNIT = b.GEO_UNIT and a.TARGET_DATE = b.TARGET_DATE and a.FILE_DATE = b.FILE_DATE")
input_df=inter_df.drop("FILE_DATE")

# COMMAND ----------

# generating columns req for geo hierarchy
fact_targ=input_df.select("*").distinct()

df_fact=fact_targ.withColumn("SBU",when(col("SBU")=="SBU 1","1000").when(col("SBU")=="SBU 2","2000").when(col("SBU")=="SBU 3","3000").otherwise(col("SBU")))
df_fact=df_fact.withColumn("PRODUCT_LINE",upper(col("PRODUCT_LINE"))).withColumn("SALE_TYPE",upper(col("SALE_TYPE"))).withColumn("GEO_UNIT_TYPE",upper(col("GEO_UNIT_TYPE")))
df_fact=df_fact.withColumn("PRODUCT_LINE",when(col("PRODUCT_LINE")=="WALLPUTTY","WALL PUTTY").otherwise(col("PRODUCT_LINE")))
df_fact=df_fact.withColumn("SALE_TYPE",when(col("SALE_TYPE")=="PROJECT","PROJECT").when(col("SALE_TYPE")=="RETAIL","RETAIL").otherwise("<DEFAULT>"))

df_fact =df_fact.withColumn('PINCODE',when(col("GEO_UNIT_TYPE")=="PINCODE",col("GEO_UNIT")).otherwise("<DEFAULT>"))\
                       .withColumn('DISTRICT',when(col("GEO_UNIT_TYPE")=="DISTRICT",col("GEO_UNIT")).when(col("GEO_UNIT_TYPE")=="PINCODE",lit(None).cast("string")).otherwise("<DEFAULT>"))\
                .withColumn('SALES_GROUP',when(((col("GEO_UNIT_TYPE")=="STATE")|(col("GEO_UNIT_TYPE")=="ZONE")|(col("GEO_UNIT_TYPE")=="COUNTRY")),"<DEFAULT>").when(((col("GEO_UNIT_TYPE")=="PINCODE")|(col("GEO_UNIT_TYPE")=="DISTRICT")),lit(None).cast("string")).otherwise(col("GEO_UNIT")))\
                .withColumn('STATE',when(((col("GEO_UNIT_TYPE")=="ZONE")|(col("GEO_UNIT_TYPE")=="COUNTRY")),"<DEFAULT>").when(((col("GEO_UNIT_TYPE")=="PINCODE")|(col("GEO_UNIT_TYPE")=="DISTRICT")|(col("GEO_UNIT_TYPE")=="SALES GROUP")),lit(None).cast("string")).otherwise(col("GEO_UNIT")))\
                .withColumn('ZONE',when(col("GEO_UNIT_TYPE")=="COUNTRY","<DEFAULT>").when(((col("GEO_UNIT_TYPE")=="PINCODE")|(col("GEO_UNIT_TYPE")=="DISTRICT")|(col("GEO_UNIT_TYPE")=="SALES GROUP")|(col("GEO_UNIT_TYPE")=="STATE")),lit(None).cast("string")).otherwise(col("GEO_UNIT")))\
                .withColumn('COUNTRY',when(col("GEO_UNIT_TYPE")=="COUNTRY",col("GEO_UNIT")).otherwise(lit(None).cast("string")))

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

# flattening geo hierarchy from pincode
pin_fact=df_fact.filter(df_fact['GEO_UNIT_TYPE']=='PINCODE').select('SBU','PRODUCT_LINE','SALE_TYPE','GEO_UNIT_TYPE','TARGET_DATE','SALES_TARGETS','NSR_TARGETS','QUANTITY_TARGETS','PINCODE')
targ_f1=pin_fact.alias("df1").join(final_geo_df.alias("df2"),on=[col("df1.SBU") == col("df2.SBU"),col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.SALE_TYPE") == col("df2.SALE_TYPE") ,col("df1.PINCODE") == col("df2.PINCODE")], how='left').select([col("df1." +columns)for columns in pin_fact.columns]+[col("df2.DISTRICT"),col("df2.SALES_GROUP"),col("df2.STATE"),col("df2.ZONE"),col("df2.COUNTRY")]).distinct()

# COMMAND ----------

# flattening geo hierarchy from district
dis_fact=df_fact.filter(df_fact['GEO_UNIT_TYPE']=='DISTRICT').select('SBU','PRODUCT_LINE','SALE_TYPE','GEO_UNIT_TYPE','TARGET_DATE','SALES_TARGETS','NSR_TARGETS','QUANTITY_TARGETS','PINCODE','DISTRICT')
targ_f2=dis_fact.alias("df1").join(final_geo_df.alias("df2"),on=[col("df1.SBU") == col("df2.SBU"),col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.SALE_TYPE") == col("df2.SALE_TYPE") ,col("df1.DISTRICT") == col("df2.DISTRICT")], how='left').select([col("df1." +columns)for columns in dis_fact.columns]+[col("df2.SALES_GROUP"),col("df2.STATE"),col("df2.ZONE"),col("df2.COUNTRY")]).distinct()

# COMMAND ----------

# flattening geo hierarchy from sales group
salgrp_fact=df_fact.filter(df_fact['GEO_UNIT_TYPE']=='SALES GROUP').select('SBU','PRODUCT_LINE','SALE_TYPE','GEO_UNIT_TYPE','TARGET_DATE','SALES_TARGETS','NSR_TARGETS','QUANTITY_TARGETS','PINCODE','DISTRICT','SALES_GROUP')
targ_f3=salgrp_fact.alias("df1").join(final_geo_df.alias("df2"),on=[col("df1.SBU") == col("df2.SBU"),col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.SALE_TYPE") == col("df2.SALE_TYPE") ,col("df1.SALES_GROUP") == col("df2.SALES_GROUP")], how='left').select([col("df1." +columns)for columns in salgrp_fact.columns]+[col("df2.STATE"),col("df2.ZONE"),col("df2.COUNTRY")]).distinct()

# COMMAND ----------

# flattening geo hierarchy from state
ste_fact=df_fact.filter(df_fact['GEO_UNIT_TYPE']=='STATE').select('SBU','PRODUCT_LINE','SALE_TYPE','GEO_UNIT_TYPE','TARGET_DATE','SALES_TARGETS','NSR_TARGETS','QUANTITY_TARGETS','PINCODE','DISTRICT','SALES_GROUP','STATE')
targ_f4=ste_fact.alias("df1").join(final_geo_df.alias("df2"),on=[col("df1.SBU") == col("df2.SBU"),col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.SALE_TYPE") == col("df2.SALE_TYPE") ,col("df1.STATE") == col("df2.STATE")], how='left').select([col("df1." +columns)for columns in ste_fact.columns]+[col("df2.ZONE"),col("df2.COUNTRY")]).distinct()

# COMMAND ----------

# flattening geo hierarchy from zone
zon_fact=df_fact.filter(df_fact['GEO_UNIT_TYPE']=='ZONE').select('SBU','PRODUCT_LINE','SALE_TYPE','GEO_UNIT_TYPE','TARGET_DATE','SALES_TARGETS','NSR_TARGETS','QUANTITY_TARGETS','PINCODE','DISTRICT','SALES_GROUP','STATE','ZONE')
targ_f5=zon_fact.alias("df1").join(final_geo_df.alias("df2"),on=[col("df1.SBU") == col("df2.SBU"),col("df1.PRODUCT_LINE") == col("df2.PRODUCT_LINE"),col("df1.SALE_TYPE") == col("df2.SALE_TYPE") ,col("df1.ZONE") == col("df2.ZONE")], how='left').select([col("df1." +columns)for columns in zon_fact.columns]+[col("df2.COUNTRY")]).distinct()

# COMMAND ----------

# flattening geo hierarchy from country
ctry_fact=df_fact.filter(df_fact['GEO_UNIT_TYPE']=='COUNTRY').select('SBU','PRODUCT_LINE','SALE_TYPE','GEO_UNIT_TYPE','TARGET_DATE','SALES_TARGETS','NSR_TARGETS','QUANTITY_TARGETS','PINCODE','DISTRICT','SALES_GROUP','STATE','ZONE','COUNTRY')
targ_f6=ctry_fact.select("*")

# COMMAND ----------

# DBTITLE 1,Union -------final_trgt_df is for targets SK MAPPING
final_targ_df=targ_f1.union(targ_f2).union(targ_f3).union(targ_f4).union(targ_f5).union(targ_f6)
# display(final_targ_df)
final_targ_df=final_targ_df.drop('GEO_UNIT_TYPE')
final_targ_df=final_targ_df.withColumnRenamed('SBU','SALES_ORG')
final_targ_df=final_targ_df.fillna( { 'PINCODE':"<DEFAULT>", 'DISTRICT':"<DEFAULT>",'SALES_GROUP':"<DEFAULT>",'STATE':"<DEFAULT>",'ZONE':"<DEFAULT>",'COUNTRY':"<DEFAULT>"  } )
final_targ_df=final_targ_df.withColumn("PINCODE_TXT",when(col("PINCODE")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("PINCODE"))).withColumn("DISTRICT_TXT",when(col("DISTRICT")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("DISTRICT"))).withColumn("SALES_GROUP_TXT",when(col("SALES_GROUP")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("SALES_GROUP"))).withColumn("STATE_TXT",when(col("STATE")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("STATE"))).withColumn("ZONE_TXT",when(col("ZONE")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("ZONE"))).withColumn("COUNTRY_TXT",when(col("COUNTRY")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("COUNTRY"))).withColumn("SALE_TYPE_SK",when(col("SALE_TYPE")=="<DEFAULT>","OTHER").otherwise(col("SALE_TYPE")))



# COMMAND ----------

# writing date to processed location before generating surrogate key
final_targ_df.write.mode('overwrite').parquet(processed_location+"FACT/SALES_TARGETS")

# COMMAND ----------

# joining existing territory data with newly coming territory data
inter_df=final_targ_df.withColumn("RLS",concat(col("PRODUCT_LINE"),lit('_'),col("STATE"),lit('_'),col("SALE_TYPE"))).select("SALES_ORG","PRODUCT_LINE","SALE_TYPE","PINCODE","DISTRICT","SALES_GROUP","STATE","ZONE","COUNTRY",lit(None).cast("string").alias("GEO_DISTRICT"),lit(None).cast("string").alias("GEO_STATE"),"PINCODE_TXT","DISTRICT_TXT","SALES_GROUP_TXT","STATE_TXT","ZONE_TXT","COUNTRY_TXT","RLS",lit(None).cast("string").alias("GEO_KEY")).distinct()
display(inter_df)
dim_ter=spark.sql("select * from {}.{}".format(processed_db_name, "DIM_TERRITORY"))  
diff_df=inter_df.alias("df1").join(dim_ter.alias("df2"),on=[col("df1.SALES_ORG")==col("df2.SALES_ORG"),col("df1.PRODUCT_LINE")==col("df2.PRODUCT_LINE"),col("df1.SALE_TYPE")==col("df2.SALE_TYPE"),col("df1.PINCODE")==col("df2.PINCODE"),col("df1.DISTRICT")==col("df2.DISTRICT"),col("df1.SALES_GROUP")==col("df2.SALES_GROUP"),col("df1.STATE")==col("df2.STATE"),col("df1.ZONE")==col("df2.ZONE"),col("df1.COUNTRY")==col("df2.COUNTRY")],how='leftouter').where("df2.SALES_ORG is NULL").select([col("df1." +columns)for columns in inter_df.columns])

geo_temp =dim_ter.unionAll(diff_df)

# COMMAND ----------

# DBTITLE 1,SK FOR GEO INCREMENTAL
primary_key = 'SALES_ORG|PRODUCT_LINE|SALE_TYPE|PINCODE|DISTRICT|SALES_GROUP|STATE|ZONE|COUNTRY'
processed_table_name = 'DIM_TERRITORY'
surrogate_key = 'GEO_KEY'

# COMMAND ----------

def create_surrogate_key(renamed_df):
#   newdf = dim_df.select(primary_key.split('|')).distinct()
#   df = newdf.withColumn(surrogate_key, monotonically_increasing_id()+1)
#   df = df.join(dim_df, on=primary_key.split('|'), how='right')
    
    merged_key_columns = ','.join(['processed_dim_table.{column} as {column}'.format(column = c) for c in primary_key.split('|')])
    merge_surrogate_columns = ''
    if surrogate_key:
      for c in surrogate_key.split('|'):
        processed_dim_table = spark.table("{processed_db_name}.{processed_table_name}".format(processed_db_name = processed_db_name, processed_table_name=processed_table_name))
        processed_dim_table.createOrReplaceTempView("processed_dim_table")
        max_surrogate_key = processed_dim_table.agg({"{c}".format(c=c): "max"}).collect()[0]
        max_surrogate_key = max_surrogate_key["max({c})".format(c=c)]
#         print(max_surrogate_key)
        if max_surrogate_key is None:
          max_surrogate_key = 0
        merge_surrogate_columns = ','+','.join(['ifnull(processed_dim_table.{column},monotonically_increasing_id()+1+{max_surrogate_key}) as {column}'.format(column = c,max_surrogate_key = max_surrogate_key)])
#         print(monotonically_increasing_id()+1+max_surrogate_key)
    create_primary_key_join_condition = ' and '.join(['curated_df.{c} = processed_dim_table.{c}'.format(c = c) for c in primary_key.split('|')])
#     curated_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, table_name))
    curated_df = renamed_df.drop('GEO_KEY')
    curated_df.createOrReplaceTempView("curated_df")
    merge_columns = ','.join(['curated_df.{column} as {column}'.format(column = c) for c in curated_df.columns])
#     primary_key_df = spark.sql("select * from processed_dim_table".format(column = merged_key_columns))
#     primary_key_df.createOrReplaceTempView("primary_key_df")
    query = spark.sql("select {col} {surrogate_columns} from curated_df left join processed_dim_table on {join_condition}".format(col = merge_columns, surrogate_columns = merge_surrogate_columns, join_condition =create_primary_key_join_condition))
    return query

# COMMAND ----------

# creating surrogate key
df= create_surrogate_key(geo_temp)

# COMMAND ----------

# writing final dim_territory data to processed location
df.write.mode('overwrite').parquet(processed_location+"DIM_TERRITORY")

# COMMAND ----------

# refreshing dim_territory data
spark.catalog.refreshTable("{db_name}.{table_name}".format(db_name = processed_db_name, table_name = "DIM_TERRITORY"))
spark.sql("refresh table {db_name}.{table_name}".format(db_name = processed_db_name, table_name = "DIM_TERRITORY"))

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

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
     A.SALE_TYPE = B.SALE_TYPE AND
     A.PINCODE = B.PINCODE AND
     A.DISTRICT = B.DISTRICT AND
     A.SALES_GROUP = B.SALES_GROUP AND
      A.STATE = B.STATE AND
      A.ZONE = B.ZONE AND
      A.COUNTRY = B.COUNTRY
     """.format(pro_db=processed_db_name)
  
  select_condition = select_condition[:-2]
  query = """select
    A.SALES_TARGETS,
A.NSR_TARGETS,
A.QUANTITY_TARGETS,
    case when( 
     A.PRODUCT_LINE is NULL OR
     A.SALES_ORG is NULL OR
     A.SALE_TYPE is NULL OR
     A.PINCODE is NULL OR
     A.DISTRICT is NULL OR
     A.SALES_GROUP is NULL OR
     A.STATE is NULL OR
     A.ZONE is NULL OR
     A.COUNTRY is NULL OR
     trim(A.PRODUCT_LINE)='' OR
     trim(A.SALES_ORG)='' OR
     trim(A.SALE_TYPE)='' OR
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
  
# creating surrogate keys
d = surrogate_mapping_hil(csv_data,"FACT_SALES_TARGETS","SALES_TARGETS",processed_db_name)

# COMMAND ----------

# writing data to processed location after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# inserting log record to last execution details table 
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
