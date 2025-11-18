# Databricks notebook source
# import statements
import time
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from datetime import datetime
from datetime import timedelta  
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date
from pyspark.sql import functions as sf

from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql.functions import col, lit, expr, to_date, to_timestamp, sum, min, when, count, concat,upper
import pandas as pd
import numpy as np

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# generating list of past 3 months dates
df = spark.sql("select current_date() as end_date, add_months(current_date(), -3) as  start_date")
df1 = df.collect()[0]

# COMMAND ----------

# defining start and end date
start_date = df1["start_date"].replace(day = 1).strftime("%Y-%m-%d")
end_date = df1["end_date"].strftime("%Y-%m-%d")

# COMMAND ----------

# defining run date 
now = datetime.now()
day_n = now.day
now_hour = now.time().hour
ist_zone = datetime.now() + timedelta(hours=5.5)
var_date = (ist_zone - timedelta(days=1)).strftime("%Y-%m-%d")

# COMMAND ----------

# enabling cross join and increasing broadcast timeout to 36000 seconds
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set("spark.sql.broadcastTimeout", 36000)

# COMMAND ----------

# variables to run the notebook manually
# pmu = ProcessMetadataUtility()
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# processed_db_name = "pro_prod"
# table_name = "FACT_PENDING_ORDERS_SNAPSHOT"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
# csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
# pmu.close(db_connector, db_cursor)

# COMMAND ----------

# code to fetch the parameters data from ADF
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
table_name = "FACT_PENDING_ORDERS_SNAPSHOT"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# DBTITLE 1,Sales Doc: Header data
VBAK = spark.sql("select * FROM {}.{}".format(curated_db_name, "VBAK")).select("VBELN","VKORG","KUNNR","AUART","VTWEG").where("VKORG = '3000'")

# COMMAND ----------

# reading vbpa data
VBPA = spark.sql("select * FROM {}.{}".format(curated_db_name, "VBPA"))

# COMMAND ----------

# DBTITLE 1,Billing Doc: Header data
VBRK = spark.sql("select * FROM {}.{}".format(curated_db_name, "VBRK"))

# COMMAND ----------

# DBTITLE 1,Sales Doc: Header status & admin data
VBUK = spark.sql("select * FROM {}.{}".format(curated_db_name, "VBUK")).select("VBELN", "GBSTK", "LFSTK", "SPSTG").where("GBSTK is not null  or  GBSTK != 'C'")

# COMMAND ----------

# DBTITLE 1,Delivery Item Data
LIPS = spark.sql("select * FROM {}.{}".format(curated_db_name, "LIPS")).select("VGPOS","POSNR", "LFIMG", "VGBEL", "NTGEW","VBELN")

LIPS.createOrReplaceTempView("lips")

upd_LIPS = spark.sql("select VBELN, POSNR, sum(LFIMG) as LFIMG, sum(NTGEW) as delivery_NTGEW from lips group by VBELN,POSNR ") # .where("VGBEL IS NOT NULL and VGPOS IS NOT NULL")

# COMMAND ----------

# DBTITLE 1,Plants / Branches
T001W = spark.sql("select * FROM {}.{}".format(curated_db_name, "T001W")).select("WERKS","NAME1", "VKORG","PSTLZ")

# COMMAND ----------

# reading vbfa data from curated layer
VBFA = spark.sql("select * FROM {}.{} ".format(curated_db_name, "VBFA")).select("VBELV","POSNV","POSNN","VBTYP_N","VBELN").where("VBTYP_N = 'J'")

# COMMAND ----------

# DBTITLE 1,Sales Doc: Business data
VBKD = spark.sql("select * FROM {}.{}".format(curated_db_name, "VBKD")).select("VBELN",'POSNR',"KDGRP","BZIRK","INCO1","INCO2")

# COMMAND ----------

# reading data from curated layer
TVAGT = spark.sql("select * from {}.{}".format(curated_db_name,"TVAGT"))

# COMMAND ----------

# DBTITLE 1,Sales Doc: Item data
VBAP_ALL_1 = spark.sql("select * FROM {}.{}".format(curated_db_name, "VBAP")).select("VBELN","POSNR","WERKS","NETWR","KWMENG","MATNR",
                                                               "ABGRU","KONDM", "BRGEW","GEWEI", "ERDAT","SPART","NTGEW").withColumn("unit_price",
                                                                            expr("NETWR/KWMENG")).where("ABGRU is null  or  ABGRU != 51").where("ERDAT >= '" + start_date + "' and ERDAT<= '" + end_date + "'")

VBAP_ALL_2 = VBAP_ALL_1.alias('vp').join (VBPA.alias('vbpa'),on=[col('vp.VBELN') == col('vbpa.VBELN') , col('vp.POSNR') == col('vbpa.POSNR')],how='LEFT').select([col("vp."+cols) for cols in VBAP_ALL_1.columns] + [col("vbpa.KUNNR").alias('SHIP_TO_CUST')])


VBAP_ALL = VBAP_ALL_2.alias('vp').join(TVAGT.alias('tvagt'),on=[col('vp.ABGRU') == col('tvagt.ABGRU')],how='LEFT').where(col('tvagt.SPRAS') == 'E').select([col("vp."+cols) for cols in VBAP_ALL_2.columns] + [col("tvagt.BEZEI")])


VBAP = VBAP_ALL.alias('vp').join(VBAK.alias('sales_header'), on=['VBELN'],how="INNER").select([col("vp."+cols) for cols in VBAP_ALL.columns] + [col("sales_header.KUNNR"),col("sales_header.VTWEG")])

# COMMAND ----------

# joining vbap and vbfa on document no and item
test = VBAP.alias('vp').join(VBFA.alias('vf'), on=[
                                               col('vf.VBELV') == col('vp.VBELN'),col('vf.POSNV') == col('vp.POSNR')], how='LEFT').select([col("vp."+cols) for cols in VBAP.columns] + [col("vf.POSNN"),col("vf.VBELN").alias("vf_vbeln")])

# COMMAND ----------

# extracting delivery value and aggregating the data on doc and item 
mast_df1 = test.alias('tes').join(upd_LIPS.alias('ul'), on=[
                                               col('ul.VBELN') == col('tes.vf_vbeln'),col('ul.POSNR') == col('tes.POSNN')], how='left').select(col('tes.VBELN').alias('VBELN'),col('tes.POSNR').alias('POSNR'),col('tes.NETWR').alias("NET_VALUE"),col('ul.LFIMG').alias("LFIMG"),col('ul.delivery_NTGEW').alias("delivery_NTGEW")).drop("vf_vbeln")

mast_df1.createOrReplaceTempView("sales_and_delivery")

agg_by_sales_order_line=spark.sql("select vbeln,posnr,sum(LFIMG) as LFIMG, sum(delivery_NTGEW) as delivery_NTGEW, sum(NET_VALUE) as NET_VALUE from sales_and_delivery group by  vbeln,posnr")

mast_df = VBAP.alias("vbap").join(agg_by_sales_order_line.alias("sales_delv_agg"),on=[col('vbap.VBELN')==col('sales_delv_agg.VBELN'),col('vbap.posnr')==col('sales_delv_agg.posnr')],how="LEFT").select([col("vbap."+cols) for cols in VBAP.columns] + [col("sales_delv_agg.LFIMG"),col("sales_delv_agg.delivery_NTGEW").alias("delivery_NTGEW"),col("sales_delv_agg.NET_VALUE")])

# COMMAND ----------

# joining master and vbuk data frames on doc no
gt_vbuk = mast_df.alias('ap').join(VBUK.alias('uk'), on=[
                                               col('ap.VBELN') == col('uk.VBELN')], how='left').select([col("ap."+cols) for cols in mast_df.columns] + [col("uk.SPSTG"),col("uk.GBSTK")])

# COMMAND ----------

# extracting plant name pincode and purchase org
df3 = gt_vbuk.join(T001W, on=['WERKS'], how='inner').select([col(cols) for cols in gt_vbuk.columns] +[col("NAME1"), col("VKORG"), col("PSTLZ")])

# COMMAND ----------

# exctacting customer group district incoterms
df_new = df3.join(VBKD, on=['VBELN','POSNR'], how='left').select([col(cols) for cols in df3.columns] +[col("KDGRP"),col("BZIRK"),col("INCO1"),col("INCO2")])

# COMMAND ----------

# selecting the columns and filling nulls with 0's
df4 = df_new.alias('vb').select([col("vb.VBELN").alias("sales_doc_id"), col("vb.POSNR").alias("sales_doc_line_id"), col("vb.KWMENG").alias('order_qty'), col("vb.MATNR").alias("matl_key"),col("GEWEI").alias("weight_unit"),col("KUNNR").alias("customer_key") ,col("vb.NTGEW").alias("net_wgt_info"), col("vb.WERKS").alias("plant_key"), col("vb.NAME1").alias("name1"), col("vb.PSTLZ").alias("post_code"), col("vb.VKORG").alias("sales_org"),col("vb.ERDAT").alias("created_date"), col("vb.BZIRK").alias("district"),col("vb.SPSTG"),col("vb.GBSTK"), col("vb.ABGRU").alias("rsn_rjctn"),col("vb.KONDM").alias("matl_pricing_group_key"),col("SPART").alias("division_id"), col("vb.KDGRP").alias("customer_group_key"),col("vb.INCO1"),col("vb.INCO2"),col("vb.unit_price").alias("unit_price"),col("vb.LFIMG").alias("actl_qty_delvrd"),col("vb.delivery_NTGEW").alias("actl_qty_delvrd_nt_wgt"),col("vb.VTWEG").alias("dist_chnl"),col("vb.NET_VALUE"),col("vb.BEZEI").alias("REASON_FOR_REJECTION"),col('vb.SHIP_TO_CUST')]).fillna({'actl_qty_delvrd': 0,'actl_qty_delvrd_nt_wgt':0})

df5 = df4.selectExpr("sales_doc_id", "sales_doc_line_id", "actl_qty_delvrd","district", "customer_key","division_id","sales_org","created_date","post_code","matl_key","customer_key","weight_unit","net_wgt_info","rsn_rjctn","SPSTG", "GBSTK", "order_qty", "plant_key", "name1", "matl_pricing_group_key", "SHIP_TO_CUST","customer_group_key", "INCO1", "INCO2","actl_qty_delvrd_nt_wgt", "(order_qty-nanvl(actl_qty_delvrd,0)) as balance_quantity", "unit_price","(net_wgt_info-nanvl(actl_qty_delvrd_nt_wgt,0)) as balance_net_weight", "(unit_price * (order_qty-nanvl(actl_qty_delvrd,0))) as balance_sales_value","dist_chnl","NET_VALUE","REASON_FOR_REJECTION")

df6 = df5.selectExpr("sales_doc_id", "sales_doc_line_id", "actl_qty_delvrd", "matl_key","division_id","net_wgt_info","post_code", "customer_key","created_date","sales_org","district","rsn_rjctn","weight_unit","SPSTG", "GBSTK", "plant_key", "name1","order_qty", "matl_pricing_group_key","actl_qty_delvrd_nt_wgt", "customer_group_key","INCO1","INCO2","SHIP_TO_CUST","unit_price", "balance_quantity", "balance_net_weight", "balance_sales_value","dist_chnl","NET_VALUE","REASON_FOR_REJECTION")

# COMMAND ----------

# creating different qty types based on spstg
final  = df6.withColumn("status", when((col("SPSTG").isin(["C"])), "pending").when(((col("SPSTG").isNull()) | (col("SPSTG").isin(["A"]))),"released"))
final  = final.withColumn("pending_qty", when((col("SPSTG").isin(["C"])), col("balance_quantity")).when(col("SPSTG").isin(["A"]),0).otherwise(0))
final  = final.withColumn("released_qty", when(((col("SPSTG").isNull()) | (col("SPSTG").isin(["A"]))), col("balance_quantity")).when(col("SPSTG").isin(["C"]),0).otherwise(0))
final  = final.withColumn("pending_sales_value", when((col("SPSTG").isin(["C"])), col("balance_sales_value")).when(col("SPSTG").isin(["A"]),0).otherwise(0))
final  = final.withColumn("released_sales_value", when(((col("SPSTG").isNull()) | (col("SPSTG").isin(["A"]))), col("balance_sales_value")).when(col("SPSTG").isin(["C"]),0).otherwise(0))

# COMMAND ----------

final.createOrReplaceTempView("temp_test")

# COMMAND ----------

# reading dim_product data from processed layer
dim_product = spark.sql("select * from {}.{}".format(processed_db_name,"dim_product"))

# COMMAND ----------

#extracting product line from dim_product table
final_ls  = final.alias("fn").join(dim_product.alias("dim_prod"), on=[col("fn.matl_key") == col("dim_prod.MATERIAL_NUMBER")], how='left').select([col("fn." + columns) for columns in final.columns] + [col("dim_prod.PRODUCT_LINE").alias("product_line")]).fillna({"product_line": "SBU3_OTHERS"})

# COMMAND ----------

# reading data from curated layer
kna1 = spark.sql("select kunnr,pstlz FROM {}.{}".format(curated_db_name, "KNA1"))

# COMMAND ----------

# reading customer pincode
final_ls_2 = final_ls.alias("ls_1").join(kna1.alias("kn"),on=[col("ls_1.customer_key") == col("kn.kunnr")],how = 'left').select([col("ls_1." + columns)for columns in final_ls.columns] + [col("kn.pstlz")])

# COMMAND ----------

# degining sales org type based on customer group and product line and distribution channel
final_1 = final_ls_2.withColumn("sales_org_type", when(((col('customer_group_key') == 'Y4') | (col('customer_group_key') == 'Y5')) & (col('dist_chnl') == 10) & (col('product_line') == "PIPES & FITTINGS"), "PROJECT").when(((col('customer_group_key') != 'Y4') | (col('customer_group_key') != 'Y5')) & (col('dist_chnl') != 10) & (col('product_line') == "PIPES & FITTINGS"), "RETAIL").otherwise("<DEFAULT>"))

# COMMAND ----------

# adding snapshot date
timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
timestamp
final_2 = final_1.withColumn("date",F.lit(datetime.strptime(var_date, '%Y-%m-%d').date()))

# COMMAND ----------

# adding geo district
done = final_2.withColumn('geo_bus_map', sf.concat(sf.col('sales_org'), sf.col('district'))).where('balance_quantity<>0')

# COMMAND ----------

# adding last executed time column
done_1=done.withColumn("last_executed_time", F.lit(last_processed_time_str)).withColumn("last_executed_date", F.to_date("last_executed_time"))

# COMMAND ----------

done_1.createOrReplaceTempView("temp_osr")

# COMMAND ----------

# reading territory data from curated layer and flattening it
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

# fetching sales group, district, geo district, state ,zone, country
done_1_df = done_1.alias("fact").join(final_geo_df.alias("Geo_data"), on = [
                                                                  done_1["sales_org"] == final_geo_df["SBU"],
                                                                                    done_1["product_line"] == final_geo_df["product_line"],
  done_1["SALES_ORG_TYPE"] == final_geo_df["SALE_TYPE"],
  done_1["PSTLZ"] == final_geo_df["PINCODE"]
], how = 'left').select(
  [col("fact." + cols) for cols in done_1.columns] + [col("Geo_data.DISTRICT").alias("geo_district"),col("Geo_data.SALES_GROUP"),col("Geo_data.STATE"),col("Geo_data.ZONE"),col("Geo_data.COUNTRY")])

# COMMAND ----------

 # SURROGATE KEY IMPLEMENTATION
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
  
  done_1_df.createOrReplaceTempView("{}".format(fact_name))
  
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
      #select_condition += fact_surrogate + ", "
      join_condition += "\n left join {pro_db}.{dim_table} on {fact_table}.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
  join_condition = join_condition + """ \n
  left join pro_prod.dim_territory B 
  ON
      ({fact_name}.SALES_ORG_TYPE = B.SALE_TYPE) AND
      (({fact_name}.PRODUCT_LINE = B.PRODUCT_LINE) OR ({fact_name}.PRODUCT_LINE is NULL AND B.PRODUCT_LINE is NULL)) AND
      (({fact_name}.PSTLZ = B.PINCODE) OR ({fact_name}.PSTLZ is NULL AND B.PINCODE is NULL)) AND
      (({fact_name}.SALES_ORG = B.SALES_ORG) OR ({fact_name}.SALES_ORG is NULL AND B.SALES_ORG is NULL)) AND
     (({fact_name}.geo_district = B.geo_district) OR ({fact_name}.geo_district is NULL AND B.DISTRICT is NULL)) AND
     (({fact_name}.SALES_GROUP = B.SALES_GROUP) OR ({fact_name}.SALES_GROUP is NULL AND B.SALES_GROUP is NULL)) AND
     (({fact_name}.STATE = B.STATE) OR ({fact_name}.STATE is NULL AND B.STATE is NULL)) AND
     (({fact_name}.ZONE = B.ZONE) OR ({fact_name}.ZONE is NULL AND B.ZONE is NULL)) AND
     (({fact_name}.COUNTRY = B.COUNTRY) OR ({fact_name}.COUNTRY is NULL AND B.COUNTRY is NULL))
     """.format(pro_db=processed_db_name,fact_name=fact_name)
  
  select_condition = select_condition[:-2]
  query = """select 
sales_doc_id as order_id
,sales_doc_line_id as order_line_id
,actl_qty_delvrd as ACTL_QTY_DELVRD
,unit_price as unit_price
,balance_quantity as balance_quantity
,pending_qty as pending_qty_wt
,released_qty as released_qty_wt
,pending_sales_value as pending_sales_value
,released_sales_value as released_sales_value
,net_wgt_info as net_wgt_info
,weight_unit as weight_unit
,order_qty as order_qty
,actl_qty_delvrd_nt_wgt as actl_qty_delvrd_nt_wgt
,GBSTK as ovrl_status
,last_executed_time as LAST_EXECUTED_TIME
,{select_condition}
,{fact_name}.created_date as CREATED_DATE
,sales_org_type as SALES_ORG_TYPE
,CASE WHEN ({fact_name}.PSTLZ is NULL OR 
            {fact_name}.product_line is NULL or 
            {fact_name}.sales_org is NULL or
            {fact_name}.geo_district is NULL or
            {fact_name}.SALES_GROUP is NULL or
            {fact_name}.STATE is NULL or
            {fact_name}.ZONE is NULL or
            {fact_name}.COUNTRY is NULL or
            trim({fact_name}.product_line)='' or 
            trim({fact_name}.PSTLZ) = '' or
            trim({fact_name}.sales_org)='' or
            trim({fact_name}.geo_district)='' or
            trim({fact_name}.SALES_GROUP) = '' or
            trim({fact_name}.STATE) = '' or
            trim({fact_name}.ZONE) = '' or
            trim ({fact_name}.COUNTRY) = '') THEN -2 ELSE ifnull(B.GEO_KEY, -1) END as GEO_KEY
,LAST_EXECUTED_DATE
,INCO1
,INCO2
,NET_VALUE
,SHIP_TO_CUST as SHIP_TO_CUST_ID
,REASON_FOR_REJECTION
    from {fact_name} {join_condition}""".format(join_condition=join_condition,fact_name=fact_name,select_condition=select_condition)
#   print("\nFinal Query for {} tables  ".format(count))
  print(query)
  
  fact_final_view_surrogate = spark.sql(query)
  cols = []

  for item in fact_final_view_surrogate.columns:
    cols.append( "{} AS {}".format(item ,item.upper()))
  fact_final_view_surrogate = fact_final_view_surrogate.selectExpr(cols)
  #print(cols)
  return fact_final_view_surrogate

# d = surrogate_mapping_hil(csv_data,"FACT_PENDING_ORDERS_SNAPSHOT","ZSD_OSR","pro_prod")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS d;
# MAGIC refresh  table pro_prod.fact_pending_orders_snapshot;

# COMMAND ----------

# creating surroagte key
d = surrogate_mapping_hil(csv_data, table_name, "ZSD_OSR", processed_db_name)
d.write.saveAsTable("d")

# COMMAND ----------

# selecting data with the required col order
full_df = spark.sql("select A.ORDER_ID,A.ORDER_LINE_ID,A.ACTL_QTY_DELVRD,A.UNIT_PRICE,A.BALANCE_QUANTITY,A.PENDING_QTY_WT,A.RELEASED_QTY_WT,A.PENDING_SALES_VALUE,A.RELEASED_SALES_VALUE,A.NET_WGT_INFO,A.WEIGHT_UNIT,A.ORDER_QTY,A.ACTL_QTY_DELVRD_NT_WGT,A.OVRL_STATUS,A.LAST_EXECUTED_TIME,A.MATERIAL_KEY,A.PLANT_KEY,A.CUSTOMER_KEY,A.DATE_CREATED_KEY,A.MATL_PRICING_GROUP_KEY,A.CUSTOMER_GROUP_KEY,A.PRODUCT_LINE_KEY,A.CREATED_DATE,A.SALES_ORG_TYPE,A.GEO_KEY,A.SALES_ORG_KEY,A.INCO1,A.INCO2,A.NET_VALUE,A.SHIP_TO_CUST_ID,A.REASON_FOR_REJECTION,A.LAST_EXECUTED_DATE from d A")

# COMMAND ----------

# inserting data to processed layer table
full_df.repartition("product_line_key").write.insertInto(processed_db_name + "." + table_name, overwrite=True)

# COMMAND ----------

# insert the log record to last execution details to SQL
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'INCREMENTAL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
