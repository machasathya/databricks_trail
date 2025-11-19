# Databricks notebook source
# import statements
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
spark.conf.set("spark.sql.broadcastTimeout",  36000)

# COMMAND ----------

# defining class object
pmu = ProcessMetadataUtility()

# COMMAND ----------

# code to fetch the parameters from ADF
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
table_name = "FACT_PRODUCTION_ORDER_CONFIRMATIONS"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# variables to run the code manually
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata_prod"
# curated_db_name = "cur_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# scope = "AZR-DBR-KV-SCOPE-300"
# processed_location = "mnt/bi_datalake/prod/pro/"
# processed_schema_name = "sales_semantic_prod"
# processed_db_name = "pro_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_PRODUCTION_ORDER_CONFIRMATIONS"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
# csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
# pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer for pf plants
afru = spark.sql("""SELECT 
                    rueck, 
                    rmzhl, 
                    aufnr, 
                    gmnga, 
                    lmnga, 
                    wablnr, 
                    myear, 
                    budat,
                    aufpl,
                    aplzl,
                      stokz FROM {}.{} where werks in ('2019','2029','2021')
                     """.format(curated_db_name,"afru"))

# COMMAND ----------

# filtering data on operation field
it_afru = afru.filter("vornr = '0010'")

# COMMAND ----------

# read only 2019,2021,2029
caufv = spark.sql("""
                    SELECT 
                    aufnr, 
                    werks, 
                    auart, 
                    erdat, 
                    plnbez, 
                    gamng FROM {}.{} """.format(curated_db_name,"caufv"))

# COMMAND ----------

# reading data from curated layer
mara = spark.sql("""
                    SELECT 
                    matnr,
                    mtart,
                    matkl,
                    brgew,
                    gewei from {}.{}""".format(curated_db_name,"mara"))

# COMMAND ----------

# reading data from curated layer
afwi = spark.sql("SELECT * FROM {}.{}".format(curated_db_name,"afwi"))

# COMMAND ----------

# reading data from curated layer
mseg = spark.sql("SELECT  mblnr, mjahr, zeile, bwart, matnr, werks, menge, erfmg, erfme, aufnr,budat_mkpf,aufpl,aplzl FROM {}.{}".format(curated_db_name,"mseg"))

# COMMAND ----------

# reading data from curated layer
jest = spark.sql("select * from {}.{} where INACT is null".format(curated_db_name,"JEST"))

# COMMAND ----------

# reading data from curated layer
afko = spark.sql("select * from {}.{}".format(curated_db_name,"AFKO"))

# COMMAND ----------

# reading data from curated layer
afvc = spark.sql("select * from {}.{}".format(curated_db_name,"AFVC"))

# COMMAND ----------

# reading data from curated layer
crhd = spark.sql("select * from {}.{}".format(curated_db_name,"CRHD"))

# COMMAND ----------

# reading data from curated layer
crtx = spark.sql("select * from {}.{} where OBJTY='A' and SPRAS='E'".format(curated_db_name,"CRTX"))

# COMMAND ----------

# reading data from curated layer
resb = spark.sql("select RSNUM,RSPOS,RSART,AUFNR,BDMNG,BWART,trim(ERFME) as ERFME from {}.{}".format(curated_db_name,"RESB"))

# COMMAND ----------

# reading data from curated layer
zsbu3 = spark.sql("select * from {}.{}".format(curated_db_name,"ZSBU3"))

# COMMAND ----------

# joining production order header data with order confirmation table
it_caufv = it_afru.alias('afru').join(caufv.alias('caufv'),on = [
  col('afru.AUFNR') == col("caufv.AUFNR")], how = 'left').select(["caufv." + cols for cols in caufv.columns]+[col("afru.budat"),col("afru.WABLNR"),col("afru.myear"),col("afru.aufpl"),col("aplzl")]).withColumn("objnr",concat(lit("OR"),col("AUFNR")))
it_caufv = it_caufv.orderBy("AUFNR")

# COMMAND ----------

# joining with mara on fg material number
it_mara = it_caufv.alias('caufv').join(mara.alias('mara'),on = [
  col('mara.matnr') == col("caufv.plnbez")], how = 'left').select(["mara." + cols for cols in mara.columns])

# COMMAND ----------

# joining afru with afwi on confirmation number and counter
it_afwi = it_afru.alias('afru').join(afwi.alias('afwi'),on = [
  col('afru.rueck') == col("afwi.rueck"), col("afru.rmzhl") == col("afwi.rmzhl")], how = 'left').select(["afwi." + cols for cols in afwi.columns])

# COMMAND ----------

# fetching all material movements from mseg against afru data
it_mseg_1 = it_afru.alias('afru').join(mseg.alias('mseg'),on = [
  col('afru.wablnr') == col("mseg.mblnr"), col("afru.myear") == col("mseg.mjahr")], how = 'left').select(["mseg." + cols for cols in mseg.columns])

# COMMAND ----------

# fetching all material movements against afwi
it_mseg_2 = it_afwi.alias('afwi').join(mseg.alias('mseg'),on = [
  col('afwi.mblnr') == col("mseg.mblnr"), col("afwi.mjahr") == col("mseg.mjahr")], how = 'left').select(["mseg." + cols for cols in mseg.columns])

# COMMAND ----------

# fix for MIGO transactions
it_mseg_3 = mseg.filter("TCODE2_MKPF = 'MIGO_GI' and bwart in ('261','262')").select("mblnr","mjahr","zeile","bwart","matnr","werks","menge","erfmg","erfme","aufnr","budat_mkpf","aufpl","aplzl")

# COMMAND ----------

# it_mseg1= it_mseg_1.union(it_mseg_2).union(it_mseg_3).dropna(how='all')
# union of afwi and afru material movements
it_mseg1= it_mseg_1.union(it_mseg_2).dropna(how='all')

# COMMAND ----------

# fetching material group and material type
it_mseg1 = it_mseg1.alias('mseg').join(mara.alias('mara'),on=[col('mseg.matnr') == col("mara.matnr")]).select(["mseg." + cols for cols in mseg.columns]+[col("mara.mtart"),col("mara.matkl")])

# COMMAND ----------

# fetching all fg related movements against production ordeers available in caufv
it_mseg = it_caufv.alias('caufv').join(mseg.alias('mseg'),on = [
  col('caufv.aufnr') == col("mseg.aufnr"),col("caufv.aufpl") == col("caufv.aufpl"),col("caufv.aplzl") == col("caufv.aplzl")], how = 'left').select(["mseg." + cols for cols in mseg.columns]).where("(bwart in ('101','102') and aufnr is not null)").distinct()

# COMMAND ----------

# generating it_afru tab
it_afru1 = it_caufv.fillna({ 'wablnr':'<DEFAULT>'}).alias('caufv').join(it_afru.fillna({ 'wablnr':'<DEFAULT>'}).alias('afru'),on = [
  col('caufv.aufnr') == col("afru.aufnr"),col("caufv.wablnr")==col("afru.wablnr"),col("caufv.myear")==col("afru.myear"),col("caufv.budat") == col("afru.budat")], how = 'left').select(col("afru.AUFNR"),col("afru.lmnga"),col("afru.gmnga"),col("stokz"),col("afru.budat"),col("afru.wablnr"),col("afru.myear")).withColumn("lmnga",when(col("stokz").isNull(),col("lmnga")).otherwise(-(col("lmnga")))).groupBy("AUFNR","wablnr","myear","budat").agg(sum("lmnga").alias("lmnga"),sum("gmnga").alias("gmnga"))

# COMMAND ----------

# generating it_mod tab
it_mod = it_mseg.withColumn("menge",when((col("bwart") == '102'),-(col("menge"))).otherwise((col("menge")))).groupBy("AUFNR").agg(sum("menge").alias("menge"))

# COMMAND ----------

# generating it_afko tab
it_afko = it_caufv.alias('caufv').join(afko.alias('afko'),on=[col("caufv.AUFNR") == col("afko.AUFNR")]).select(["caufv." + cols for cols in it_caufv.columns]+[col("afko.aufpl")]).drop(col("caufv.aufpl")).drop("aplzl")

# generating it_aufpl tab
it_aufpl = it_afko.alias('afko').join(afvc.alias('afvc'),on=[col("afko.AUFPL") == col('afvc.AUFPL')]).select(["afko." + cols for cols in it_afko.columns]+[col("afvc.aplzl"),col("afvc.ARBID")])

# generating it_crhd tab
it_crhd = it_aufpl.alias('aufpl').join(crhd.alias('crhd'),on=[col("aufpl.ARBID") == col("crhd.OBJID")]).select(["aufpl." + cols for cols in it_aufpl.columns]+[col("crhd.ARBPL")])

# generating it_crtx tab
it_crtx = it_crhd.alias('crhd').join(crtx.alias('crtx'),on=[col("crhd.ARBID") == col("crtx.OBJID")]).select(["crhd." + cols for cols in it_crhd.columns]+[col("crtx.KTEXT")]).sort(col("aufpl"),col("aplzl"))

# generating it_operation tab
it_operation = it_crtx.groupBy("AUFNR").agg(last("ARBPL").alias("ARBPL"),last("KTEXT").alias("KTEXT"))

# COMMAND ----------

# generating it_component tab
it_component = it_caufv.alias('caufv').join(resb.alias('resb'), on =[col("caufv.AUFNR") == col("resb.AUFNR")]).select(["resb." + cols for cols in resb.columns]).drop(col("LAST_UPDATED_DT_TS")).distinct()

# COMMAND ----------

# code to generate the std BOM qty for each rm used against production order

# it_temp1 = it_component.alias("df1").join(mara,on=[col("df1.matnr") == col("mara.matnr")]).select(["df1." + cols for cols in it_component.columns]+[col("mara.mtart")])

# it_zsbu3 = it_temp1.alias("df1").join(zsbu3,on=[col("df1.matnr") == col("zsbu3.matnr"),col("df1.werks") == col("zsbu3.werks")] ,how = 'left').select(["df1." + cols for cols in it_temp1.columns]+[col("STD_WT").alias("ZSBU3_STD_WT"),col("VALID_FROM"),col("VALID_TO")]).where((col("df1.bdter").between(col("VALID_FROM"),col("VALID_TO"))) | ((col("VALID_FROM").isNull()) & (col("VALID_TO").isNull()))).drop("VALID_FROM","VALID_TO")


it_prd_wt_1 = it_component.withColumn("BDMNG",when((col("ERFME").isin(["TO","MT"])),(1000*col("BDMNG"))).otherwise(col("BDMNG")))

# it_prd_wt_1 = it_zsbu3.withColumn("BDMNG",when((col("ERFME").isin(["TO","MT"])),(1000*col("BDMNG"))).when((col("mtart").isin(["ZHLB","HALB"])) & (col("ERFME").isin(["EA"])),col("ZSBU3_STD_WT")*col("BDMNG")).otherwise(col("BDMNG")))

it_prd_wt_2 = it_prd_wt_1.withColumn("BDMNG",when((col("bwart").isin(["261","532"])),col("BDMNG")).when((col("bwart").isin(["262","531"])),-(col("BDMNG"))))

it_prd_wt_2.createOrReplaceTempView("prd_wt")

prd_wt = spark.sql("""
select AUFNR,
sum(case when (erfme in('TO','MT','KG'))then bdmng
                      else 0 end) as BDMNG from prd_wt group by AUFNR
""")

# prd_wt = spark.sql("""
# select AUFNR,
# sum(case when (erfme in('TO','MT','KG') or (erfme = 'EA' and mtart in ('ZHLB','HALB')))then bdmng
#                       else 0 end) as BDMNG from prd_wt group by AUFNR
# """)

# display(prd_wt.where("aufnr = '202920003047'"))

# COMMAND ----------

# fetching actual consumption of rm against production order

# it_mseg_zsbu3 = it_mseg1.alias("df1").join(zsbu3,on=[col("df1.matnr") == col("zsbu3.matnr"),col("df1.werks") == col("zsbu3.werks")] ,how = 'left').select(["df1." + cols for cols in it_mseg1.columns]+[col("STD_WT").alias("ZSBU3_STD_WT"),col("VALID_FROM"),col("VALID_TO")]).where((col("df1.budat_mkpf").between(col("VALID_FROM"),col("VALID_TO"))) | ((col("VALID_FROM").isNull()) & (col("VALID_TO").isNull()))).drop("VALID_FROM","VALID_TO")


it_prd_actl_1 = it_mseg1.withColumn("ERFMG",when((col("ERFME").isin(["TO","MT"])),(1000*col("erfmg"))).otherwise(col("erfmg")))

# it_prd_actl_1 = it_mseg_zsbu3.withColumn("ERFMG",when((col("ERFME").isin(["TO","MT"])),(1000*col("erfmg"))).when((col("mtart").isin(["ZHLB","HALB"])) & (col("ERFME").isin(["EA"])),col("ZSBU3_STD_WT")*col("ERFMG")).otherwise(col("erfmg")))

it_prd_actl_2 = it_prd_actl_1.withColumn("ERFMG",when((col("bwart").isin(["261","532"])),col("erfmg")).when((col("bwart").isin(["262","531"])),-(col("erfmg"))))

it_prd_actl_2.createOrReplaceTempView("prd_actl")

prd_actl = spark.sql("""
select AUFNR,mblnr,mjahr,budat_mkpf,
sum(case when (erfme in('TO','MT','KG') ) then ERFMG
                      else 0 end) as ERFMG from prd_actl group by aufnr,mblnr,mjahr,budat_mkpf
""")


# prd_actl = spark.sql("""
# select AUFNR,mblnr,mjahr,budat_mkpf,
# sum(case when (erfme in('TO','MT','KG') or (erfme = 'EA' and mtart in ('ZHLB','HALB'))) then ERFMG
#                       else 0 end) as ERFMG from prd_actl group by aufnr,mblnr,mjahr,budat_mkpf
# """)

# display(it_prd_actl_2.where("aufnr = '201920003850'"))

# COMMAND ----------

# generating process rejection, runner wastage non wastage grinding consumption values
it_temp1 = it_prd_actl_1.withColumn("ERFMG",when((col("bwart").isin(["261","531"])),col("erfmg")).when((col("bwart").isin(["262","532"])),-(col("erfmg"))))

it_temp2 = it_temp1.alias('df1').join(it_caufv.alias('df2'), on =[col("df1.mblnr") == col("df2.wablnr"),col("df1.aufnr") == col("df2.aufnr"),col("df1.mjahr") == col("df2.myear"),col("budat_mkpf") == col("budat")]).select(["df1." + cols for cols in it_temp1.columns] + [col("df2.auart")])

it_proc_reg = it_temp2.withColumn("PROCESS_REJECTION",when((col("auart").isin(["ZFT1","ZFT2","ZRCY"]) & col("matkl").isin(["CSCRAP","NUCSCRAP"]) & col("bwart").isin(["531","532"])),col("erfmg")).when((col("auart").isin(["ZP01","ZP02","ZASY"]) & col("matkl").isin(["USCRAP","NUSCRAP"]) & col("bwart").isin(["531","532"])),col("erfmg")))
                                  
it_runn_wast = it_proc_reg.withColumn("RUNNER_WASTAGE",when((col("auart").isin(["ZFT1","ZFT2","ZRCY"]) & col("matkl").isin(["UCSCRAP"]) & col("bwart").isin(["531","532"])),col("erfmg")))

it_non_wast = it_runn_wast.withColumn("NON_USABLE_WASTAGE",when((col("auart").isin(["ZFT1","ZFT2","ZRCY"]) & col("matkl").isin(["NUCSCRAP"]) & col("bwart").isin(["531","532"])),col("erfmg")).when((col("auart").isin(["ZP01","ZP02","ZASY"]) & col("matkl").isin(["NUSCRAP"]) & col("bwart").isin(["531","532"])),col("erfmg")))

it_grnd_consu = it_non_wast.withColumn("GRINDING_CONSUMPTION",when((col("auart").isin(["ZFT1","ZFT2","ZRCY"]) & col("matkl").isin(["PL79"]) & col("bwart").isin(["261","262"])),col("erfmg")).when((col("auart").isin(["ZP01","ZP02","ZASY"]) & col("matkl").isin(["PL77"]) & col("bwart").isin(["531","532"])),col("erfmg")))


it_grnd_consu.createOrReplaceTempView("waste")

wastage_cols = spark.sql("""
select AUFNR,mblnr,mjahr,budat_mkpf,
                      sum(case when erfme in('TO','MT','KG') then PROCESS_REJECTION
                      else 0 end) as PROCESS_REJECTION,
                      sum(case when erfme in('TO','MT','KG') then RUNNER_WASTAGE
                      else 0 end) as RUNNER_WASTAGE, 
                      sum(case when erfme in('TO','MT','KG') then NON_USABLE_WASTAGE
                      else 0 end) as NON_USABLE_WASTAGE,
                      sum(case when erfme in('TO','MT','KG') then GRINDING_CONSUMPTION
                      else 0 end) as GRINDING_CONSUMPTION                     
                      from waste group by aufnr,mblnr,mjahr,budat_mkpf
""") 

# COMMAND ----------

# finding unit weight against material
it_gross_wt = it_caufv.alias('caufv').join(mara.alias('mara'),on = [
  col('mara.matnr') == col("caufv.plnbez")], how = 'left').select(["caufv." + cols for cols in it_caufv.columns]+[col("brgew")])

# COMMAND ----------

# clubbing all intermediate dataframes to one final df
it_final_caufv = it_gross_wt.select(col("AUFNR"),col("werks"),col("erdat"),col("plnbez"),col("objnr"),col("gamng").alias("PLANNED_QTY_EA"),col("auart"),col("brgew"),col("wablnr"),col("myear"),col("budat").alias("POSTING_DATE"))

it_status = it_final_caufv.alias('caufv').join(jest.alias('jest'), 
                                                     on =[ col("caufv.objnr") == col("jest.objnr")]).select(["caufv." + cols for cols in it_final_caufv.columns]+[col("jest.STAT")]).withColumn("STATUS",when((col("STAT").isin (['I0045','I0046','IF047'])),lit("Closed")).otherwise(lit("Open"))).groupBy("AUFNR").agg(collect_list(col("STATUS")).alias("STATUS")).withColumn("STATUS",when((array_contains("STATUS","Closed")),lit("Closed")).otherwise("Open"))

it_final_status = it_final_caufv.alias('caufv').join(it_status.alias('status') , on = [col("caufv.AUFNR") == col("status.AUFNR")]).select(["caufv." + cols for cols in it_final_caufv.columns]+[col("status.STATUS")])

it_final_prd_wt = it_final_status.alias('status').join(prd_wt.alias("wt"), on = [col("status.aufnr") == col("wt.aufnr")]).select(["status." + cols for cols in it_final_status.columns]+[col("wt.BDMNG").alias("PRD_WT")])

it_final_prd_actl = it_final_prd_wt.alias("prd").join(prd_actl.alias("actl"), on = [col("prd.AUFNR") == col("actl.AUFNR"),col("prd.POSTING_DATE") == col("actl.budat_mkpf"),col("prd.wablnr") == col("actl.mblnr"),col("prd.myear") == col("actl.mjahr")]).select(["prd." + cols for cols in it_final_prd_wt.columns]+[col("actl.erfmg").alias("ACTL_PRD_KG")])

it_final_afru = it_final_prd_actl.alias('final').join(it_afru1.alias('afru'), on=[col("final.AUFNR") == col("afru.AUFNR"),col("afru.budat") == col("final.POSTING_DATE"),col("afru.wablnr") == col("final.wablnr"),col("final.myear")==col("afru.myear")]).select(["final." + cols for cols in it_final_prd_actl.columns]+[col("afru.lmnga").alias("PRD_QTY_EA")])

it_final_waste = it_final_afru.alias("df1").join(wastage_cols.alias("waste"),on=[col("df1.AUFNR") == col("waste.AUFNR"),col("df1.POSTING_DATE") == col("waste.budat_mkpf"),col("df1.wablnr") == col("waste.mblnr"),col("df1.myear") == col("waste.mjahr")]).select(["df1." + cols for cols in it_final_afru.columns]+[col("PROCESS_REJECTION"),col("RUNNER_WASTAGE"),col("NON_USABLE_WASTAGE"),col("GRINDING_CONSUMPTION")])

it_final_mod = it_final_waste.alias("df1").join(it_mod.alias("mod"),on=[col("df1.aufnr") == col("mod.aufnr")],how='left').select(["df1." + cols for cols in it_final_waste.columns]+[col("mod.menge").alias("PRD_QTY")])

it_final_workcenter = it_final_mod.alias('afru').join(it_operation.alias('final'),on=[col("final.aufnr") == col("afru.aufnr")],how='left').select(["afru." + cols for cols in it_final_mod.columns] + [col("final.arbpl").alias("Machine_No"),col("final.ktext").alias("Machine_Description")])

# COMMAND ----------

# splitting all the values that are available at a production order level to confirmation level equally
final_df = it_final_workcenter.groupBy('AUFNR').count()
final = it_final_workcenter.alias('df').join(final_df.alias('cg'),on=[col('df.aufnr') == col('cg.AUFNR')],how='left').select(["df." + cols for cols in it_final_workcenter.columns] + ['cg.count'])

final_temp1 = final.withColumn("PLANNED_QTY_EA_SPLIT",round(lit(col('PLANNED_QTY_EA') / col('count')),3))

final_temp2 = final_temp1.withColumn("brgew_split",round(lit(col('brgew') / col('count')),4)).withColumn("PRD_QTY_SPLIT",round(lit(col('PRD_QTY') / col('count')),3))

final_temp3 = final_temp2.withColumn("PRD_WT_SPLIT",round(lit(col('PRD_WT') / col('count')),4))

final_temp4 = final_temp3.withColumn("PRD_WT",when(col("auart") == "ZASY",col("brgew")).otherwise(col("PRD_WT"))).withColumn("PRD_WT_SPLIT",when(col("auart") == "ZASY",col("brgew_split")).otherwise(col("PRD_WT_SPLIT")))

final = final_temp4.selectExpr("AUFNR as PRODUCTION_ORDER","werks as PLANT_ID","erdat as CREATED_DATE","plnbez as MATERIAL_ID","PRD_WT as STANDARD_RM_WT","PRD_WT_SPLIT as STANDARD_RM_WT_SPLIT","PLANNED_QTY_EA","PLANNED_QTY_EA_SPLIT","auart as ORDER_TYPE","POSTING_DATE","STATUS","ACTL_PRD_KG as ACTUAL_RM_WT","PRD_QTY_EA","PROCESS_REJECTION","RUNNER_WASTAGE","NON_USABLE_WASTAGE","GRINDING_CONSUMPTION","PRD_QTY as FINAL_PRD_QTY","PRD_QTY_SPLIT as FINAL_PRD_QTY_SPLIT","Machine_Description as MACHINE_DESCRIPTION")

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table pro_prod.dim_product

# COMMAND ----------

# reading dim_product data from processed layer
dim_product = spark.sql("select * from {}.{}".format("pro_prod","dim_product"))

# COMMAND ----------

# extracting list of production order where prd qty ea is 0
null_prod_ordrs = [row.PRODUCTION_ORDER for row in final.groupBy("PRODUCTION_ORDER").sum("PRD_QTY_EA").where("sum(PRD_QTY_EA) = 0 ").select("PRODUCTION_ORDER").toLocalIterator()]

# COMMAND ----------

# filtering records for which prd qty ea is 0
final = final.where("production_order not in {}".format(tuple(null_prod_ordrs)))

# COMMAND ----------

# extracting material group from material master and filtering the data based on material group and machine description
final_1  = final.alias("df1").join(dim_product.alias("df2"),on=[col("MATERIAL_ID") == col("MATERIAL_NUMBER")]).select(["df1." + cols for cols in final.columns] + [col("df2.FG_MATERIAL_GROUP")]).withColumn("FLAG",when((col('PLANT_ID')=='2019') & ((((upper(col('FG_MATERIAL_GROUP'))=='CPVC PIPES') | (upper(col('FG_MATERIAL_GROUP'))=='UPVC PIPES') | (upper(col('FG_MATERIAL_GROUP'))=='COLUMN PIPES')) & ((col('MACHINE_DESCRIPTION').like("%Theysohn Cpvc Extruder 01%") | col('MACHINE_DESCRIPTION').like("%Rollepal Upvc Extruder 01%") | col('MACHINE_DESCRIPTION').like("%Theysohn CON-63 Extruder%")))) | (((upper(col('FG_MATERIAL_GROUP'))=='CPVC FITTINGS') | (upper(col('FG_MATERIAL_GROUP'))=='UPVC FITTINGS')) & ((col('MACHINE_DESCRIPTION').like("%Packing%")))) | (((upper(col('FG_MATERIAL_GROUP'))=='SWR PIPES') | (upper(col('FG_MATERIAL_GROUP'))=='PRESSURE PIPES')) & ((col('MACHINE_DESCRIPTION').like("%Quality%"))))),1)
.when((col('PLANT_ID')=='2029') & ((upper(col('FG_MATERIAL_GROUP'))=='CPVC PIPES') | (upper(col('FG_MATERIAL_GROUP'))=='UPVC PIPES') | (upper(col('FG_MATERIAL_GROUP'))=='SWR PIPES') | (upper(col('FG_MATERIAL_GROUP'))=='UGD PIPE') | (upper(col('FG_MATERIAL_GROUP'))=='PRESSURE PIPES') | (upper(col('FG_MATERIAL_GROUP'))=='CPVC FITTINGS') | (upper(col('FG_MATERIAL_GROUP'))=='UPVC FITTINGS') | (upper(col('FG_MATERIAL_GROUP'))=='SWR FITTINGS') | (upper(col('FG_MATERIAL_GROUP'))=='PRESSURE FITTINGS') | (upper(col('FG_MATERIAL_GROUP'))=='UGD FITTINGS')) & ((col('MACHINE_DESCRIPTION').like("%Quality%"))),1)
.when((col('PLANT_ID')=='2021') & ((((upper(col('FG_MATERIAL_GROUP'))=='SWR PIPES') | (upper(col('FG_MATERIAL_GROUP'))=='PRESSURE PIPES') | (upper(col('FG_MATERIAL_GROUP'))=='COLUMN PIPES') | (upper(col('FG_MATERIAL_GROUP'))=='UGD PIPE')) & ((col('MACHINE_DESCRIPTION').like("%Quality Check%")))) | (((upper(col('FG_MATERIAL_GROUP'))=='CPVC PIPES') | (upper(col('FG_MATERIAL_GROUP'))=='UPVC PIPES')) & ((col('MACHINE_DESCRIPTION').like("%Theysohn CON 63, dual  line%"))))),1).otherwise(0))

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_1.write.parquet(processed_location+'PRODUCTION_ORDER_CONFIRMATIONS', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
  final_1.createOrReplaceTempView("{}".format(fact_name))
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
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name,count=count)
  select_condition = select_condition[:-2]      
  query = """select
 PRODUCTION_ORDER,
{fact_name}.CREATED_DATE,
STANDARD_RM_WT,
STANDARD_RM_WT_SPLIT,
PLANNED_QTY_EA,
PLANNED_QTY_EA_SPLIT,
ORDER_TYPE,
STATUS,
ACTUAL_RM_WT,
PRD_QTY_EA,
PROCESS_REJECTION,
RUNNER_WASTAGE,
NON_USABLE_WASTAGE,
GRINDING_CONSUMPTION,
FINAL_PRD_QTY,
FINAL_PRD_QTY_SPLIT,
MACHINE_DESCRIPTION,
FLAG
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

# generating surrogate keys
d = surrogate_mapping_hil(csv_data,table_name,"PRODUCTION_ORDER_CONFIRMATIONS",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# inserting log records to last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
