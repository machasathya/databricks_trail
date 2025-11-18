# Databricks notebook source
# MAGIC %md
# MAGIC ##Data bricks implementation of ZMMSTOREG_ZPST1.
# MAGIC ###This NOTEBOOK runs ZMMSTOREG_1 and unions output dataframes of ZMMSTOREG_ZPST1 and ZMMSTOREG_ZDST1, writes final df to ZMM_STOREG before SK implementation and finally to FACT_STO.
# MAGIC ###Final dataframe contains data for Plant to Depot Movement Only.

# COMMAND ----------

# Import statements
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, concat, lit, struct, row_number, when, asc, udf, to_date, isnull,abs,upper
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DateType
import ProcessMetadataUtility as pmu
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
import numpy as np

# COMMAND ----------

# Initialising object for processmetadatautility class
pmu = ProcessMetadataUtility()

# COMMAND ----------

# increasing broadcast time to 36000 seconds
spark.conf.set("spark.sql.broadcastTimeout", 36000)

# COMMAND ----------

# parameters used to fetch the data from ADF
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
table_name = "FACT_STOCK_MOVEMENT"

# COMMAND ----------

# Variables used to run the notebook manually
# meta_table = "fact_surrogate_meta"
# processed_schema_name = "global_semantic_prod"
# schema_name = "metadata_prod"
# db_url = "hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# processed_location = "mnt/bi_datalake/prod/pro/"
# curated_db_name = "cur_prod"
# processed_db_name = "pro_prod"
# table_name = "FACT_STOCK_MOVEMENT"

# COMMAND ----------

# MAGIC %md
# MAGIC ###lt_main
# MAGIC

# COMMAND ----------

#Original invoice type selection...
#excluded ZPFS based on confirmation from Krishna & Venkatesh
vbrk = spark.sql("select * from {}.{}".format(curated_db_name,"VBRK")). where("FKART IN ('ZSTO','ZST1') AND (SFAKN is null or trim(sfakn) = '') and ( FKSTO is null OR FKSTO <> 'X' ) ")
lt_vbrk = vbrk.select('VBELN','FKDAT','FKART','ZZVEH_NO','ZZLR_NO','FKART','KNUMV','KUNAG','XBLNR')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Billing Document Line Item ( STOs )
# MAGIC

# COMMAND ----------

# reading vbrp data from curated layer
vbrp=spark.sql("select * from {}.{}".format(curated_db_name,"VBRP"))
lt_vbrp = vbrp.select('POSNR','FKIMG','VRKME','NTGEW','GEWEI','VGBEL','VGPOS','MATNR','NETWR','SPART',col('WERKS').alias('vbrp_WERKS'),'VBELN','AUBEL','AUPOS')

# COMMAND ----------

# Joining invoice item and header data
lt_main = lt_vbrk.alias('lt_vbrk').join(lt_vbrp.alias('lt_vbrp'),lt_vbrk.VBELN == lt_vbrp.VBELN,how='inner').select(
    [col("lt_vbrk." + cols) for cols in lt_vbrk.columns] + [col("lt_vbrp." + cols) for cols in lt_vbrp.columns if cols != 'VBELN'])

# COMMAND ----------

# MAGIC %md
# MAGIC ###lt_excise

# COMMAND ----------

# reading data from curated layer
j_itexcdtl = spark.sql("select * from {}.{}".format(curated_db_name,"J_1IEXCDTL")).select('RDOC1','MATNR',col('MENGE').alias('QTY'),col('MEINS').alias('cdtl_MEINS'),'RDOC2','RDOC3','RITEM1','RITEM2',col('TRNTYP').alias('cdtl_TRNTYP'),col('DOCNO').alias('chdtl_DOCNO'),col('DOCYR').alias('cdtl_DOCYR'),'EXNUM')

# COMMAND ----------

# reading data from curated layer
j_itexchdr = spark.sql("select * from {}.{}".format(curated_db_name,"J_1IEXCHDR")).select(col('WERKS').alias('chdr_WERKS'),'KUNWE','EXDAT',col('DOCYR').alias('chdr_DOCYR'),'KUNAG',col('TRNTYP').alias('chdr_TRNTYP'),col('DOCNO').alias('chdr_DOCNO'),'STATUS','STRTYP')

# COMMAND ----------

# joining j_itexcdtl and j_itexcdtr
excise = j_itexchdr.alias('j_itexchdr').join(j_itexcdtl.alias('j_itexcdtl'),(j_itexchdr.chdr_TRNTYP == j_itexcdtl.cdtl_TRNTYP) & (j_itexchdr.chdr_DOCNO == j_itexcdtl.chdtl_DOCNO) & (j_itexchdr.chdr_DOCYR==j_itexcdtl.cdtl_DOCYR),how='inner').select('RDOC1','MATNR','QTY','cdtl_MEINS','RDOC2','RDOC3','STATUS','cdtl_DOCYR','KUNWE','EXNUM','EXDAT','KUNAG','cdtl_TRNTYP','chdtl_DOCNO','RITEM1','chdr_WERKS','RITEM2','STRTYP')

# COMMAND ----------

mara = spark.sql("select * from {}.{}".format(curated_db_name,"MARA")).select('MATNR', 'NTGEW','GEWEI')

# joining mara with excise dataset
lt_excise = excise.alias('excise').join(mara.alias('mara'),on=['MATNR'],how='inner').where("cdtl_TRNTYP in ('DLFC','OTHR')").where("STRTYP in ('S1','S2')").where("STATUS in ('C','P')").where("RDOC3 != '000000'").select('RDOC1','RDOC2','RDOC3',col('QTY'),'cdtl_MEINS','EXNUM','EXDAT','cdtl_DOCYR','chdtl_DOCNO','KUNWE','RITEM1','MATNR', 'NTGEW','chdr_WERKS','RITEM2','GEWEI').withColumn('WEIGHT',col('QTY')*col('NTGEW'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###itab1 = join lt_main and lt_excise

# COMMAND ----------

distinct_vgbels = lt_main.groupBy("VGBEL").count().select("VGBEL")
itab1_excise = distinct_vgbels.join(lt_excise.alias('lt_excise'),lt_excise.RDOC1==distinct_vgbels.VGBEL ,"INNER" ).select([col("lt_excise." + columns) for columns in lt_excise.columns])
likp_temp = spark.sql("select * from {}.{}".format(curated_db_name,"LIKP")).select('VBELN', col('WERKS').alias('kp_WERKS'),'WADAT_IST','ERDAT')
itab1_excise_with_likp=itab1_excise.join(likp_temp,likp_temp.VBELN==itab1_excise.RDOC1,'INNER')

# COMMAND ----------

# FIX: Incorrect JOIN between  itab1_excise_with_likp and ekko on RDOC1 = EBELN
#  

konv_ekko1 = spark.sql("select KNUMV,max(KBETR) as KBETR from {}.{} where KSCHL IN ('FRC1','FRB1') group by KNUMV".format(curated_db_name,"KONV")).select('KNUMV','KBETR')
konv_ekko1.createOrReplaceTempView('konv_ekko1')
ekko1 = spark.sql("select EBELN,kv.KBETR as KBETR, kv.knumv as knumv from {}.{} ekko1  join konv_ekko1 kv on ( ekko1.knumv = kv.knumv)".format(curated_db_name,"EKKO")).select('EBELN','KBETR','KNUMV')
itab1_excise_with_likp_ekko1 = itab1_excise_with_likp.alias('itab1').join(ekko1.alias('ekko1'),on=[itab1_excise_with_likp['RDOC1']==ekko1['EBELN']],how="left").select([col("itab1." + columns) for columns in itab1_excise_with_likp.columns] + [col('ekko1.KBETR').alias('KBETR'),col('ekko1.KNUMV').alias('KNUMV')])
lt_main.createOrReplaceTempView('lt_main_temp')
itab1_excise_with_likp_ekko1.createOrReplaceTempView('ekko1_temp')
itab1_excise_with_likp_ekko = spark.sql("select lt.VGBEL,lt.FKDAT,lt.KNUMV,et.RDOC1,et.RDOC2,et.RDOC3,et.QTY,et.cdtl_MEINS,et.EXNUM,et.EXDAT,et.cdtl_DOCYR,et.chdtl_DOCNO,et.KUNWE,et.RITEM1,et.MATNR,et.NTGEW,et.chdr_WERKS,et.RITEM2,et.GEWEI,et.WEIGHT,lt.NETWR from lt_main_temp lt join ekko1_temp et on ( lt.VGBEL=et.RDOC1 and lt.VGPOS=et.RITEM1)")  

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %md
# MAGIC Path2 Starts

# COMMAND ----------

itab1_temp = itab1_excise_with_likp_ekko.select("RDOC1","RITEM1")
itab2 = lt_main.join(itab1_temp,(lt_main.VGBEL == itab1_temp.RDOC1) & (lt_main.VGPOS == itab1_temp.RITEM1) ,"LEFT" ).where("RDOC1 is null").drop("RDOC1","RITEM1")

# COMMAND ----------

likp_1 = spark.sql("select * from {}.{}".format(curated_db_name,"LIKP")).select('VBELN', col('WERKS').alias('kp_WERKS'),'WADAT_IST','ERDAT')

# COMMAND ----------

itab2_kp = itab2.join(likp_1,likp_1.VBELN== itab2.VGBEL,'INNER')

# COMMAND ----------

mara_t2 = spark.sql("select * from {}.{}".format(curated_db_name,"MARA")).select('MATNR','NTGEW','GEWEI')
new_itab2_kp_ma =  itab2_kp.drop('NTGEW').drop('GEWEI').join(mara_t2, ['MATNR'] ,'INNER')

# COMMAND ----------

# changing column names
new_itab2_kp_ma=new_itab2_kp_ma.withColumn('RDOC3',concat(col('AUBEL'),col('AUPOS'))).select(col('vbrp_WERKS'),col('kp_WERKS').alias('KUNWE'),col('MATNR'),col('lt_vbrk.VBELN').alias('RDOC2'),col('POSNR').alias('RITEM2'),col('FKDAT'),col('VGBEL').alias('RDOC1'),col('VGPOS').alias('RITEM1'),col('kp_WERKS').alias('KUNAG'),col('AUBEL').alias('EBELN'),col('AUBEL'),col('AUPOS').alias('EBELP'),col('VGBEL').alias('XBLNR'),col('RDOC3'),col('FKIMG').alias('QTY'),col('VRKME').alias('MEINS'),col('GEWEI'),col('NTGEW'),col('kp_WERKS'),col('vbrp_WERKS'),col('VGBEL'),col('FKIMG'),'KNUMV','WADAT_IST','NETWR')
new_itab2_kp_ma = new_itab2_kp_ma.withColumn('WEIGHT',col('NTGEW')*col('FKIMG'))

# COMMAND ----------

konv_ekko2 = spark.sql("select KNUMV,max(KBETR) as KBETR from {}.{} where KSCHL IN  ('FRC1','FRB1', 'ZFRB') group by KNUMV".format(curated_db_name,"KONV")).select('KNUMV','KBETR')
konv_ekko1.createOrReplaceTempView('konv_ekko2')
ekko_t2 = spark.sql("select EBELN,EXNUM,kv.KBETR from {}.{} ekko2  join konv_ekko2 kv on ( ekko2.knumv = kv.knumv)".format(curated_db_name,"EKKO")).select('EBELN','EXNUM','KBETR')
new_itab2_kp_ma_ko = new_itab2_kp_ma.join(ekko_t2, ekko_t2.EBELN == new_itab2_kp_ma.AUBEL,'LEFT')

# COMMAND ----------

path2_df = new_itab2_kp_ma_ko.withColumn('cdtl_DOCYR',lit(None).cast('string')).withColumn('chdtl_DOCNO',lit(None).cast('string')).select(col('VGBEL'),col('RDOC1'),col('RDOC2'),
col('RDOC3'),col('QTY'),col('MEINS').alias('cdtl_MEINS'),col('EXNUM'),col('FKDAT'),('KNUMV'),col('WADAT_IST').alias('EXDAT'),col('cdtl_DOCYR'),col('chdtl_DOCNO'),col('kp_WERKS').alias('KUNWE'),col('RITEM1'),col('MATNR'),col('NTGEW'),col('vbrp_WERKS').alias('chdr_WERKS'),col('RITEM2'), col('GEWEI'),col('WEIGHT'),col('NETWR'))
path2_final_df=path2_df.select('VGBEL','FKDAT','KNUMV','RDOC1','RDOC2','RDOC3','QTY','cdtl_MEINS','EXNUM','EXDAT','cdtl_DOCYR','chdtl_DOCNO','KUNWE','RITEM1','MATNR','NTGEW','chdr_WERKS','RITEM2','GEWEI','WEIGHT','NETWR')
# path2_df.count()

# COMMAND ----------

union_itab_1_2 = path2_final_df.union(itab1_excise_with_likp_ekko)

# COMMAND ----------

konv_sto = spark.sql("select * from {}.{}".format(curated_db_name,"KONV")).select('KNUMV','KSCHL','KBETR','KWERT','KPOSN')#.where("KSCHL IN ('JOIG','JOCG','JOSG') ")
konv_sto.createOrReplaceTempView("konv_sto")
lt_konv_df=spark.sql("""SELECT KNUMV,KPOSN,SUM( CASE WHEN KSCHL='JOIG' THEN KBETR/10 ELSE 0 END) AS IGST_RATE,
                  SUM(CASE WHEN KSCHL='JOCG' THEN KBETR/10 ELSE 0 END) AS CGST_RATE,
                  SUM(CASE WHEN KSCHL='JOSG' THEN KBETR/10 ELSE 0 END) AS SGST_RATE
                  FROM konv_sto GROUP BY KNUMV,KPOSN""")

# COMMAND ----------

# get measures from conditions associated with STO
itab1_st = union_itab_1_2.join(lt_konv_df, (union_itab_1_2.KNUMV == lt_konv_df.KNUMV) &(union_itab_1_2.RITEM2 == lt_konv_df.KPOSN),'LEFT' ).drop(lt_konv_df.KNUMV)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Document Data MSEG and MKPF

# COMMAND ----------

m1 = spark.sql("select * from {}.{}".format(curated_db_name,"MSEG"))

# COMMAND ----------

mseg = spark.sql("select * from {db}.{table}".format(db=curated_db_name,table="MSEG")).select('XBLNR_MKPF','LGORT','BWART','MBLNR','MJAHR','VBELN_IM','VBELP_IM',col('MEINS').alias('mseg_MEINS'),col("MENGE").alias("mseg_QTY"),'ZEILE','BWART').where("BWART in ('101','102')")

# COMMAND ----------

mkpf = spark.sql("select * from {}.{}".format(curated_db_name,"MKPF")).select(col('MBLNR').alias('mk_MBLNR'),'MJAHR','BUDAT','BLART','ZVEHICLE','ZLR_NO').where("BLART = 'WE'")

# COMMAND ----------

lt_mk_ms = mseg.alias('mseg').join(mkpf, (mseg.MBLNR == mkpf.mk_MBLNR) & (mseg.MJAHR == mkpf.MJAHR), "INNER")

# COMMAND ----------

itab1 = itab1_st 

# COMMAND ----------

mkpf_df=spark.sql("select * from {}.{}".format(curated_db_name,"MKPF")).select(col('MBLNR').alias('mk_MBLNR'),'MJAHR','BUDAT','BLART','ZVEHICLE','ZLR_NO').where("BLART = 'WE'")
mseg_df1=spark.sql("select * from {}.{}".format(curated_db_name,"MSEG")).select('XBLNR_MKPF','LGORT','BWART','MBLNR','MJAHR','VBELN_IM','VBELP_IM',col('MEINS').alias('mseg_MEINS'),col("MENGE").alias("RCQTY"),'ZEILE','BWART').where("BWART in ( '101','102') ")
mseg_df = mseg_df1.withColumn('RCQTY',when(col('BWART')=='101',col('RCQTY')).otherwise(col('RCQTY')*lit(-1)))

mm1=mkpf.join(mseg_df,[mkpf.mk_MBLNR==mseg_df.MBLNR,mkpf.MJAHR==mseg_df.MJAHR],how='INNER')

mm1.createOrReplaceTempView('mseg_mkpf')
mm = spark.sql("select VBELN_IM,VBELP_IM,sum(RCQTY) as RCQTY from mseg_mkpf group by VBELN_IM,VBELP_IM")
# Identified cases where material movement is not recorded for few STOs.  
dd =  itab1.join(mm, (itab1.RDOC1 == mm.VBELN_IM) & (itab1.RITEM1 == mm.VBELP_IM),"LEFT").withColumn("rdoc3_EBELN", col('RDOC3').substr(0,10)).withColumn("rdoc3_EBELP", col('RDOC3').substr(12,6)).withColumn("rdoc3_KPOSN", col('RDOC3').substr(11,6))
# code to handle cancelation of material movements ( consider BWART - 102 ) 


# COMMAND ----------

ekko = spark.sql("select * from {}.{}".format(curated_db_name,"EKKO")).select('KNUMV','EBELN','LIFNR')

# COMMAND ----------

ekpo = spark.sql("select * from {}.{}".format(curated_db_name,"EKPO")).select('EBELN','EBELP',col('MENGE').alias('PO_QTY'),col('NETWR').alias('ek_NETWR'))

# COMMAND ----------

lt_po_ko=ekko.join(ekpo,on=['EBELN'],how='inner')

# COMMAND ----------

itab1_mk_ms_ko_po = dd.drop('KNUMV').join(lt_po_ko,(dd.rdoc3_EBELN == lt_po_ko.EBELN) & (dd.rdoc3_EBELP == lt_po_ko.EBELP),'LEFT')

# COMMAND ----------

konv_po = spark.sql("select * from {}.{}".format(curated_db_name,"KONV")).select('KNUMV','KWERT','KPOSN','KSCHL','KBETR').where("KSCHL = 'ZFRB' OR KSCHL = 'ZFRC'  OR  KSCHL = 'Z09T' OR KSCHL = 'Z16T' OR  KSCHL = 'Z22T' OR  KSCHL = 'Z27T' ")

# COMMAND ----------

lt_konv_po = konv_po.groupBy('KNUMV','KPOSN').sum('KWERT').select(col('KNUMV').alias('nv_KNUMV'),'KPOSN',col('sum(KWERT)').alias('KWERT'))

# COMMAND ----------

itab1_mk_ms_ko_po_nv = itab1_mk_ms_ko_po.join(lt_konv_po,(itab1_mk_ms_ko_po.KNUMV==lt_konv_po.nv_KNUMV)&(itab1_mk_ms_ko_po['rdoc3_KPOSN']==lt_konv_po['KPOSN']),'LEFT').withColumn('GR Quantity (MT)',col('RCQTY')*col('NTGEW')/1000).withColumn('Freight',abs(col('QTY'))*col('KWERT')/col('PO_QTY')).withColumn('supp_quantity_in_mt' ,when(col("GEWEI") == "KG", col('QTY')*col('NTGEW')/1000).otherwise(col('QTY')*col('NTGEW')/1000))
# itab1_mk_ms_ko_po_nv.count()

# COMMAND ----------

t001w_1 = spark.sql("select * from {}.{}".format(processed_db_name,"DIM_PLANT_DEPOT")).select('plant_id', 'IS_PLANT')
# .where(col('IS_PLANT') == 1)

# COMMAND ----------

itab1_mk_ms_ko_po_nv_tw = itab1_mk_ms_ko_po_nv.join(t001w_1,itab1_mk_ms_ko_po_nv.chdr_WERKS== t001w_1.plant_id,'INNER').withColumn('DRIVER_NAME',lit(None).cast('string')).withColumn('STORES_MOVEMENT_TYPE',lit(None).cast('string')).withColumn('E_WAY_BILL_NO',lit(None).cast('string')).withColumn('STORES_MOVEMENT_TYPE',lit(None).cast('string')).withColumn('BASE_UNIT_OF_MEASURE_INFORMATION',lit(None).cast('string'))

# COMMAND ----------

df_final=itab1_mk_ms_ko_po_nv_tw.select(col('RDOC2').alias('INVOICE_NO'),col('FKDAT').alias('INVOICE_DATE'),col('RITEM2').alias('INVOICE_LINE_ID'),col('NETWR').alias('NET_VALUE'),col('EXNUM').alias('CHALLAN_NO'),col('RITEM1').alias('REFERENCE_ITEM_INFORMATION'),col('IGST_RATE'),col('CGST_RATE'),col('SGST_RATE'),col('KUNWE').alias('RECEIVING_PLANT'),col('chdr_WERKS').alias('SUPPLYING_PLANT'),col('RCQTY').alias('REC_QUANTITY'),col('cdtl_MEINS').alias('SALES_UNIT_INFORMATION'),col('MATNR').alias('MATERIAL_NO'),col('supp_quantity_in_mt').alias('SUPP_QTY_MT'),col('BASE_UNIT_OF_MEASURE_INFORMATION'),col('EBELN').alias('PO_NUMBER'),col('EBELP').alias('PO_ITEM'),col('Freight').alias('FREIGHT'),col('LIFNR').alias('TRANSPORTER_CODE'),col('QTY').alias('SUPP_QUANTITY'),col('EXDAT').alias('SUPPLYING_DATE'),col('GR Quantity (MT)').alias('REC_QUANTITY_MT')).fillna({'REC_QUANTITY_MT': 0, 'REC_QUANTITY': 0})
# df_final.count()

# COMMAND ----------

union_zpst1_zdst1=df_final

# COMMAND ----------

last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

final_1 = union_zpst1_zdst1.withColumn("last_executed_time", lit(last_processed_time_str))

# COMMAND ----------

#logic for finance
dim_plant = spark.sql("select plant_id,IS_PLANT,POSTAL_CODE from {}.{}".format(processed_db_name,"DIM_PLANT_DEPOT"))

dim_product = spark.sql("select MATERIAL_NUMBER,PRODUCT_LINE,SBU from {}.{}".format(processed_db_name,"DIM_PRODUCT")).withColumn("SALES_ORG",when((col("SBU") == 'SBU 1'),"1000").when((col("SBU") == 'SBU 2'),"2000").when((col("SBU") == 'SBU 3'),"3000").otherwise(col("SBU")))

final_2 = final_1.alias("pla").join(dim_plant, on = [final_1["SUPPLYING_PLANT"] == dim_plant["plant_id"]],
                                   how='left').select([col("pla." + cols) for cols in final_1.columns] + [col("IS_PLANT").alias("SUPPLYING_IS_PLANT"),col("POSTAL_CODE").alias("SUPPLYING_POSTAL_CODE")])


final_3 = final_2.alias("plr").join(dim_plant, on = [final_2["RECEIVING_PLANT"] == dim_plant["plant_id"]],
                                   how='left').select([col("plr." + cols) for cols in final_2.columns] + [col("IS_PLANT").alias("RECEIVING_IS_PLANT"),col("POSTAL_CODE").alias("RECEIVING_POSTAL_CODE")])


final_4 = final_3.withColumn("TRANS_TYPE",when(((col("SUPPLYING_IS_PLANT") == 1) & (col("RECEIVING_IS_PLANT") == 0)),lit("PLANT TO DEPOT")))
final_5 = final_4.withColumn("TRANS_TYPE",when(((col("SUPPLYING_IS_PLANT") == 0) & (col("RECEIVING_IS_PLANT") == 0)),lit("DEPOT TO DEPOT")).otherwise(col("TRANS_TYPE")))


final_6 = final_5.alias("l_df").join(dim_product, on = [final_5["MATERIAL_NO"] == dim_product["MATERIAL_NUMBER"]],
                                   how='left').select([col("l_df." + cols) for cols in final_5.columns] + [col("PRODUCT_LINE"),col("SALES_ORG")])

final_7 = final_6.withColumn("SALES_TYPE",when((col("SALES_ORG").isin(["3000"])),lit("RETAIL")).otherwise(lit("<DEFAULT>")))

# display(final_7.filter("product_line is null"))

# COMMAND ----------

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

final = final_7.alias("l_df").join(final_geo_df.alias("r_df"), on = [
                                                                  final_7["SALES_ORG"] == final_geo_df["SBU"],
                                                                                    final_7["product_line"] == final_geo_df["product_line"],
  final_7["SALES_TYPE"] == final_geo_df["SALE_TYPE"],
  final_7["RECEIVING_POSTAL_CODE"] == final_geo_df["PINCODE"]
], how = 'left').select(
  [col("l_df." + cols) for cols in final_7.columns] + [col("DISTRICT"),col("SALES_GROUP"),col("STATE"),col("ZONE"),col("COUNTRY")])

# COMMAND ----------

final.write.mode('overwrite').parquet(processed_location+'ZMM_STOREG', mode='overwrite')
final = spark.read.parquet(processed_location+'ZMM_STOREG')
final.createOrReplaceTempView("final_union")

# COMMAND ----------

pmu = ProcessMetadataUtility()
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###SURROGATE IMPLEMENTATION

# COMMAND ----------

db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
  
  final.createOrReplaceTempView("{}".format(fact_name))
  
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
      tmp_unmapped_str += "({fact_column} is NULL OR trim({fact_column}) = '')" \
                        " THEN -2 ELSE ifnull(A{count}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column,count=count , fact_name=fact_name,dim_table=dim_table)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
          
      join_condition += "\n left join {pro_db}.{dim_table} A{count} on {fact_table}.{fact_column} = A{count}.{dim_column}".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name, count=count)
  join_condition = join_condition + """ \n
  left join pro_prod.dim_territory B 
  ON
      ({fact_name}.SALES_TYPE = B.SALE_TYPE) AND
      (({fact_name}.PRODUCT_LINE = B.PRODUCT_LINE) OR ({fact_name}.PRODUCT_LINE is NULL AND B.PRODUCT_LINE is NULL)) AND
      (({fact_name}.RECEIVING_POSTAL_CODE = B.PINCODE) OR ({fact_name}.RECEIVING_POSTAL_CODE is NULL AND B.PINCODE is NULL)) AND
      (({fact_name}.SALES_ORG = B.SALES_ORG) OR ({fact_name}.SALES_ORG is NULL AND B.SALES_ORG is NULL)) AND
      (({fact_name}.DISTRICT = B.DISTRICT) OR ({fact_name}.DISTRICT is NULL AND B.DISTRICT is NULL)) AND
      (({fact_name}.SALES_GROUP = B.SALES_GROUP) OR ({fact_name}.SALES_GROUP is NULL AND B.SALES_GROUP is NULL)) AND
      (({fact_name}.STATE = B.STATE) OR ({fact_name}.STATE is NULL AND B.STATE is NULL)) AND
      (({fact_name}.ZONE = B.ZONE) OR ({fact_name}.ZONE is NULL AND B.ZONE is NULL)) AND
      (({fact_name}.COUNTRY = B.COUNTRY) OR ({fact_name}.COUNTRY is NULL AND B.COUNTRY is NULL))
     """.format(pro_db=processed_db_name,fact_name=fact_name)
  
  select_condition = select_condition[:-2]
  query = """select
  INVOICE_DATE,
  INVOICE_NO,
INVOICE_LINE_ID,
NET_VALUE,
CHALLAN_NO,
IGST_RATE,
CGST_RATE,
SGST_RATE,
REC_QUANTITY,
SALES_UNIT_INFORMATION,
SUPP_QTY_MT,
BASE_UNIT_OF_MEASURE_INFORMATION,
PO_NUMBER,
PO_ITEM,
FREIGHT,
TRANSPORTER_CODE,
SUPP_QUANTITY,
REC_QUANTITY_MT,
TRANS_TYPE
  ,last_executed_time as LAST_EXECUTED_TIME
  ,CASE WHEN ({fact_name}.RECEIVING_POSTAL_CODE is NULL OR 
            {fact_name}.product_line is NULL or 
            {fact_name}.SALES_ORG is NULL or
            {fact_name}.DISTRICT is NULL or
            {fact_name}.SALES_GROUP is NULL or
            {fact_name}.STATE is NULL or
            {fact_name}.ZONE is NULL or
            {fact_name}.COUNTRY is NULL or
            trim({fact_name}.product_line)='' or 
            trim({fact_name}.RECEIVING_POSTAL_CODE) = '' or
            trim({fact_name}.SALES_ORG)='' or
            trim({fact_name}.DISTRICT)='' or
            trim({fact_name}.SALES_GROUP) = '' or
            trim({fact_name}.STATE) = '' or
            trim({fact_name}.ZONE) = '' or
            trim ({fact_name}.COUNTRY) = '') THEN -2 ELSE ifnull(B.GEO_KEY, -1) END as GEO_KEY
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

# creating surrogate key
d = surrogate_mapping_hil(csv_data,"FACT_STOCK_MOVEMENT","ZMM_STOREG",processed_db_name)

# COMMAND ----------

# writing data to processed layer table
d.write.insertInto(processed_db_name + "." + "FACT_LOGISTICS_STO", overwrite=True)

# COMMAND ----------

# inserting log record to last_execution_details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
