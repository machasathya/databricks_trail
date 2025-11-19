# Databricks notebook source
from datetime import datetime 
from pyspark.sql.functions import col, lit, expr, to_date, to_timestamp, sum, min, when, count, concat,upper
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql.functions import rank, min,current_date
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
import numpy as np

# COMMAND ----------

spark.conf.set("spark.sql.broadcastTimeout",36000)

# COMMAND ----------

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
table_name = "FACT_SALES_INVOICE"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# meta_table = "fact_surrogate_meta"
# schema_name = "metadata_prod"
# curated_db_name = "cur_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# scope = "AZR-DBR-KV-SCOPE-300"
# processed_path = "mnt/bi_datalake/prod/pro/"
# processed_schema_name = "global_semantic_prod"
# processed_db_name = "pro_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_SALES_INVOICE"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

konv = spark.sql("select * from(select *, rank() over (partition by knumv, kposn, kschl order by knumv, kposn, kschl) as rank from {}.{}) a where rank=1".format(curated_db_name,"konv"))

# COMMAND ----------

zsd_tax_turn = spark.sql("select * FROM {}.{}".format(curated_db_name, "ZSD_TAX_TURNOVER"))

# COMMAND ----------

kna1 = spark.sql("select kunnr,pstlz,land1 FROM {}.{}".format(curated_db_name, "KNA1"))

# COMMAND ----------

vbkd = spark.sql("select * FROM {}.{}".format(curated_db_name, "VBKD"))

# COMMAND ----------

plant_mast = spark.sql("select * from {}.{}".format(processed_db_name,"DIM_PLANT_DEPOT"))

# COMMAND ----------

zsbu3 = spark.sql("select * from {}.{}".format(curated_db_name,"ZSBU3"))

# COMMAND ----------

# DBTITLE 1,Sales Document: Header Data
vbak = spark.sql("select * FROM {}.{}".format(curated_db_name, "VBAK"))

# COMMAND ----------

vbap = spark.sql("select vbeln, posnr, route, mfrgr FROM {}.{}".format(curated_db_name, "VBAP"))

# COMMAND ----------

knvv = spark.sql("select kunnr, vkorg, vtweg, vkbur, vkgrp,kdgrp, bzirk, spart FROM {}.{}".format(curated_db_name, "KNVV"))

# COMMAND ----------

vbpa = spark.sql("select vbeln, adrnr, kunnr, pernr, lifnr, parvw FROM {}.{}".format(curated_db_name, "VBPA"))

# COMMAND ----------

# PARQUET NULL ISSUE
vbrk = spark.sql("select vbeln,fkdat, kdgrp,kunag, kunrg, vkorg,vtweg,inco1,inco2, kurrf, konda, waerk, fkart,zzveh_no, zzlr_no,zzdr_no, zzewaybill_no, regio, bzirk, knumv,fksto FROM {}.{} where rfbsk == 'C' AND (fksto is null or fksto=' ' or fksto = '') AND fkart NOT IN ('S1','S2','S3') ".format(curated_db_name, "VBRK"))

# COMMAND ----------

vbrp = spark.sql("select vbeln,aubel,aupos,lgort, matnr, matkl, posnr, voleh, spart, vrkme, werks, cmpre,arktx, prsdt, vgbel, vgpos, kondm, fkimg, brgew, netwr, mwsbp,bwtar,volum FROM {}.{} where fkimg > 0  ".format(curated_db_name, "VBRP"))

# COMMAND ----------

excdtl = spark.sql("select * from (select exdat, exnum, rdoc1, ritem1, zeile, docno, docyr, trntyp, rank() over (partition by rdoc1, ritem1 order by exnum) as rank from {}.{}) where rank = 1 ".format(curated_db_name,"J_1IEXCDTL"))

# COMMAND ----------

bkpf = spark.sql("select belnr, awkey, bukrs, gjahr from {}.{}".format(curated_db_name,"bkpf")).where("blart IN ( 'RV' , 'ZC' )")

# COMMAND ----------

bseg = spark.sql("select bupla, bukrs,gjahr,belnr,buzei from {}.{} where buzei = 1".format(curated_db_name,"bseg"))

# COMMAND ----------

wa_taxturn = konv.alias('konv').join(zsd_tax_turn.alias('zsd'), on=['kschl'], how='left').select([col("konv." + cols) for cols in konv.columns] + [col("zsd.zctyp")])

# COMMAND ----------

marm = spark.sql("select matnr, meinh, umren, umrez from {}.{} where meinh = 'M3'".format(curated_db_name,"marm"))

# COMMAND ----------

c_gt_vbrp = vbrk.alias('vbrk').join(vbrp.alias('vbrp'), on=['vbeln'], how='inner').select(
    [col(cols) for cols in vbrp.columns] + [col("vbrk.fkdat"), col("vbrk.kdgrp"), col("vbrk.kunag"), col("vbrk.kunrg"),
                                            col("vbrk.vkorg"), col("vbrk.vtweg"), col("vbrk.inco1"), col("vbrk.inco2"),
                                            col("vbrk.konda"), col("vbrk.waerk"), col("vbrk.fkart"), col("vbrk.knumv"),
                                            col("vbrk.zzveh_no"), col("vbrk.kurrf"),
                                            col("vbrk.zzlr_no"), col("vbrk.zzdr_no"), col("vbrk.zzewaybill_no"),col("vbrk.fksto"),
                                            col("regio"), col("bzirk")])

# COMMAND ----------

it_knvv = vbrk.alias('vbrk').join(knvv.alias('knvv'), on=[vbrk["kunrg"] == knvv["kunnr"],
                                                         vbrk["vkorg"] == knvv["vkorg"],
                                                         vbrk["vtweg"] == knvv["vtweg"]], how='left').select(
    [col("knvv." + cols) for cols in knvv.columns])

# COMMAND ----------

c_gt_bkpf = vbrk.alias('vbrk').join(bkpf.alias('bkpf'), vbrk["vbeln"] == bkpf["awkey"], how='inner').select(
    [col(cols) for cols in bkpf.columns])

# COMMAND ----------

c_gt_bseg = c_gt_bkpf.alias('bkpf').join(bseg.alias('bseg'), on=[bkpf["bukrs"]==bseg["bukrs"],
                                                                   bkpf["belnr"]==bseg["belnr"],
                                                                   bkpf["gjahr"]==bseg["gjahr"]], how='left').select(
     [col("bseg." + cols) for cols in bseg.columns] + [col("bkpf.awkey")]).distinct()

# COMMAND ----------

c_gt_excise = c_gt_vbrp.alias("c_gt_vbrp").join(excdtl.alias("j_1iexcdtl"), [c_gt_vbrp["vgbel"] == excdtl["rdoc1"],
                                                                             c_gt_vbrp["vgpos"] == excdtl["ritem1"]],
                                                'left').select([col(cols) for cols in excdtl.columns])

# COMMAND ----------

c_gt_vbpa = vbrk.alias('vbrk').join(vbpa.alias('vbpa'), on=['vbeln'], how='inner').select(
    [col(cols) for cols in vbpa.columns] + [col("kdgrp")])


c_gt_vbak = c_gt_vbrp.alias("c_gt_vbrp").join(vbak.alias("vbak"), c_gt_vbrp["aubel"] == vbak["vbeln"], 'inner').select(
    [col("vbak." + cols) for cols in vbak.columns])

c_gt_vbkd = c_gt_vbrp.alias("c_gt_vbrp").join(vbkd.alias("vbkd"), [c_gt_vbrp["aubel"] == vbkd["vbeln"],
                                                                   c_gt_vbrp["aupos"] == vbkd["posnr"]], 'inner').select(
    [col("vbkd." + cols) for cols in vbkd.columns])


c_gt_vbap = c_gt_vbrp.alias("c_gt_vbrp").join(vbap.alias("vbap"), [c_gt_vbrp["aubel"] == vbap["vbeln"],
                                                                   c_gt_vbrp["aupos"] == vbap["posnr"]], 'inner').select(
    [col("vbap." + cols) for cols in vbap.columns])

# COMMAND ----------

c_gt_konv = vbrk.alias("vbrk").join(wa_taxturn.alias("konv"), on=[vbrk["knumv"] == wa_taxturn["knumv"]], how='inner').select(
    [col("konv." + cols) for cols in wa_taxturn.columns])

# COMMAND ----------

# temp_df = c_gt_konv.groupBy("knumv", "kposn", "kschl").agg(sum("kwert").alias("kwert"))

# c_gt_konv = temp_df.alias("temp_df").join(c_gt_konv.alias("c_gt_konv"), on=[temp_df["knumv"] == c_gt_konv["knumv"],
#                                                                             temp_df["kposn"] == c_gt_konv["kposn"], 
#                                                                             temp_df["kschl"] == c_gt_konv["kschl"]], how='inner').select(
#     [col("c_gt_konv." + cols) for cols in c_gt_konv.columns if cols.lower() != "kwert"] + [col("temp_df.kwert")])

# COMMAND ----------

ls_final_base = c_gt_vbrp.selectExpr("vbeln", "vgbel", "vgpos" , "aubel", "aupos", "fkimg", "lgort","bwtar", "matnr", "matkl", "posnr", "voleh",
                                "spart", "(brgew/1000) as brgew", "vrkme", "werks", "cmpre", "arktx AS maktx", "netwr", "mwsbp",
                                "(netwr + mwsbp) as total", "prsdt", "\"MT\" as gewei", "fkdat", "kunag",
                                "kunrg", "vkorg", "vtweg", "inco1", "inco2", "konda", "waerk", "fkart", "zzveh_no","kondm", "knumv",
                                "zzlr_no", "zzdr_no", "zzewaybill_no", "regio", "bzirk", "volum", "kurrf","fksto")

#                                            how='left').select(
#     [col("base." + cols) for cols in ls_final_base.columns] + [col("marm.umren"), col("marm.umrez")]).withColumn('volum', ((col('umren') / col('umrez')) * col('fkimg')))
  
ls_final_1 = ls_final_base.alias("base").join(marm.alias("marm"), on=['matnr'],how='left').select(
    [col("base." + cols) for cols in ls_final_base.columns] + [col("marm.umren"), col("marm.umrez")]).withColumn('volum', ((col('umren') / col('umrez')) * col('fkimg')))

# COMMAND ----------

ls_final_temp = ls_final_1.alias('ls').join(plant_mast.alias("plant"),ls_final_1["werks"] == plant_mast["plant_id"],
                                              how='left').select(
  [col("ls." + cols) for cols in ls_final_1.columns] + [col("IS_PLANT"),col("REPORT_PLANT_ID"),col('PLANT_ID')]
)

# COMMAND ----------

ls_final_temp1 = ls_final_temp.withColumn("TEMP_PLANT_ID",when(col("IS_PLANT") == 1,col("werks")).otherwise(col("bwtar")))
#new Logic for Freight
ls_final_temp1 = ls_final_temp1.withColumn("TRANS_TYPE",when(col("IS_PLANT") == 1,"PLANT TO CUSTOMER"))
ls_final_temp1 = ls_final_temp1.withColumn("TRANS_TYPE",when(col("IS_PLANT") == 0,"DEPOT TO CUSTOMER").otherwise(col("TRANS_TYPE")))

ls_final_temp1 = ls_final_temp1.withColumn("TRANS_TYPE",when(((col("VKORG").isin(["2000","3000"])) & ((col("plant_id")) != (col("report_plant_id")))),"PLANT TO CUSTOMER").otherwise(col("TRANS_TYPE")))

ls_final_temp2 = ls_final_temp1.alias('ls').join(zsbu3,[ls_final_temp1["matnr"] == zsbu3["matnr"],
                                                ls_final_temp1["TEMP_PLANT_ID"] == zsbu3["werks"]],
                                                how='left').select(
  [col("ls." + cols) for cols in ls_final_temp1.columns] + [col("STD_WT").alias("ZSBU3_STD_WT"),col("VALID_FROM"),col("VALID_TO")]
).where((col("fkdat").between(col("VALID_FROM"),col("VALID_TO"))) | ((col("VALID_FROM").isNull()) & (col("VALID_TO").isNull()))).drop("TEMP_PLANT_ID","VALID_FROM","VALID_TO","IS_PLANT")

# COMMAND ----------

ls_final_2 = ls_final_temp2.alias("ls_final_2").join(c_gt_bkpf.alias("c_gt_bkpf_2"), ls_final_1["vbeln"] == c_gt_bkpf["awkey"],
                                           how='left').select(
    [col("ls_final_2." + cols) for cols in ls_final_temp2.columns] + [col("c_gt_bkpf_2.belnr"), col("c_gt_bkpf_2.bukrs"),
                                                              col("c_gt_bkpf_2.gjahr")])

# COMMAND ----------

ls_final_3 = ls_final_2.alias("ls_final_3").join(c_gt_bseg.alias("c_gt_bseg_3"), [ls_final_2["bukrs"] == c_gt_bseg["bukrs"],
                                                                          ls_final_2["belnr"] == c_gt_bseg["belnr"],
                                                                          ls_final_2["gjahr"] == c_gt_bseg["gjahr"]],
                                           how='left').select(
    [col("ls_final_3." + cols) for cols in ls_final_2.columns] + [col("c_gt_bseg_3.bupla")])

# COMMAND ----------

ls_final_4 = ls_final_3.alias("ls_final_4").join(c_gt_excise.alias("c_gt_excise_4"), [ls_final_3["vgbel"] == c_gt_excise["rdoc1"],
                                                                              ls_final_3["vgpos"] == c_gt_excise[
                                                                                  "ritem1"]], how='left').select(
    [col("ls_final_4." + cols) for cols in ls_final_3.columns] + [col("c_gt_excise_4.exdat"),
                                                                col("c_gt_excise_4.exnum")]).distinct()

# COMMAND ----------

ls_final_bzirk = ls_final_4.alias("ls_final_bzirk").join(it_knvv.alias("it_knvv"), on=[ls_final_4['kunag'] == it_knvv['kunnr'],
                                                         ls_final_4["vkorg"] == knvv["vkorg"],
                                                         ls_final_4["vtweg"] == knvv["vtweg"],
                                                         ls_final_4["spart"] == knvv["spart"] ], how='left').distinct().select(
     [col("ls_final_bzirk." + cols) for cols in ls_final_4.columns if cols != 'bzirk'] + [when(ls_final_4.bzirk.isNull(), col("it_knvv.bzirk")).otherwise(col("ls_final_bzirk.bzirk")).alias("bzirk"), col("it_knvv.kdgrp"), col("it_knvv.vkgrp"), col("it_knvv.vkbur")]).distinct()

# COMMAND ----------

ls_final_5 = ls_final_bzirk.drop("kunrg").alias("ls_final_5").join(c_gt_vbpa.where("parvw='RG'").alias("c_gt_vbpa_5"), on=["vbeln"],
                                                         how='left').select(
    [col("ls_final_5." + cols) for cols in ls_final_bzirk.columns if cols != 'kunrg'] + [col("c_gt_vbpa_5.kunnr").alias("kunrg"),
                                                              col("c_gt_vbpa_5.adrnr").alias("payer_adrnr")])

# COMMAND ----------

ls_final_6 = ls_final_5.alias("ls_final_6").join(c_gt_vbpa.where("parvw='AG'").alias("c_gt_vbpa_6"), on=["vbeln"],
                                                         how='left').select(
    [col("ls_final_6." + cols) for cols in ls_final_5.columns] + [col("c_gt_vbpa_6.kunnr").alias("sold_kunnr"),
                                                              col("c_gt_vbpa_6.kunnr").alias("agent_kunnr"),
                                                                      col("c_gt_vbpa_6.adrnr").alias("sold_adrnr"),
                                                               col("c_gt_vbpa_6.adrnr").alias("agent_adrnr")])

# COMMAND ----------

ls_final_7 = ls_final_6.alias("ls_final_7").join(c_gt_vbpa.where("parvw='Z1'").alias("c_gt_vbpa_7"), on=["vbeln"],
                                           how='left').select(
    [col("ls_final_7." + cols) for cols in ls_final_6.columns] + [col("c_gt_vbpa_7.kunnr").alias("agent_part_kunnr"),
                                                               col("c_gt_vbpa_7.adrnr").alias("agent_part_adrnr")])

# COMMAND ----------

ls_final_8 = ls_final_7.alias("ls_final_8").join(c_gt_vbpa.where("parvw='RE'").alias("c_gt_vbpa_8"), on=["vbeln"],
                                           how='left').select(
    [col("ls_final_8." + cols) for cols in ls_final_7.columns] + [col("c_gt_vbpa_8.kunnr").alias("bill_kunnr"),
                                                               col("c_gt_vbpa_8.adrnr").alias("bill_adrnr")])

# COMMAND ----------

ls_final_9 = ls_final_8.alias("ls_final_9").join(c_gt_vbpa.where("parvw='WE'").alias("c_gt_vbpa_9"), on=["vbeln"],
                                           how='left').select(
    [col("ls_final_9." + cols) for cols in ls_final_8.columns] + [col("c_gt_vbpa_9.kunnr").alias("ship_kunnr"),
                                                                                   col("c_gt_vbpa_9.adrnr").alias("ship_adrnr")
                                                                                ]).distinct()

# COMMAND ----------

ls_final_10 = ls_final_9.alias("ls_final_10").join(c_gt_vbpa.where("parvw='VE'").alias("c_gt_vbpa_10"), on=["vbeln"],
                                           how='left').select(
    [col("ls_final_10." + cols) for cols in ls_final_9.columns] + [col("c_gt_vbpa_10.pernr").alias("sales_pernr")])

# COMMAND ----------

ls_final_11 = ls_final_10.alias("ls_final_11").join(c_gt_vbap.alias("c_gt_vbap_11"), [ls_final_10["aubel"] == c_gt_vbap["vbeln"],
                                                                          ls_final_10["aupos"] == c_gt_vbap["posnr"]],
                                           how='left').select(
    [col("ls_final_11." + cols) for cols in ls_final_10.columns] + [col("c_gt_vbap_11.route"), col("c_gt_vbap_11.mfrgr")]).distinct()

# COMMAND ----------

ls_final_12 = ls_final_11.alias("ls_final_12").join(c_gt_vbpa.where("parvw='SP'").alias("c_gt_vbpa_12"), on=["vbeln"],
                                           how='left').select(
    [col("ls_final_12." + cols) for cols in ls_final_11.columns] + [col("c_gt_vbpa_12.lifnr").alias("lifnr")])

# COMMAND ----------

ls_final_13 = ls_final_12.alias("ls_final_13").join(c_gt_vbak.alias("c_gt_vbak_13"), [ls_final_12["aubel"] == c_gt_vbak["vbeln"]],
                                           how='left').select(
    [col("ls_final_13." + cols) for cols in ls_final_12.columns] + [to_date(col("c_gt_vbak_13.bstdk").cast("string"), 'yyyyMMdd').alias("bstdk"),
                                                              col("c_gt_vbak_13.bstnk").alias("bstnk"),
                                                              col("c_gt_vbak_13.erdat").alias("erdat"),
                                                              col("c_gt_vbak_13.segment").alias("segment"),
                                                              col("c_gt_vbak_13.application").alias("application"),
                                                              col("c_gt_vbak_13.order_type").alias("order_type")]).distinct()

# COMMAND ----------

ls_final_14 = ls_final_13.alias("ls_final_14").join(c_gt_vbkd.alias("c_gt_vbkd_14"), on=[ls_final_13["aubel"] == c_gt_vbkd["vbeln"]],
                                           how='left').select(
    [col("ls_final_14." + cols) for cols in ls_final_13.columns]).distinct().withColumn("so_vbeln", ls_final_13.aubel).withColumn("posnr_va", ls_final_13.aupos)


# COMMAND ----------

ls_final_15 = ls_final_14.select("*", when((ls_final_14.kunag != ls_final_14.kunrg), 'Sub Dealer').when((ls_final_14.kunag == ls_final_14.kunrg) & (ls_final_14.vtweg == 20),'Stockist').otherwise('Direct').alias('cust_type'))

# COMMAND ----------

ls_final_15_1 = ls_final_15.alias("ls_final_16").join(kna1.alias("kna_16"), on=[col("ls_final_16.kunrg") == col("kna_16.kunnr")], how='left').select([col("ls_final_16." + cols) for cols in ls_final_15.columns] + [col('pstlz'),col('land1')])

# COMMAND ----------

#-- Get geo hierarchy for Ship to Pincode
ls_final_15_3 = ls_final_15_1.alias("ls_1").join(kna1.alias("kna_16"), on=[col("ls_1.ship_kunnr") == col("kna_16.kunnr")], how='left').select([col("ls_1." + cols) for cols in ls_final_15_1.columns] + [col("kna_16.pstlz").alias("shipto_pstlz")])

#kna1_ship = spark.sql("select kunnr,pstlz as pstlz_ship,land1 as land1_ship FROM {}.{}".format(curated_db_name, "KNA1"))
#ls_final_15_1 = ls_final_15_1.alias("ls_final_16_ship").join(kna1_ship.alias("kna_16_ship"), on=[col("ls_final_16_ship.ship_kunnr") == col("kna_16_ship.kunnr")], how='left').select([col("ls_final_16_ship." + cols) for cols in ls_final_15_1.columns] + [col('pstlz_ship'),col('land1_ship')])

# COMMAND ----------

export_pin = spark.sql("select * FROM {}.{}".format(curated_db_name, "EXPORT_PINCODES"))

# COMMAND ----------

ls_final_15_2 = ls_final_15_3.alias("ls_1").join(kna1.alias("kna_16"), on=[col("ls_1.ship_kunnr") == col("kna_16.kunnr")], how='left').withColumn("land_2",when(((col("fkart") == "ZENV") & (col("ls_1.ship_kunnr") == col("kna_16.kunnr"))),col("kna_16.land1")).otherwise(col("ls_1.land1"))).select([col("ls_1." + cols) for cols in ls_final_15_3.columns if cols not in {"land1"}]+[col("land_2")])

# COMMAND ----------

ls_final_16 = ls_final_15_2.alias("ls_2").join(export_pin.alias("exp"), on=[col("ls_2.land_2") == col("exp.COUNTRY_KEY")], how='left').withColumn("pstlz",when(((col("fkart") == "ZENV") & (col("ls_2.land_2") == col("exp.COUNTRY_KEY"))),col("exp.PINCODE")).otherwise(col("pstlz"))).select([col("ls_2." + cols) for cols in ls_final_15_2.columns if cols not in {"pstlz"}]+[col("pstlz")])

# COMMAND ----------

ls_final_17 = ls_final_16.alias("ls_final_17").join(c_gt_konv.alias('c_gt_konv_17'), on=[ 
  col("ls_final_17.knumv") == col("c_gt_konv_17.knumv"),
  col("ls_final_17.posnr") == col("c_gt_konv_17.kposn")], how='left').distinct().select(
    [col("ls_final_17." + cols) for cols in ls_final_16.columns] + [col("c_gt_konv_17.kschl"),col("c_gt_konv_17.kwert"),col("c_gt_konv_17.kbetr"),col("c_gt_konv_17.kawrt"),col("c_gt_konv_17.zctyp")])

# COMMAND ----------

ls_final_17.createOrReplaceTempView("ls_final_17")

# COMMAND ----------

#PARQUET NULL ISSUE
spark.sql("select distinct a.knumv, a.posnr, a.kschl, a.kwert ,a.kawrt, cast(a.kurrf as decimal) kurrf, case when ( a.kdgrp is null or a.kdgrp ='') then '-' else a.kdgrp end as kdgrp from (select *, rank() over (partition by knumv, posnr, kschl order by knumv, posnr, kschl) as rank from ls_final_17 where kschl in ('ZASS', 'JEXP','ZEXP','JCST','ZVAT'))a where a.rank = 1").createOrReplaceTempView('ls_final_17_temp')

spark.sql("select distinct a.knumv, a.posnr, a.kschl, a.kwert, a.kbetr,a.inco1 from (select *, rank() over (partition by knumv, posnr, kschl order by knumv, posnr, kschl) as rank from ls_final_17)a where a.rank = 1").createOrReplaceTempView('ls_final_18_temp')

spark.sql("select distinct a.knumv, a.posnr, a.kschl, a.kwert, a.vkorg, a.spart from (select *, rank() over (partition by knumv, posnr, kschl order by knumv, posnr, kschl) as rank from ls_final_17)a where a.rank = 1").createOrReplaceTempView('ls_final_19_temp')

# COMMAND ----------

def zasval(kwert_list, kschl_list, kawrt_list, kurrf_list, kdgrp_list):
  list_kschl = list(kschl_list)
  list_kwert = list(kwert_list)
  list_kawrt = list(kawrt_list)
  list_kurrf = list(kurrf_list)
  list_kdgrp = list(kdgrp_list)
  if 'ZASS' in list_kschl:
    index_zass = list_kschl.index('ZASS')
    if list_kdgrp[index_zass] == 17:
      return list_kwert[index_zass]
  if 'JEXP' in list_kschl:
    index_zass = list_kschl.index('JEXP')
    return list_kawrt[index_zass]
  elif 'ZVAT' in list_kschl:
    index_zass = list_kschl.index('ZVAT')
    return list_kawrt[index_zass] * 875 / 1000
  elif 'JCST' in list_kschl:
    index_zass = list_kschl.index('JCST')
    return list_kawrt[index_zass] * 875 / 1000
  elif 'ZEXP' in list_kschl:
    index_zass = list_kschl.index('ZEXP')
    return list_kwert[index_zass] * list_kurrf[index_zass]
  else:
    if 'ZASS' in list_kschl:
      index_zass = list_kschl.index('ZASS')
      return list_kwert[index_zass]
    else:
      return 0.00

# COMMAND ----------

def secondary_frieght(inco_list, kschl_list, kbetr_list, kwert_list):
  list_kschl = list(kschl_list)
  list_kwert = list(kwert_list)
  list_kbetr = list(kbetr_list)
  list_inco = list(inco_list)
  sub_list = ['ZFR', 'FOR', 'CIP']
  lv_freight = 0.0
  sec_freight = 0.0
  if all(x not in list_inco for x in sub_list):
    if 'ZDFR' in list_kschl:
      index_zdfr = list_kschl.index('ZDFR')
      lv_freight = list_kbetr[index_zdfr]
    if 'ZFRF' in list_kschl:
      index_zfrf = list_kschl.index('ZFRF')
      lv_freight = list_kbetr[index_zfrf]
    if 'ZCNF' in list_kschl:
      index_zcnf = list_kschl.index('ZCNF')
      sec_freight = lv_freight * list_kwert[index_zcnf]
    return sec_freight
  else:
    return 0.0

# COMMAND ----------

def base_price(kschl_list, kwert_list, vkorg_list, spart_list):
  list_kschl = list(kschl_list)
  list_kwert = list(kwert_list)
  list_vkorg = list(vkorg_list)
  list_spart = list(spart_list)
  sum_zbasfin1 = 0
  if 'ZBP4' in list_kschl:
    index_zbp = list_kschl.index('ZBP4')
    sum_zbasfin1 = list_kwert[index_zbp]
    if 'ZAD1' in list_kschl:
      index_zad = list_kschl.index('ZAD1')
      if list_vkorg[index_zad] == '2000' and list_spart[index_zad] == '23':
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZADD' in list_kschl:
      index_zad = list_kschl.index('ZADD')
      if list_vkorg[index_zad] == '2000' and list_spart[index_zad] == '23':
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZHED' in list_kschl:
      index_zad = list_kschl.index('ZHED')
      if list_vkorg[index_zad] == '2000' and list_spart[index_zad] == '23':
        sum_zbasfin1 += list_kwert[index_zad]
  else:
    if 'ZBP1' in list_kschl:
      index_zad = list_kschl.index('ZBP1')
      if list_vkorg[index_zad] == '1000' and list_spart[index_zad] == '11':
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZBCC' in list_kschl:
      index_zad = list_kschl.index('ZBCC')
      if list_vkorg[index_zad] == '1000' and list_spart[index_zad] == '14':
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZBRD' in list_kschl:
      index_zad = list_kschl.index('ZBRD')
      if list_vkorg[index_zad] == '1000' and list_spart[index_zad] == '88':
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZBPB' in list_kschl:
      index_zad = list_kschl.index('ZBPB')
      if list_vkorg[index_zad] == '2000' and list_spart[index_zad] in ['21', '22', '23', '24', '25']:
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZAD1' in list_kschl:
      index_zad = list_kschl.index('ZAD1')
      if list_vkorg[index_zad] == '2000' and list_spart[index_zad] == '23':
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZADD' in list_kschl:
      index_zad = list_kschl.index('ZADD')
      if list_vkorg[index_zad] == '2000' and list_spart[index_zad] == '23':
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZHED' in list_kschl:
      index_zad = list_kschl.index('ZHED')
      if list_vkorg[index_zad] == '2000' and list_spart[index_zad] == '23':
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZTBM' in list_kschl:
      index_zad = list_kschl.index('ZTBM')
      if list_vkorg[index_zad] == '2000' and list_spart[index_zad] == '88':
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZBSP' in list_kschl:
      index_zad = list_kschl.index('ZBSP')
      if list_vkorg[index_zad] == '3000' and list_spart[index_zad] in ['31', '32', '33', '34', '35', '88']:
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZBPS' in list_kschl:
      index_zad = list_kschl.index('ZBPS')
      if list_spart[index_zad] == '99':
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZEDP' in list_kschl:
      index_zad = list_kschl.index('ZEDP')
      if list_vkorg[index_zad] == '4000' and list_spart[index_zad] == '41':
        sum_zbasfin1 += list_kwert[index_zad]
    elif 'ZSDP' in list_kschl:
      index_zad = list_kschl.index('ZSDP')
      if list_vkorg[index_zad] == '4000' and list_spart[index_zad] == '42':
        sum_zbasfin1 += list_kwert[index_zad]
  return sum_zbasfin1

# COMMAND ----------

spark.udf.register("zasval", zasval, DecimalType(25,2))
spark.udf.register("secondary_frieght", secondary_frieght, DecimalType(25,2))
spark.udf.register("base_price", base_price, DecimalType(25,2))

# COMMAND ----------

spark.sql("select knumv, posnr, zasval(collect_list(kwert), collect_list(kschl), collect_list(kawrt),collect_list(kurrf),collect_list(kdgrp)) as zasval from ls_final_17_temp group by knumv, posnr order by knumv, posnr").createOrReplaceTempView("ls_final_17_1")

spark.sql("select knumv, posnr, secondary_frieght(collect_list(inco1), collect_list(kschl), collect_list(kbetr),collect_list(kwert)) as secondary_freight from ls_final_18_temp group by knumv, posnr order by knumv, posnr").createOrReplaceTempView("ls_final_18_1")

spark.sql("select knumv, posnr, base_price(collect_list(kschl), collect_list(kwert), collect_list(vkorg),collect_list(spart)) as base_price from ls_final_19_temp group by knumv, posnr order by knumv, posnr").createOrReplaceTempView("ls_final_19_1")

# COMMAND ----------

zasval_df = spark.sql("select knumv, posnr, case when zasval is null then 0 else zasval end as zasval from(select * from ls_final_17_1)")

secondary_freight_df = spark.sql("select knumv, posnr, case when secondary_freight is null then 0 else secondary_freight end as zsc_frgt  from(select * from ls_final_18_1)")

zbasfin1 = spark.sql("select knumv, posnr, case when base_price is null then 0 else base_price end as zbasfin1 from(select * from ls_final_19_1)")

# COMMAND ----------

calc = spark.sql("""select knumv, posnr,  
                 sum(case when kschl!='ZF05' then kwert
                      else 0 end) as freight, 
                 sum(case when kschl!='ZTRD' then kwert 
                      else 0 end) as trade_dis,
                 sum(case when kschl!='JKFC' then kwert 
                      else 0 end) as jkfc_dis,
                 sum(case when kschl!='JKFC' then (kbetr / 10)
                      else 0 end) as jkfc_rate,
                 sum(case when fkart != 'ZFOC' and kschl='JEXP' then (kbetr / 10)
                      else 0 end) as p_jexp,
                 sum(case when fkart != 'ZFOC' and kschl='ZETX' then (kbetr / 10)
                      else 0 end) as p_zetx,
                 sum(case when fkart != 'ZFOC' and kschl='JCST' then (kbetr / 10)
                      else 0 end) as p_jcst,
                 sum(case when fkart != 'ZFOC' and kschl='ZVAT' then (kbetr / 10)
                      else 0 end) as p_zvat,
                 sum(case when fkart != 'ZFOC' and kschl='ZGTA' then (kbetr / 10)
                      else 0 end) as p_zgta,
                 sum(case when fkart != 'ZFOC' and kschl='ZTCS' then (kbetr / 10)
                      else 0 end) as p_ztcs,
                 sum(case when fkart != 'ZFOC' and kschl='ZETO' then (kbetr / 10)
                      else 0 end) as p_zeto,
                 sum(case when fkart != 'ZFOC' and kschl='ZSSC' then (kbetr / 10)
                      else 0 end) as p_zssc,
                 sum(case when fkart != 'ZFOC' and kschl='ZB01' then (kbetr / 10)
                      else 0 end) as p_zb01,
                 sum(case when fkart != 'ZFOC' and kschl='ZRDF' then (kbetr / 10)
                      else kbetr end) as zrdf,
                 sum(case when kschl = 'ZIFR' then kwert * kurrf
                      else 0 end) as zifr,
                 sum(case when kschl = 'ZPAK' then kwert * kurrf
                      else 0 end) as zpak,
                 sum(case when kschl = 'ZFOB' then kwert * kurrf
                      else 0 end) as zfob,
                 sum(case when kschl = 'ZCMP' then kwert * kurrf
                      else 0 end) as zcmp,
                 sum(case when kschl = 'JOCG'then kbetr / 10
                      else 0 end) as cgst_rate,
                 sum(case when kschl = 'JOCG'then kwert
                      else 0 end) as cgst_amnt,
                 sum(case when kschl = 'JOSG'then kbetr / 10
                      else 0 end) as sgst_rate,
                 sum(case when kschl = 'JOSG'then kwert
                      else 0 end) as sgst_amnt,
                 sum(case when kschl = 'JOIG'then kbetr / 10
                      else 0 end) as igst_rate,
                 sum(case when kschl = 'JOIG'then kwert
                      else 0 end) as igst_amnt,
                 sum(case when kschl = 'JOUG'then kbetr / 10
                      else 0 end) as ugst_rate,
                 sum(case when kschl = 'JOUG'then kwert
                      else 0 end) as ugst_amnt,
                 sum(case when kschl='ZF00' then kwert
                      else 0 end) as zf00,
                 sum(case when kschl='ZF02' then kwert
                      else 0 end) as frgt_zf02,
                 sum(case when kschl='ZF01' then kwert
                      else 0 end) as frgt_zf01,
                 sum(case when kschl = 'ZBPB' then kwert / fkimg
                      when kschl = 'ZTBM' then kwert /fkimg
                      else 0 end) as zbpb_pu,
                 sum(case when zctyp is not null then kwert
                      else 0 end) as ztaxturn,
                 sum(case when zctyp = 'PRICE' then kwert
                      else 0 end) as zbasfin,
                 sum(case when kschl = 'ZMUL' then kwert
                      else 0 end) as zmul,
                 sum(case when kschl = 'ZINS' then kwert
                      else 0 end) as zins,
                 sum(case when kschl = 'ZMRP' then kwert
                      else 0 end) as zmrp,
                 sum(case when kschl = 'ZD01' then kwert * -1
                      else 0 end) as zd01,
                 sum(case when kschl = 'ZD02' then kwert * -1
                      else 0 end) as zd02,
                 sum(case when kschl = 'ZD03' then kwert * -1
                      else 0 end) as zd03,
                 sum(case when kschl = 'ZD04' then kwert
                      else 0 end) as zd04,
                 sum(case when kschl = 'ZD05' then kwert
                      else 0 end) as zd05,
                 sum(case when kschl = 'ZOD1' then kwert
                      else 0 end) as zod1,
                 sum(case when kschl = 'ZOD2' then kwert
                      else 0 end) as zod2,
                 sum(case when kschl = 'ZOD4' then kwert 
                      else 0 end) as zod4,
                 sum(case when kschl = 'ZOD5' then kwert
                      else 0 end) as zod5,
                 sum(case when kschl = 'ZOD6' then kwert
                      else 0 end) as zod6,
                 sum(case when kschl = 'ZRPB' then kwert
                      else 0 end) as zrpb,
                 sum(case when kschl = 'ZRPC' then kwert
                      else 0 end) as zrpc,
                 sum(case when kschl = 'ZEXP' then kwert
                      else 0 end) as zexp
                 from ls_final_17 group by knumv, posnr""")

# COMMAND ----------

interim_df = zasval_df.alias('zas').join(secondary_freight_df.alias('frght'), on=['knumv', 'posnr'], how='right').select(col('frght.knumv'), col('frght.posnr'), col('zas.zasval'), col('frght.zsc_frgt'))

# COMMAND ----------

interim_df = interim_df.na.fill({'zasval': 0})

# COMMAND ----------

interim_df = interim_df.alias('inter').join(calc.alias('calc'), on=['knumv', 'posnr'], how='inner').select([col('inter.knumv'), col('inter.posnr'), col('inter.zasval'), col('inter.zsc_frgt')] + [col("calc." + cols) for cols in calc.columns if cols not in ('knumv', 'posnr')])

# COMMAND ----------

interim_df = interim_df.alias('inter').join(zbasfin1.alias('base'), on=['knumv', 'posnr'], how='inner').select([col("inter." + cols) for cols in interim_df.columns] + [col('base.zbasfin1')])

# COMMAND ----------

ls_final_19 = ls_final_16.alias("ls_final_19").join(interim_df.alias('calc'), on=['knumv', 'posnr'], how='left').select(
    [col("ls_final_19." + cols) for cols in ls_final_16.columns if cols != 'kurrf'] + [col("calc." + columns) for columns in interim_df.columns if columns not in ['knumv', 'posnr']])

# COMMAND ----------

ls_final_20_1 = ls_final_19.withColumn("zasval", (ls_final_19.zasval - ls_final_19.frgt_zf02))
# display(ls_final_20.filter("vbeln in ('7000102012','7000102008')"))

# COMMAND ----------

ls_final_20 = ls_final_20_1.withColumn("zasval",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("zasval"))).withColumn("zf00",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("zf00"))).withColumn("frgt_zf01",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("frgt_zf01"))).withColumn("frgt_zf02",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("frgt_zf02"))).withColumn("p_zb01",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("p_zb01"))).withColumn("cgst_rate",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("cgst_rate"))).withColumn("cgst_amnt",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("cgst_amnt"))).withColumn("sgst_rate",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("sgst_rate"))).withColumn("sgst_amnt",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("sgst_amnt"))).withColumn("igst_rate",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("igst_rate"))).withColumn("igst_amnt",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("igst_amnt"))).withColumn("ugst_rate",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("ugst_rate"))).withColumn("ugst_amnt",when(col("fkart") == 'ZFOC',lit(0)).otherwise(col("ugst_amnt")))

# COMMAND ----------

dim_product = spark.sql("select * from {}.{}".format(processed_db_name,"dim_product"))

# COMMAND ----------

ls_final_21  = ls_final_20.alias("ls_final_21").join(dim_product.alias("dim_prod"), on=[col("ls_final_21.matnr") == col("dim_prod.MATERIAL_NUMBER")], how='left').select([col("ls_final_21." + columns) for columns in ls_final_20.columns] + [col("dim_prod.PRODUCT_LINE").alias("product_line"), col("dim_prod.CHANNEL").alias("channel")]).fillna({"product_line": "OTHERS"})

ls_final_21_temp1 = ls_final_21.withColumn("product_line",when(((col("product_line") == "OTHERS") & (col("vkorg") == '1000')),"SBU1_OTHERS").when(((col("product_line") == "OTHERS") & (col("vkorg") == '2000')),"SBU2_OTHERS").when(((col("product_line") == "OTHERS") & (col("vkorg") == '3000')),"SBU3_OTHERS").otherwise(col("product_line"))).withColumn("sales_org_type", when(((col('kdgrp') == 'Y4') | (col('kdgrp') == 'Y5')) & (col('vtweg') == 10) & (col('product_line') == "PIPES & FITTINGS"), "PROJECT").when(((col('kdgrp') != 'Y4') | (col('kdgrp') != 'Y5')) & (col('vtweg') != 10) & (col('product_line') == "PIPES & FITTINGS"), "RETAIL").otherwise("<DEFAULT>"))

ls_final_21_pf = ls_final_21_temp1.withColumn("ZMRP",when(((col("fkart") == 'ZFOC') & (col("product_line") == "PIPES & FITTINGS")),lit(0)).otherwise(col("ZMRP"))).withColumn("ZD01",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "PIPES & FITTINGS")),lit(0)).otherwise(col("ZD01"))).withColumn("ZD02",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "PIPES & FITTINGS")),lit(0)).otherwise(col("ZD02"))).withColumn("ZD03",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "PIPES & FITTINGS")),lit(0)).otherwise(col("ZD03"))).withColumn("ZD04",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "PIPES & FITTINGS")),lit(0)).otherwise(col("ZD04"))).withColumn("ZD05",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "PIPES & FITTINGS")),lit(0)).otherwise(col("ZD05")))

ls_final_21_wp = ls_final_21_pf.withColumn("ZRPB",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "WALL PUTTY")),lit(0)).otherwise(col("ZRPB"))).withColumn("ZRPC",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "WALL PUTTY")),lit(0)).otherwise(col("ZRPC"))).withColumn("ZOD1",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "WALL PUTTY")),lit(0)).otherwise(col("ZOD1"))).withColumn("ZOD2",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "WALL PUTTY")),lit(0)).otherwise(col("ZOD2"))).withColumn("zmul",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "WALL PUTTY")),lit(0)).otherwise(col("ZMUL"))).withColumn("ZOD4",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "WALL PUTTY")),lit(0)).otherwise(col("ZOD4"))).withColumn("ZOD5",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "WALL PUTTY")),lit(0)).otherwise(col("ZOD5"))).withColumn("ZOD6",when((((col("fkart") == 'ZFOC')) & (col("product_line") == "WALL PUTTY")),lit(0)).otherwise(col("ZOD6")))

ls_final_21_zexp = ls_final_21_wp.withColumn("ZMRP",when((((col("fkart") == 'ZENV')) & (col("product_line") == "PIPES & FITTINGS")),col("ZEXP")).otherwise(col("ZMRP"))).withColumn("ZRPC",when((((col("fkart") == 'ZENV'))&(col("product_line") == "WALL PUTTY")),col("ZEXP")).otherwise(col("ZRPC")))

ls_final_22 = ls_final_21_zexp.withColumn("sales_type", 
                                          when(((col('kdgrp') == 'Y1') | (col('kdgrp') == 'Y2') | (col('kdgrp') == 'Y3') | (col('kdgrp') == 'Y4') | (col('kdgrp') == 'Y5')) & (col('vtweg') == 10) & (col('product_line') == "PIPES & FITTINGS"), "PROJECT")
                                          .when((((col('vtweg') == 50) & (col('kdgrp') == 'Y1')) | ((col('vtweg') == 60) & (col('kdgrp') == 'Y2')) | ((col('vtweg') == 70) & (col('kdgrp') == 'Y3'))), "RETAIL")
                                          .when((((col('vtweg') == 90) & (col('kdgrp') == 'Y6')) & (col("product_line") == "PIPES & FITTINGS")),"EXPORTS")
                                          .when((((col('vtweg') == 10) & (col('kdgrp') == 'Y7')) & (col("product_line") == "PIPES & FITTINGS")),"P&F_OTHERS")
                                          .when((((col('vtweg') == 50) & (col('kdgrp') == 26)) & (col("product_line") == "WALL PUTTY")),"RETAIL")
                                          .when((((col('vtweg') == 10) & (col('kdgrp') == 23)) & (col("product_line") == "WALL PUTTY")),"PROJECT")
                                          .when(((col('vtweg') == 10) & (col('product_line') == "AC SHEETS")), "DIRECT/PROJECT").when(((col('vtweg') == 20) & (col('kunag') != col('kunrg')) & (col('product_line') == "AC SHEETS")), "RETAILER")
                                          .when(((col('vtweg') == 20) & (col('kunag') == col('kunrg')) & (col('product_line') == "AC SHEETS")), "STOCKIST").when(((col('vtweg') == 10) & (col('product_line') == "NON AC SHEETS")), "INSTITUTIONAL")
                                          .when(((col('vtweg') == 90) & (col('fkart') == 'ZENV') & (col('product_line') == "NON AC SHEETS")), "EXPORTS").when(((col('vtweg') == 20) & (col('product_line') == "NON AC SHEETS")), "RETAIL")
                                          .otherwise("OTHER"))

# COMMAND ----------

ls_final_zsbu3_1 = ls_final_22.withColumn('QUANTITY_EA',ls_final_22.brgew)

# COMMAND ----------

traded_mat = spark.sql("select matnr, meinh, umren, umrez from {}.{} where meinh = 'MT'".format(curated_db_name,"marm"))

# COMMAND ----------

ls_final_zsbu3_2 = ls_final_zsbu3_1.alias("base").join(traded_mat.alias("marm"), on= ['matnr'],
                                           how='left').select(
    [col("base." + cols) for cols in ls_final_zsbu3_1.columns] + [col("marm.umren").alias("umren_EA"), col("marm.umrez").alias("umrez_EA")]).withColumn('brgew',when(((ls_final_zsbu3_1.product_line == 'PIPES & FITTINGS')),(ls_final_zsbu3_1.fkimg * ls_final_zsbu3_1.ZSBU3_STD_WT)/1000).otherwise(col('brgew'))).drop("umren_EA","umrez_EA")

# COMMAND ----------

ls_final_23 = ls_final_zsbu3_2.withColumn('total_sales_value', when(ls_final_zsbu3_2.product_line == 'WALL PUTTY', ls_final_zsbu3_2.zasval)
                                          .when(ls_final_zsbu3_2.product_line == 'PIPES & FITTINGS', ls_final_zsbu3_2.netwr - (ls_final_zsbu3_2.frgt_zf01 + ls_final_zsbu3_2.frgt_zf02 + ls_final_zsbu3_2.zins))
                                          .when(ls_final_zsbu3_2.product_line == 'SMART FIX', ls_final_zsbu3_2.ztaxturn)
                                          .when((ls_final_zsbu3_2.product_line == 'BLOCKS') | (ls_final_zsbu3_2.product_line == 'PANELS') | (ls_final_zsbu3_2.product_line == 'BOARDS'), ls_final_zsbu3_2.zasval - ls_final_zsbu3_2.zmul)
                                          .when((((ls_final_zsbu3_2.product_line == 'CC SHEETS') | (ls_final_zsbu3_2.product_line == 'AC SHEETS') | (ls_final_zsbu3_2.product_line == 'NON AC SHEETS')) & (ls_final_zsbu3_2.fkart == 'ZCRE')), ls_final_zsbu3_2.zbasfin)
                                          .when((((ls_final_zsbu3_2.product_line == 'CC SHEETS') | (ls_final_zsbu3_2.product_line == 'AC SHEETS') | (ls_final_zsbu3_2.product_line == 'NON AC SHEETS')) & (ls_final_zsbu3_2.fkart != 'ZFOC')), ls_final_zsbu3_2.zbasfin)
                                          .when(ls_final_zsbu3_2.product_line == 'TILE ADHESIVE', ls_final_zsbu3_2.zasval)
                                          .when(((ls_final_zsbu3_2.product_line == 'SMART PLASTER') & (ls_final_zsbu3_2.channel == 'MANUFACTURED')), ls_final_zsbu3_2.zasval - ls_final_zsbu3_2.zmul)
                                          .when(((ls_final_zsbu3_2.product_line == 'SMART PLASTER') & (ls_final_zsbu3_2.channel == 'TRADED')), (ls_final_zsbu3_2.ztaxturn - ls_final_zsbu3_2.zmul))
                                          .when(col("product_line") == 'ENGINEERING DIVISION',ls_final_zsbu3_2.netwr)
                                          .when(col("product_line") == 'PARADOR FLOORING',ls_final_zsbu3_2.zasval).otherwise(0))

# COMMAND ----------

# first_cust_flag = ls_final_23.selectExpr("kunag", "CONCAT(vbeln,'-',posnr) as first_transaction_cond").groupBy("kunag").agg(min("first_transaction_cond").alias('FIRST_TRANSACTION')).selectExpr("kunag", "1 FIRST_CUST_FLAG", "substr(FIRST_TRANSACTION, 1, (instr(FIRST_TRANSACTION, '-')) - 1) FIRST_TRANSACTION", "substr(FIRST_TRANSACTION, instr(FIRST_TRANSACTION, '-') + 1, length(FIRST_TRANSACTION)) as posnr")

window = Window.partitionBy("kunag").orderBy("fkdat","vbeln","posnr").rowsBetween(Window.unboundedPreceding, Window.currentRow)
first_cust_flag = ls_final_23.withColumn("rank", rank().over(window)).where("rank = 1").selectExpr("kunag","1 FIRST_CUST_FLAG","vbeln FIRST_TRANSACTION","posnr")

# COMMAND ----------

# first_prd_sales_flg = ls_final_23.selectExpr("product_line", "CONCAT(vbeln,'-',posnr) as first_transaction_cond").groupBy("product_line").agg(min("first_transaction_cond").alias('FIRST_TRANSACTION')).selectExpr("product_line", "1 FIRST_PRD_SALES_FLAG", "substr(FIRST_TRANSACTION, 1, (instr(FIRST_TRANSACTION, '-')) - 1) FIRST_TRANSACTION", "substr(FIRST_TRANSACTION, instr(FIRST_TRANSACTION, '-') + 1, length(FIRST_TRANSACTION)) as posnr")
window = Window.partitionBy("product_line").orderBy("fkdat","vbeln","posnr").rowsBetween(Window.unboundedPreceding, Window.currentRow)
first_prd_sales_flg = ls_final_23.withColumn("rank", rank().over(window)).where("rank = 1").selectExpr("product_line","1 FIRST_PRD_SALES_FLAG","vbeln FIRST_TRANSACTION","posnr")

# COMMAND ----------

# first_cust_prd_sales_flg = ls_final_23.selectExpr("product_line","kunag","CONCAT(vbeln,'-',posnr) as first_transaction_cond").groupBy("product_line","kunag").agg(min("first_transaction_cond").alias('FIRST_TRANSACTION')).selectExpr("product_line","kunag", "1 FIRST_CUST_PRD_SALES_FLAG", "substr(FIRST_TRANSACTION, 1, (instr(FIRST_TRANSACTION, '-')) - 1) FIRST_TRANSACTION", "substr(FIRST_TRANSACTION, instr(FIRST_TRANSACTION, '-') + 1, length(FIRST_TRANSACTION)) as posnr")
window = Window.partitionBy("product_line","kunag").orderBy("fkdat","vbeln","posnr").rowsBetween(Window.unboundedPreceding, Window.currentRow)
first_cust_prd_sales_flg = ls_final_23.withColumn("rank", rank().over(window)).where("rank = 1").selectExpr("product_line","kunag","1 FIRST_CUST_PRD_SALES_FLAG","vbeln FIRST_TRANSACTION","posnr")

# COMMAND ----------

final_1 = first_cust_prd_sales_flg.alias("cust_prd_sales").join(first_prd_sales_flg.alias("prd_sales"),on = [first_cust_prd_sales_flg["FIRST_TRANSACTION"] == first_prd_sales_flg["FIRST_TRANSACTION"],first_cust_prd_sales_flg["posnr"] == first_prd_sales_flg["posnr"]],how = 'full').selectExpr("case when cust_prd_sales.FIRST_TRANSACTION is null then prd_sales.FIRST_TRANSACTION else cust_prd_sales.FIRST_TRANSACTION end as FIRST_TRANSACTION","prd_sales.FIRST_PRD_SALES_FLAG","cust_prd_sales.FIRST_CUST_PRD_SALES_FLAG", "case when cust_prd_sales.posnr is null then prd_sales.posnr else cust_prd_sales.posnr end as posnr")

# COMMAND ----------

final = final_1.alias("f_df").join(first_cust_flag.alias('cust_frst_trans'), on = [
                                                                  final_1["FIRST_TRANSACTION"] == first_cust_flag["FIRST_TRANSACTION"],
                                                                                    final_1["posnr"] == first_cust_flag["posnr"]], how = 'full').selectExpr("case when f_df.FIRST_TRANSACTION is null then cust_frst_trans.FIRST_TRANSACTION else f_df.FIRST_TRANSACTION end as FIRST_TRANSACTION","f_df.FIRST_PRD_SALES_FLAG","f_df.FIRST_CUST_PRD_SALES_FLAG", "cust_frst_trans.FIRST_CUST_FLAG", "case when f_df.posnr is null then cust_frst_trans.posnr else f_df.posnr end as posnr").fillna({'FIRST_CUST_FLAG': 0,'FIRST_PRD_SALES_FLAG': 0,'FIRST_CUST_PRD_SALES_FLAG': 0})

# COMMAND ----------

ls_final_24 = final.alias("final_df").join(ls_final_23.alias('frst_trans'), on = [
                                                                               col("final_df.FIRST_TRANSACTION") == col("frst_trans.vbeln"),
                                                                               col("final_df.posnr") == col("frst_trans.posnr")], how = 'right').select(
  [col("frst_trans." + cols) for cols in ls_final_23.columns] + 
  [col("final_df.FIRST_CUST_FLAG"),col("final_df.FIRST_CUST_PRD_SALES_FLAG"),col("final_df.FIRST_PRD_SALES_FLAG")]).fillna({'FIRST_CUST_FLAG': 0,'FIRST_PRD_SALES_FLAG': 0,'FIRST_CUST_PRD_SALES_FLAG': 0})

# COMMAND ----------

ls_final_25 = ls_final_24.withColumn("total_freight", when((col("vkorg") == '1000'), (col("zf00") - col("zsc_frgt"))).when(((col("vkorg") == '2000') | (col("vkorg") == '3000')), col("frgt_zf01") + col("frgt_zf02")).otherwise(0))

# COMMAND ----------

# ls_final_26 = ls_final_25.withColumn("ZSBU3_MT",when())
# display(current_date())

# COMMAND ----------

ls_final_26 = ls_final_25.withColumn("last_executed_time", lit(last_processed_time_str)).withColumn("status",when(((col("fksto").isNull()) | (col("fksto") == ' ') |(col("fksto") =='' )),"BILLED").otherwise("CANCELLED"))

# COMMAND ----------

# ls_final.write.parquet(processed_location+'ZSUM2', mode='overwrite')

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

ls_final_27 = ls_final_26.alias("fact").join(final_geo_df.alias("Geo_data"), on = [
                                                                  ls_final_26["vkorg"] == final_geo_df["SBU"],
                                                                                    ls_final_26["product_line"] == final_geo_df["product_line"],
  ls_final_26["SALES_ORG_TYPE"] == final_geo_df["SALE_TYPE"],
  ls_final_26["PSTLZ"] == final_geo_df["PINCODE"]
], how = 'left').select(
  [col("fact." + cols) for cols in ls_final_26.columns] + [col("DISTRICT"),col("SALES_GROUP"),col("STATE"),col("ZONE"),col("COUNTRY")])

# COMMAND ----------

# Get Geo Hierarchy for Shipto Pin code
final_geo_df_ship = final_geo_df.withColumnRenamed("PINCODE","PINCODE_SHIP").withColumnRenamed("DISTRICT","DISTRICT_SHIP").withColumnRenamed("SALES_GROUP","SALES_GROUP_SHIP").withColumnRenamed("STATE","STATE_SHIP").withColumnRenamed("ZONE","ZONE_SHIP").withColumnRenamed("COUNTRY","COUNTRY_SHIP")

ls_final = ls_final_27.alias("fact_ship").join(final_geo_df_ship.alias("Geo_data_ship"), on = [
                                                                  ls_final_27["vkorg"] == final_geo_df_ship["SBU"],
                                                                                    ls_final_27["product_line"] == final_geo_df_ship["product_line"],
  ls_final_27["SALES_ORG_TYPE"] == final_geo_df_ship["SALE_TYPE"],
  ls_final_27["shipto_pstlz"] == final_geo_df_ship["PINCODE_SHIP"]
], how = 'left').select(
  [col("fact_ship." + cols) for cols in ls_final_27.columns] + [col("DISTRICT_SHIP"),col("SALES_GROUP_SHIP"),col("STATE_SHIP"),col("ZONE_SHIP"),col("COUNTRY_SHIP")])

# COMMAND ----------

pmu = ProcessMetadataUtility()
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

#SURROGATE IMPLEMENTATION-----------------

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  sur_col_list = []
  join_condition=""
  join_condition_fts="\n left join {pro}.DIM_FIRST_TIME_SALES on".format(pro=processed_db_name) #for first_time_sales
  count=0
  
  ls_final.createOrReplaceTempView("{}".format(fact_name))
  for row_dim_fact_mapping in dim_fact_mapping:
    fact_table = row_dim_fact_mapping["fact_table"]
    dim_table = row_dim_fact_mapping["dim_table"]
    fact_surrogate = row_dim_fact_mapping["fact_surrogate"]
    dim_surrogate = row_dim_fact_mapping["dim_surrogate"]
    fact_column = row_dim_fact_mapping["fact_column"]
    dim_column = row_dim_fact_mapping["dim_column"]
    
    spark.sql("refresh table {processed_db_name}.{dim_table}".format(processed_db_name=processed_db_name,
                                                                   dim_table=dim_table))
    
    if((fact_table_name==fact_table or fact_table_name.lower()==fact_table) and (dim_table!="DIM_FIRST_TIME_SALES")):
      count += 1
      tmp_unmapped_str = "CASE WHEN "
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull(A{count}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table, count=count)
      tmp_unmapped_str += "{fact_name}.{fact_column},\n".format(fact_column=fact_column, fact_name=fact_name)
      sur_col_list.append("{fact_surrogate}".format(fact_surrogate=fact_surrogate))
      
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} A{count} on {fact_table}.{fact_column} = A{count}.{dim_column} ".format(
            dim_table=dim_table, count=count, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
  
    if((fact_table_name==fact_table or fact_table_name.lower()==fact_table) and dim_table=='DIM_FIRST_TIME_SALES'):
      count +=1
      fact_column_temp = fact_column.split("|")
      for rows in fact_column_temp:
        join_condition_fts += """\n {fact_name}.{rows} = {dim_table}.{rows} AND""".format(fact_name=fact_name,dim_table=dim_table,rows=rows)
      join_condition_fts = join_condition_fts[:-3]
      join_condition = join_condition + """ \n
  left join pro_prod.dim_territory B 
  ON
      ({fact_name}.SALES_ORG_TYPE = B.SALE_TYPE) AND
      (({fact_name}.PRODUCT_LINE = B.PRODUCT_LINE) OR ({fact_name}.PRODUCT_LINE is NULL AND B.PRODUCT_LINE is NULL)) AND
      (({fact_name}.PSTLZ = B.PINCODE) OR ({fact_name}.PSTLZ is NULL AND B.PINCODE is NULL)) AND
      (({fact_name}.vkorg = B.SALES_ORG) OR ({fact_name}.vkorg is NULL AND B.SALES_ORG is NULL)) AND
      (({fact_name}.DISTRICT = B.DISTRICT) OR ({fact_name}.DISTRICT is NULL AND B.DISTRICT is NULL)) AND
      (({fact_name}.SALES_GROUP = B.SALES_GROUP) OR ({fact_name}.SALES_GROUP is NULL AND B.SALES_GROUP is NULL)) AND
      (({fact_name}.STATE = B.STATE) OR ({fact_name}.STATE is NULL AND B.STATE is NULL)) AND
      (({fact_name}.ZONE = B.ZONE) OR ({fact_name}.ZONE is NULL AND B.ZONE is NULL)) AND
      (({fact_name}.COUNTRY = B.COUNTRY) OR ({fact_name}.COUNTRY is NULL AND B.COUNTRY is NULL))
  
  left join pro_prod.dim_territory C 
  ON
      ({fact_name}.SALES_ORG_TYPE = C.SALE_TYPE) AND
      (({fact_name}.PRODUCT_LINE = C.PRODUCT_LINE) OR ({fact_name}.PRODUCT_LINE is NULL AND C.PRODUCT_LINE is NULL)) AND
      (({fact_name}.shipto_pstlz = C.PINCODE) OR ({fact_name}.shipto_pstlz is NULL AND C.PINCODE is NULL)) AND
      (({fact_name}.vkorg = C.SALES_ORG) OR ({fact_name}.vkorg is NULL AND C.SALES_ORG is NULL)) AND
      (({fact_name}.DISTRICT_SHIP = C.DISTRICT) OR ({fact_name}.DISTRICT_SHIP is NULL AND C.DISTRICT is NULL)) AND
      (({fact_name}.SALES_GROUP_SHIP = C.SALES_GROUP) OR ({fact_name}.SALES_GROUP_SHIP is NULL AND C.SALES_GROUP is NULL)) AND
      (({fact_name}.STATE_SHIP = C.STATE) OR ({fact_name}.STATE_SHIP is NULL AND C.STATE is NULL)) AND
      (({fact_name}.ZONE_SHIP = C.ZONE) OR ({fact_name}.ZONE_SHIP is NULL AND C.ZONE is NULL)) AND
      (({fact_name}.COUNTRY_SHIP = C.COUNTRY) OR ({fact_name}.COUNTRY_SHIP is NULL AND C.COUNTRY is NULL))
     """.format(pro_db=processed_db_name,fact_name=fact_name)
  
  select_condition = select_condition[:-2]
  join_condition += join_condition_fts
  
  select_condition = select_condition + """
    ,dim_first_time_sales.FIRST_TIME_SALES_KEY AS FIRST_TIME_SALES_KEY
    """
  query = """select
  vbeln as INVOICE_ID,posnr as INVOICE_LINE_ID, fkimg as QUANTITY , QUANTITY_EA,lgort as STORAGE_LOC ,voleh as VOLUME_UNIT, brgew as GROSS_WEIGHT , cmpre as ITEM_CREDIT_PRICE , netwr as NET_VALUE , mwsbp as TAX_AMT ,gewei as WEIGHT_UNIT , waerk as DOC_CRCY, fkart as BILLING_TYPE , zzveh_no as VEHICLE_NO, zzlr_no as LR_NO ,zzdr_no as DRIVER_CONT_NO ,zzewaybill_no as WAYBILL_NO ,regio as REGION, volum as VOLUME, bupla as BUSINESS_PLACE, bukrs as COMPANY_CODE  ,exdat as EXCISE_DUTY_DATE , exnum as EXCISE_DUTY_NUM , bzirk as DISTRICT,
 order_type as ORDER_TYPE,
 cust_type as CUST_TYPE,
 pstlz as PSTLZ,
 zasval as ASSESSIBLE_VALUE2,
 zsc_frgt as SECDRY_FREIGHT,
 trade_dis as TRADE_DIS,
 jkfc_dis as KERALA_DISC,
 jkfc_rate as KERALA_DISC_RATE,
 p_jexp as EXCISE_CONDITIONS,
 p_zetx as ENTRY_TAX_PERC,
 p_jcst as CST_PERC,
 p_zvat as VAT_PERC,
 p_zgta as ADDL_TAX_PERC,
 p_ztcs as TCS_PERC,
 p_zeto as ORISSA_ENTRY_TAX_PERC,
 p_zssc as STATE_SEC_TAX_PERC,
 p_zb01 as BASE_PRICE_DISC,
 zrdf as RATE_DIFF,
 zifr as OCEAN_AIR_FREIGHT,
 zpak as PACKING_HIL_EXPORT,
 zfob as FOB_VALUE,
 zcmp as ZCMP,
 ztaxturn as TAXABLE_TURNOVER,
 cgst_rate as CGST_PERC,
 cgst_amnt as CGST_AMNT,
 sgst_rate as SGST_PERC,
 sgst_amnt as SGST_AMNT,
 igst_rate as IGST_PERC,
 igst_amnt as IGST_AMNT,
 ugst_rate as UGST_PERC,
 ugst_amnt as UGST_AMNT,
 zbasfin as BASE_PRICE_HIL,
 zbasfin1 as  BASE_PRICE,
 zf00 as FREIGHT_HIL,
 frgt_zf02 as FREIGHT_ZF02,
 frgt_zf01 as FREIGHT_ZF01,
 total_freight as TOTAL_FREIGHT,
 zmul as MULTIPLICATION_FACTOR,
 zins as INSURANCE,
 zmrp as MRP, 
 zd01 as PF_ADDL_DISC,
 zd02 as LOGISTICS_SUPPORT,
 zd03 as PF_CD_ON_BILLING,
 zd04 as ORC,
 zd05 as PF_PRICE_DIFF,
 zod1 as ZOD1,
 zod2 as ZOD2,
 zod4 as WP_ADDL_DISC,
 zod5 as WP_PRICE_DIFF,
 zod6 as WP_CD_ON_BILLING,
 zrpb as ZRPB,
 zrpc as ZRPC,
 zbpb_pu as ZBPB_PU,
 inco1 as INCO1,
 inco2 as INCO2,
 sales_org_type as SALES_ORG_TYPE,
 total_sales_value AS TOTAL_SALES_VALUE,
 TRANS_TYPE,
 CASE WHEN ({fact_name}.PSTLZ is NULL OR 
            {fact_name}.product_line is NULL or 
            {fact_name}.vkorg is NULL or
            {fact_name}.DISTRICT is NULL or
            {fact_name}.SALES_GROUP is NULL or
            {fact_name}.STATE is NULL or
            {fact_name}.ZONE is NULL or
            {fact_name}.COUNTRY is NULL or
            trim({fact_name}.product_line)='' or 
            trim({fact_name}.PSTLZ) = '' or
            trim({fact_name}.vkorg)='' or
            trim({fact_name}.DISTRICT)='' or
            trim({fact_name}.SALES_GROUP) = '' or
            trim({fact_name}.STATE) = '' or
            trim({fact_name}.ZONE) = '' or
            trim ({fact_name}.COUNTRY) = '') THEN -2 ELSE ifnull(B.GEO_KEY, -1) END as GEO_KEY,
  CASE WHEN ({fact_name}.shipto_pstlz is NULL OR 
            {fact_name}.product_line is NULL or 
            {fact_name}.vkorg is NULL or
            {fact_name}.DISTRICT_SHIP is NULL or
            {fact_name}.SALES_GROUP_SHIP is NULL or
            {fact_name}.STATE_SHIP is NULL or
            {fact_name}.ZONE_SHIP is NULL or
            {fact_name}.COUNTRY_SHIP is NULL or
            trim({fact_name}.product_line)='' or 
            trim({fact_name}.shipto_pstlz) = '' or
            trim({fact_name}.vkorg)='' or
            trim({fact_name}.DISTRICT_SHIP)='' or
            trim({fact_name}.SALES_GROUP_SHIP) = '' or
            trim({fact_name}.STATE_SHIP) = '' or
            trim({fact_name}.ZONE_SHIP) = '' or
            trim ({fact_name}.COUNTRY_SHIP) = '') THEN -2 ELSE ifnull(C.GEO_KEY, -1) END as SHIP_TO_GEO_KEY,
  {select_condition}, to_timestamp(last_executed_time, 'yyyy-MM-dd HH:mm:ss') as LAST_EXECUTED_TIME, status as  INVOICE_STATUS  from {fact_name}  {join_condition}
  """.format(
              join_condition=join_condition,fact_name=fact_name,select_condition=select_condition)

  print("\nFinal Query for {fact_table}\n (Total Surrogate Keys = {count}) :\n {query}".format(count=count, query=query,fact_table=fact_table))
  fact_final_view_surrogate = spark.sql(query)
  fact_final_view_surrogate_persist = fact_final_view_surrogate
  fact_final_view_surrogate_persist.createOrReplaceTempView("{fact_table}_busi_sur".format(fact_table=fact_table))
  
###   Where condition for unmapped records, selecting records where surrogate keys are -1
  sur_col_list = list(set(sur_col_list))
  str_where_condition = ""
  for item in sur_col_list:
    str_where_condition+= "{item} = -1 or ".format(item=item)
    
###   Select query for unmapped records
  str_where_condition = str_where_condition[:-3]
  unmapped_final_with_sur_df = spark.sql("select * from {fact_table}_busi_sur where {str_where_condition}".format(str_where_condition=str_where_condition,fact_table=fact_table))
  
  cols = []
  for item in fact_final_view_surrogate.columns:
    cols.append( "{} AS {}".format(item ,item.upper()))
  fact_final_view_surrogate = fact_final_view_surrogate.selectExpr(cols)
  
  return fact_final_view_surrogate, unmapped_final_with_sur_df

# COMMAND ----------

fact_sales_invoice, unmapped_final_df = surrogate_mapping_hil(csv_data, table_name, "ZSUM2", processed_db_name)
# d.count()

# COMMAND ----------

fact_sales_invoice.repartition("PRODUCT_LINE_KEY").createOrReplaceTempView("zsum2")
spark.catalog.refreshTable(processed_db_name + "." + table_name)
spark.sql("INSERT OVERWRITE TABLE {db} SELECT INVOICE_ID,INVOICE_LINE_ID,QUANTITY,QUANTITY_EA,STORAGE_LOC,VOLUME_UNIT,GROSS_WEIGHT,ITEM_CREDIT_PRICE,NET_VALUE,TAX_AMT,WEIGHT_UNIT,DOC_CRCY,BILLING_TYPE,VEHICLE_NO,LR_NO,DRIVER_CONT_NO,WAYBILL_NO,REGION,VOLUME,BUSINESS_PLACE,COMPANY_CODE,EXCISE_DUTY_DATE,EXCISE_DUTY_NUM,DISTRICT,ORDER_TYPE,CUST_TYPE,PSTLZ,INCO1,INCO2,ASSESSIBLE_VALUE2,SECDRY_FREIGHT,TRADE_DIS,KERALA_DISC,KERALA_DISC_RATE,EXCISE_CONDITIONS,ENTRY_TAX_PERC,CST_PERC,VAT_PERC,ADDL_TAX_PERC,TCS_PERC,ORISSA_ENTRY_TAX_PERC,STATE_SEC_TAX_PERC,BASE_PRICE_DISC,RATE_DIFF,OCEAN_AIR_FREIGHT,PACKING_HIL_EXPORT,FOB_VALUE,ZCMP,TAXABLE_TURNOVER,CGST_PERC,CGST_AMNT,SGST_PERC,SGST_AMNT,IGST_PERC,IGST_AMNT,UGST_PERC,UGST_AMNT,BASE_PRICE_HIL,BASE_PRICE,FREIGHT_HIL,FREIGHT_ZF02,FREIGHT_ZF01,TOTAL_FREIGHT,MULTIPLICATION_FACTOR,INSURANCE,MRP,PF_ADDL_DISC,LOGISTICS_SUPPORT,PF_CD_ON_BILLING,ORC,PF_PRICE_DIFF,ZOD1,ZOD2,WP_ADDL_DISC,WP_PRICE_DIFF,WP_CD_ON_BILLING,ZRPB,ZRPC,ZBPB_PU,SALES_ORG_TYPE,TOTAL_SALES_VALUE,TRANS_TYPE,GEO_KEY,SHIP_TO_GEO_KEY,DIVISION_KEY,PLANT_KEY,SALES_ORG_KEY,DIST_CHANNEL_KEY,SALES_TYPE_KEY,DATE_CREATED_KEY,PAYER_CUST_KEY,MATERIAL_KEY,PAYER_ADDR_KEY,SALES_PER_KEY,BILLING_DATE_KEY,SOLD_TO_PARTY_KEY,AGENT_ADDR_KEY,AGENT_CUST_KEY,BILLTO_ADDR_KEY,BILLTO_CUST_KEY,SOLDTO_ADDR_KEY,SHIPTO_ADDR_KEY,SHIPTO_CUST_KEY,PRODUCT_LINE_KEY,TRANSPORTER_KEY,FIRST_TIME_SALES_KEY,LAST_EXECUTED_TIME,INVOICE_STATUS FROM zsum2".format(db=processed_db_name + "." + table_name))

# COMMAND ----------

# fact_sales_invoice.write.insertInto(processed_db_name + "." + table_name, overwrite=True)

# COMMAND ----------

date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists zsum2
