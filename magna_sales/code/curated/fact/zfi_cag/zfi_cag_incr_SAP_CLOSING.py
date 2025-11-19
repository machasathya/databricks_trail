# Databricks notebook source
# DBTITLE 1,Imports
import datetime 
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, lit, expr, to_date, to_timestamp, sum, min, when, count, concat,upper
from pyspark.sql.types import StringType, DateType, IntegerType, DecimalType
from datetime import timedelta,datetime,date

# COMMAND ----------

last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)

start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)

sap_closing_month = (start_day_of_prev_month.strftime("%B")).upper()

sap_closing_year = start_day_of_prev_month.strftime("%Y")

start_day_of_prev_month_str = start_day_of_prev_month.strftime("%Y-%m-%d")

print(sap_closing_month,sap_closing_year)

# COMMAND ----------

meta_table = "fact_surrogate_meta"
sap_log_table = "sap_closing_run_meta"
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
table_name = "FACT_CUST_PAYMENT_OUTSTANDING"
last_processed_time = datetime.now()
last_processed_date_str = last_processed_time.strftime("%Y-%m-%d")
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# meta_table = "fact_surrogate_meta"
# sap_log_table = "sharepoint_run_meta"
# schema_name = "metadata_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# processed_db_name = "pro_prod"
# processed_location = "mnt/bi_datalake/prod/pro/"
# curated_db_name = "cur_prod"
# table_name = "FACT_CUST_PAYMENT_OUTSTANDING"
# last_processed_time = datetime.datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

pmu = ProcessMetadataUtility()

# COMMAND ----------

pmu = ProcessMetadataUtility()
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
sap_log_data = pmu.get_data(cursor=db_cursor,col_lookup = sap_log_table,value = table_name,column = 'TABLE_NAME',schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

sap_log_data = sap_log_data.where("YEAR = '{}' and MONTH = '{}'".format(sap_closing_year,sap_closing_month)).select("STATUS").collect()

# COMMAND ----------

if len(sap_log_data)==0:
  pass
elif len(sap_log_data[0][0])!=0 and sap_log_data[0][0] == 'COMPLETED':
  dbutils.notebook.exit(0)

# COMMAND ----------

var_date = last_day_of_prev_month.strftime("%Y-%m-%d")
p_budat = F.to_date(F.lit(var_date),'yyyy-MM-dd')
p_budat_str = datetime.strptime(var_date, "%Y-%m-%d").date()
month_str = p_budat_str.strftime("%B")
print(p_budat_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ### FORM GET DATA

# COMMAND ----------

spark.catalog.refreshTable(processed_db_name + ".DIM_SALE_TYPE")
spark.catalog.refreshTable(processed_db_name + ".DIM_PRODUCT_LINE")
spark.catalog.refreshTable(processed_db_name + ".DIM_SALES_ORG")
spark.catalog.refreshTable(processed_db_name + ".DIM_TERRITORY")
spark.catalog.refreshTable(processed_db_name + ".FACT_SALES_INVOICE")
spark.catalog.refreshTable(processed_db_name + ".DIM_CUSTOMER")

dim_sales_type = spark.read.table(processed_db_name + ".DIM_SALE_TYPE")
dim_product_line = spark.read.table(processed_db_name + ".DIM_PRODUCT_LINE")
dim_sales_org = spark.read.table(processed_db_name + ".DIM_SALES_ORG")
dim_customer = spark.read.table(processed_db_name + ".DIM_CUSTOMER")

# COMMAND ----------

# DBTITLE 1,Creation of lt_vbrp
# LT_VBAP

sales_invoice = spark.read.table(processed_db_name + ".FACT_SALES_INVOICE").selectExpr("INVOICE_ID as vbeln", "INVOICE_LINE_ID as posnr", "PSTLZ as pstlz", "PRODUCT_LINE_KEY", "SALES_TYPE_KEY", "(NET_VALUE + TAX_AMT) as total", "SALES_ORG_KEY")

sales_invoice_sale_type = sales_invoice.alias('inv').join(dim_sales_type.alias('sale'), on=[sales_invoice['SALES_TYPE_KEY'] == dim_sales_type['SALES_TYPE_KEY']], how='inner').select('inv.vbeln', 'inv.posnr', 'inv.pstlz', 'inv.PRODUCT_LINE_KEY', 'inv.total', 'sale.SALES_TYPE', "inv.SALES_ORG_KEY")

sales_invoice_sales_org = sales_invoice_sale_type.alias('sales').join(dim_sales_org.alias('org'), on=[sales_invoice_sale_type['SALES_ORG_KEY'] == dim_sales_org['SALES_ORG_KEY']], how='inner').select('sales.vbeln', 'sales.posnr', 'sales.pstlz', 'org.SALES_ORG', 'sales.PRODUCT_LINE_KEY', 'sales.total', 'sales.SALES_TYPE')

lt_vbrp_df = sales_invoice_sales_org.alias('final').join(dim_product_line.alias('prod'), on=[sales_invoice_sales_org['PRODUCT_LINE_KEY'] == dim_product_line['PRODUCT_LINE_KEY']], how='inner').select('final.vbeln', 'final.posnr', 'final.pstlz', 'prod.PRODUCT_LINE', 'final.total', 'final.SALES_TYPE', 'final.SALES_ORG')

lt_vbrp_df.createOrReplaceTempView("LT_VBRP")

# COMMAND ----------

# DBTITLE 1,Creation of lt_t001
# LT_T001

lt_t001_query = """
SELECT 
  BUKRS, WAERS 
FROM
  {db}.T001 
ORDER BY 
  BUKRS
"""

lt_t001_df = spark.sql(lt_t001_query.format(db=curated_db_name))
# lt_t001_df = lt_t001_df.drop_duplicates(subset=["bukrs"])
lt_t001_df.createOrReplaceTempView("LT_T001")


# COMMAND ----------

lt_knvv_default_sales_org_query = """
SELECT 
  KUNNR, min(VKORG) as DEFAULT_VKORG
FROM
  {db}.KNVV group by KUNNR
"""

lt_knvv_default_sales_org_df = spark.sql(lt_knvv_default_sales_org_query.format(db=curated_db_name))
# lt_t001_df = lt_t001_df.drop_duplicates(subset=["bukrs"])
lt_knvv_default_sales_org_df.createOrReplaceTempView("LT_KNVV_DEFAULT_SALES_ORG")


# COMMAND ----------

# DBTITLE 1,Creation of lt_bsid
# LT_BSID

lt_bsid_df = spark.sql("SELECT VBELN, BUKRS, KUNNR, UMSKS, UMSKZ, AUGDT, AUGBL, ZUONR, GJAHR, BELNR, BUZEI, BUDAT, BLDAT, WAERS, XBLNR, BLART, SHKZG, DMBTR, WRBTR, ZFBDT, ZTERM, ZBD1T, REBZG, REBZJ FROM {db}.BSID WHERE BUDAT <= '{p_budat}' AND (BSTAT NOT IN ('S') OR BSTAT is null)  ORDER BY BUKRS,KUNNR".format(p_budat=p_budat_str, db=curated_db_name))

# COMMAND ----------

# DBTITLE 1,Creation of lt_bsad
# LT_BSAD

# lt_bsad_df = spark.sql("SELECT VBELN, BUKRS, KUNNR, UMSKS, UMSKZ, AUGDT, AUGBL, ZUONR, GJAHR, BELNR, BUZEI, BUDAT, BLDAT, WAERS, XBLNR, BLART, SHKZG, DMBTR, WRBTR, ZFBDT, ZTERM, ZBD1T, REBZG, REBZJ FROM {db}.BSAD WHERE BUDAT <= '{p_budat}' AND AUGDT > '{p_budat}' AND (BSTAT NOT IN ('S') or BSTAT is null) ORDER BY BUKRS,KUNNR".format(p_budat=p_budat_str, db=curated_db_name))

# COMMAND ----------

# DBTITLE 1,Merging lt_bsad to lt_bsid
# lt_docs_df = lt_bsid_df.union(lt_bsad_df)
lt_docs_df = lt_bsid_df

# COMMAND ----------

# DBTITLE 1,Creation of lt_docs with transformation and calculations
# Transformation | Amount sign transformation | Credit vs Debit
lt_docs_df = lt_docs_df.withColumn("DMBTR",F.when(lt_docs_df.SHKZG=="H", -1 * lt_docs_df.DMBTR).otherwise(lt_docs_df.DMBTR))

# Transformation | unlnk calculation
lt_docs_df = lt_docs_df.withColumn("UNLNK", F.when(lt_docs_df.UMSKZ.isNull(), "X"))

# Calculation | deudt calculation
lt_docs_df = lt_docs_df.withColumn("DEUDT", F.expr("date_add(zfbdt, zbd1t)"))


lt_docs_temp_df_a = lt_docs_df

# Calculation | ovdeu calculation
# if blart = 'UE'
date_difference_budat = F.datediff(p_budat,lt_docs_temp_df_a.BUDAT)
# else
date_difference_bldat = F.datediff(p_budat,lt_docs_temp_df_a.BLDAT)
#Execution
lt_docs_temp_df_a = lt_docs_temp_df_a.withColumn("OVDEU", F.when(lt_docs_temp_df_a.BLART=='UE', date_difference_bldat).otherwise(date_difference_budat + 1))
lt_docs_temp_df_a = lt_docs_temp_df_a.withColumn("DATE_IND", F.lit("POST"))

# Calculation | lclcy calculation
lt_docs_temp_df_a = lt_docs_temp_df_a.withColumn("LCLCY", lt_docs_temp_df_a.WAERS)

# Standardizing BUZEI to 6 digits
lt_docs_temp_df_a = lt_docs_temp_df_a.withColumn("BUZEI", F.lpad(lt_docs_temp_df_a.BUZEI, 6, '0'))
lt_docs_temp_df_a.createOrReplaceTempView("LT_DOCS")

# COMMAND ----------

# DBTITLE 1,Creation of lt_docs_kunnr/lt_kna1
# Creation of LT_DOCS_KUNNR
lt_docs_kunnr_df_query = """
SELECT * FROM LT_DOCS
ORDER BY KUNNR
"""
lt_docs_kunnr_df = spark.sql(lt_docs_kunnr_df_query)
# lt_docs_kunnr_df = lt_docs_kunnr_df.drop_duplicates(subset=["KUNNR"])
lt_docs_kunnr_df.createOrReplaceTempView("LT_DOCS_KUNNR")
# LT_KNA1

lt_kna1_query = """
SELECT B.*, A.NAME1, A.SORTL, A.PSTLZ, A.REGIO
FROM {db}.KNA1 A
RIGHT JOIN LT_DOCS_KUNNR B
ON A.KUNNR = B.KUNNR
ORDER BY A.KUNNR
"""
lt_kna1_df = spark.sql(lt_kna1_query.format(db=curated_db_name))
#lt_kna1_df = lt_kna1_df.filter(" KUNNR like '%1109251%'")
lt_kna1_df.createOrReplaceTempView("LT_KNA1")

# COMMAND ----------

# DBTITLE 1,Creation of lt_t001w_df
lt_t001w_query = """
SELECT 
  KUNNR 
FROM
  {db}.T001W 
ORDER BY 
  KUNNR
"""

lt_t001w_df = spark.sql(lt_t001w_query.format(db=curated_db_name))

# COMMAND ----------

# DBTITLE 1,Creation of lt_vbpa_df
lt_vbpa_df =spark.sql("SELECT DISTINCT VBELN, PERNR FROM {db}.VBPA WHERE PARVW='VE' ORDER BY VBELN".format(db=curated_db_name))

# COMMAND ----------

dic_issue = spark.sql("SELECT lpad(SAP_CODE, 10, '0') as KUNNR, ISSUE_AMOUNT FROM {db}.DIC_ISSUE".format(db=curated_db_name))

# COMMAND ----------

legal_cases = spark.sql("SELECT lpad(CUSTOMER_CODE, 10, '0') as KUNNR, TOTAL FROM {db}.LEGAL_CASES".format(db=curated_db_name))

# COMMAND ----------

issue_amount = dic_issue.union(legal_cases).selectExpr("KUNNR", "case when ISSUE_AMOUNT is null then 0 else ISSUE_AMOUNT * -100000 end as AMOUNT")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FORM PROCESS DATA

# COMMAND ----------

# DBTITLE 1,Creation of lt_knvv
  lt_knvv_df_query = """
  SELECT * from LT_KNA1
  """
  lt_knvv_df = spark.sql(lt_knvv_df_query.format(db=curated_db_name))
  lt_knvv_df.createOrReplaceTempView("LT_KNVV")

# COMMAND ----------

  lt_hysil_df_query = """
  SELECT lt.*
  FROM LT_KNVV lt
  WHERE lt.KUNNR not in(
  SELECT lpad(CUSTOMER_CODE, 10,'0')
  FROM {db}.dim_hysil_customers)
  """
  lt_hysil_df = spark.sql(lt_hysil_df_query.format(db=processed_db_name))
  lt_hysil_df.createOrReplaceTempView("LT_HYSIL")

# COMMAND ----------

# DBTITLE 1,Creation of it_fifo_tab
# CASE for No Special G/L Indicator
wa_fifo_tab_gl_unchecked_df_query = """
SELECT VBELN, BUKRS, KUNNR, ZUONR, GJAHR, BELNR, BUZEI, BUDAT, BLDAT, WAERS, XBLNR, BLART, SHKZG, UMSKZ , DMBTR, WRBTR , ZFBDT, ZTERM, ZBD1T, REBZG, DEUDT as DUEDT, UNLNK, DATE_IND, OVDEU, LCLCY, REGIO, PSTLZ
FROM LT_HYSIL WHERE UMSKZ is null
"""

wa_fifo_tab_gl_unchecked_df = spark.sql(wa_fifo_tab_gl_unchecked_df_query)
wa_fifo_tab_gl_unchecked_df.withColumn("DATE_IND", F.lit("POST"))
wa_fifo_tab_gl_unchecked_df.createOrReplaceTempView("it_fifo_tab_gl_unchecked")


# CASE for Special G/L Indicator :: TO BE IMPLEMENTED

wa_fifo_tab_gl_checked_df_query = """
SELECT VBELN, BUKRS, KUNNR, ZUONR, GJAHR, BELNR, BUZEI, BUDAT, BLDAT, WAERS, XBLNR, BLART, SHKZG, UMSKZ, DMBTR, WRBTR, ZFBDT, ZTERM, ZBD1T, REBZG, DEUDT as DUEDT, UNLNK, DATE_IND, OVDEU,  LCLCY, REGIO, PSTLZ
FROM LT_HYSIL WHERE UMSKZ is not null
"""
wa_fifo_tab_gl_checked_df = spark.sql(wa_fifo_tab_gl_checked_df_query)
wa_fifo_tab_gl_checked_df.withColumn("DATE_IND", F.lit("POST"))
wa_fifo_tab_gl_checked_df.createOrReplaceTempView("it_fifo_tab_gl_checked")


# COMMAND ----------

# Grouping IT_FIFO_TAB
it_fifo_tab_gl_unchecked_df = spark.sql("""
SELECT * 
FROM it_fifo_tab_gl_unchecked
ORDER BY kunnr, zuonr, belnr, buzei, gjahr
""")

it_fifo_tab_gl_unchecked_df = it_fifo_tab_gl_unchecked_df.withColumn("zkey", F.concat(F.col("kunnr"), F.col("zuonr")))
fifo_joined_df_zkey = it_fifo_tab_gl_unchecked_df.withColumn("zkey1", F.concat(F.col("kunnr"), F.col("belnr"), F.col("gjahr")))

# Removing the KUNNR which is either a plant or depot
fifo_joined_df_plant = fifo_joined_df_zkey.alias('fifo').join(lt_t001w_df.alias('plant'), on=[fifo_joined_df_zkey["KUNNR"] == lt_t001w_df["KUNNR"]], how='left').select([F.col("fifo." + cols) for cols in fifo_joined_df_zkey.columns] + [F.col("plant.KUNNR").alias('kunnr_plant')]).where("kunnr_plant is null")

fifo_joined_inter = fifo_joined_df_plant.drop("kunnr_plant")

fifo_joined_df = fifo_joined_inter.alias('fifo').join(lt_vbpa_df.alias('vbpa'), on=[fifo_joined_inter["ZUONR"] == lt_vbpa_df["VBELN"]], how='left').select([F.col("fifo." + cols) for cols in fifo_joined_inter.columns] + [F.col("vbpa.PERNR").alias('PERNR')]).fillna({'PERNR': 'NA'})

# COMMAND ----------

it_final = fifo_joined_df.groupBy("kunnr", "belnr", "gjahr").agg(F.sum("dmbtr").alias("amount")).where("amount == 0").withColumn("zkey1", F.concat(F.col("kunnr"), F.col("belnr"), F.col("gjahr")))

# COMMAND ----------

it_data = fifo_joined_df.alias("fifo").join(it_final.alias("it"), on=[F.col('fifo.zkey1')==F.col('it.zkey1')], how='left').select([F.col("fifo." + cols) for cols in fifo_joined_df.columns] + [F.col("it.zkey1").alias("it_zkey1")]).where("it_zkey1 is null").drop("it_zkey1")
# it_data.count()
# display(it_data)

# COMMAND ----------

it_data_temp = it_data.where((F.col("BLART") != "JV") & (F.col("BLART") != "AB"))

item_details_temp = it_data_temp.alias("it_data").join(lt_vbrp_df.alias("vbrp"), on=[F.col('it_data.VBELN')==F.col('vbrp.vbeln')], how='left').select([F.col("it_data." + cols) for cols in it_data_temp.columns] + [F.col("vbrp.POSNR"), F.col("vbrp.product_line"), F.col("vbrp.sales_type"), F.col("total"), F.col("vbrp.SALES_ORG")])

item_details = it_data.where((F.col("BLART") == "JV") | (F.col("BLART") == "AB")).withColumn("POSNR", F.lit(None)).withColumn("product_line", F.lit(None)).withColumn("sales_type", F.lit(None)).withColumn("total", F.col("DMBTR")).withColumn("SALES_ORG", F.lit(None)).union(item_details_temp).withColumn("total", F.when(F.col("total").isNull(), F.col("DMBTR")).otherwise(F.col("total")))

item_details = item_details.alias("it_data").join(lt_knvv_default_sales_org_df, on=[item_details['KUNNR'] == lt_knvv_default_sales_org_df['KUNNR']]).select([F.col("it_data." + cols) for cols in item_details.columns] + [F.col("DEFAULT_VKORG")]).withColumn("SALES_ORG", F.when(F.col("SALES_ORG").isNull(), F.col("DEFAULT_VKORG")).otherwise(F.col("SALES_ORG")))
# item_details.count()
# display(item_details)

# COMMAND ----------

debit_list = item_details.where("dmbtr > 0").orderBy(it_data.OVDEU.desc())

credit_list = it_data.where("dmbtr < 0").groupBy("KUNNR").agg(F.sum("dmbtr").alias("amount")).union(issue_amount).groupBy("KUNNR").agg(F.sum("amount").alias("credit_sum"))

combined_list = debit_list.join(credit_list, on=['KUNNR'], how='left').select([F.col(cols) for cols in debit_list.columns] + [F.col("credit_sum")]).fillna({'credit_sum': '0'})

# COMMAND ----------

# Window logic to calculate the running total of the debit amount
w = Window.partitionBy("KUNNR").orderBy(F.desc("OVDEU"), "ZUONR", "POSNR").rowsBetween(
    Window.unboundedPreceding,  # Take all rows from the beginning of frame
    Window.currentRow           # To current row
)
sel_combined_list = combined_list.withColumn("total_due", F.sum("total").over(w)).orderBy("KUNNR", F.desc("OVDEU"), "ZUONR", "POSNR")

# COMMAND ----------

# Window logic to check the paid amount of the previous row. 
# This is to check if the previous row has consumed the entire credit amount
win = Window.partitionBy("KUNNR").orderBy(F.desc("OVDEU"), "ZUONR", "POSNR").rowsBetween(
    -1,          # Take one row from the current row
    -1           # Take one row from the current row
)
null_cond = F.isnull(F.lag(F.col("paid_amount"), 1).over(win)) # The value of lag will be null for the first row of the group 
cond = F.lag(F.col("paid_amount"), 1).over(win) <= 0
paid_flag = sel_combined_list.withColumn("paid_amount", F.col("total_due") + F.col("credit_sum")).withColumn("prev_flag", F.when(null_cond, -1).when(cond, 0).otherwise(1))

# COMMAND ----------

# Case when is used to check 3 conditions.
# 1. If the customer has not paid at all
# 2. If the balance amount is <= 0
# 3. If the balance amount is > 0
#    a.) The current record will have the pending amount
#    b.) The next record will have the same value as DMBTR, this is because the credit value is consumed by the earlier records
balance_amount = paid_flag.orderBy("KUNNR", F.desc("OVDEU"), "ZUONR", "POSNR").selectExpr("*", "case when(credit_sum == 0) then total when(paid_amount <= 0) then 0 when((paid_amount > 0) and (prev_flag <= 0)) then paid_amount else total end as balance")

# COMMAND ----------

def age_bucket(over_due):
  if 0 <= over_due <= 15:
    return "0_to_15_days"
  elif 15 < over_due <= 30:
    return "16_to_30_days"
  elif 30 < over_due <= 45:
    return "31_to_45_days"
  elif 45 < over_due <= 60:
    return "46_to_60_days"
  elif 60 < over_due <= 75:
    return "61_to_75_days"
  elif 75 < over_due <= 90:
    return "76_to_90_days"
  elif 90 < over_due <= 105:
    return "91_to_105_days"
  elif 105 < over_due <= 120:
    return "106_to_120_days"
  elif 120 < over_due <= 135:
    return "121_to_135_days"
  elif 135 < over_due <= 150:
    return "136_to_150_days"
  elif 150 < over_due <= 165:
    return "151_to_165_days"
  elif 165 < over_due <= 180:
    return "166_to_180_days"
  elif 180 < over_due <= 195:
    return "181_to_195_days"
  elif 195 < over_due <= 210:
    return "196_to_210_days"
  elif 210 < over_due <= 225:
    return "211_to_225_days"
  elif 225 < over_due <= 240:
    return "226_to_240_days"
  elif 240 < over_due <= 255:
    return "241_to_255_days"
  elif 255 < over_due <= 270:
    return "256_to_270_days"
  elif 270 < over_due <= 285:
    return "271_to_285_days"
  elif 285 < over_due <= 300:
    return "286_to_300_days"
  elif 300 < over_due <= 315:
    return "301_to_315_days"
  elif 315 < over_due <= 330:
    return "316_to_330_days"
  elif 330 < over_due <= 345:
    return "331_to_345_days"
  elif 345 < over_due <= 360:
    return "346_to_360_days"
  elif 360 < over_due <= 390:
    return "361_to_390_days"
  elif 390 < over_due <= 420:
    return "391_to_420_days"
  elif 420 < over_due <= 450:
    return "421_to_450_days"
  elif 450 < over_due <= 480:
    return "451_to_480_days"
  elif 480 < over_due <= 510:
    return "481_to_510_days"
  elif 510 < over_due <= 540:
    return "511_to_540_days"
  elif 540 < over_due <= 570:
    return "541_to_570_days"
  elif 570 < over_due <= 600:
    return "571_to_600_days"
  elif 600 < over_due <= 630:
    return "601_to_630_days"
  elif 630 < over_due <= 660:
    return "631_to_660_days"
  elif 660 < over_due <= 690:
    return "661_to_690_days"
  elif 690 < over_due <= 720:
    return "691_to_720_days"
  else:
    return "grtr_than_720_days"
age_bucket = F.udf(age_bucket, T.StringType())

# COMMAND ----------

buckets = balance_amount.withColumn("bucket", age_bucket(balance_amount['OVDEU'])).select("KUNNR", "PERNR", "PSTLZ", "sales_type", "product_line", "SALES_ORG", "balance", "bucket").filter('balance > 0').fillna({"product_line": "OTHERS", "sales_type": "OTHER"})

# COMMAND ----------

transpose_bucket = buckets.groupBy("KUNNR", "PERNR", "PSTLZ", "sales_type", "product_line", "SALES_ORG").pivot("bucket").sum("balance").na.fill(0).withColumn("snapshot_date", F.lit(var_date))
# .withColumn("month", F.lit(month_str))


transpose_bucket.createOrReplaceTempView("transpose_bucket")

# months = transpose_bucket.select("month").distinct().collect()

# COMMAND ----------

transpose_bucket=transpose_bucket.withColumn("sales_org_type",F.when(((F.col("product_line") == "PIPES & FITTINGS") & (F.col("product_line") != "OTHERS")),F.col("sales_type")).otherwise("<DEFAULT>")).withColumn("product_line",F.when(((F.col("product_line") == "OTHERS") & (F.col("SALES_ORG") == '1000')),"SBU1_OTHERS").when(((F.col("product_line") == "OTHERS") & (F.col("SALES_ORG") == '2000')),"SBU2_OTHERS").when(((F.col("product_line") == "OTHERS") & (F.col("SALES_ORG") == '3000')),"SBU3_OTHERS").otherwise(F.col("product_line"))).withColumn("last_executed_time", F.lit(last_processed_time_str)).withColumn("last_executed_date", F.to_date("last_executed_time"))

# COMMAND ----------

list_of_date_range_cols = ["0_to_15_days","16_to_30_days","31_to_45_days","46_to_60_days","61_to_75_days",
"76_to_90_days","91_to_105_days","106_to_120_days","121_to_135_days","136_to_150_days",
"151_to_165_days","166_to_180_days","181_to_195_days","196_to_210_days","211_to_225_days",
"226_to_240_days","241_to_255_days","256_to_270_days","271_to_285_days","286_to_300_days",
"301_to_315_days","316_to_330_days","331_to_345_days","346_to_360_days","361_to_390_days",
"391_to_420_days","421_to_450_days","451_to_480_days","481_to_510_days","511_to_540_days",
"541_to_570_days","571_to_600_days","601_to_630_days","631_to_660_days","661_to_690_days",
"691_to_720_days","grtr_than_720_days"]

df_cols = transpose_bucket.columns
for cols in list_of_date_range_cols:
  if cols not in df_cols:
    transpose_bucket = transpose_bucket.withColumn(cols, F.lit(0).cast("decimal(38,2)"))

# COMMAND ----------

# transpose_bucket.coalesce(2).write.partitionBy('month').parquet('mnt/datalake/r01/pro/CAG', mode='append')

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

transpose_bucket_final_df_1 = transpose_bucket.alias("fact").join(final_geo_df.alias("geo"), on = [
                                                                  transpose_bucket["sales_org"] == final_geo_df["SBU"],
                                                                                    transpose_bucket["product_line"] == final_geo_df["product_line"],
  transpose_bucket["SALES_ORG_TYPE"] == final_geo_df["SALE_TYPE"],
  transpose_bucket["PSTLZ"] == final_geo_df["PINCODE"]
], how = 'left').select(
  [col("fact." + cols) for cols in transpose_bucket.columns] + [col("DISTRICT"),col("SALES_GROUP"),col("STATE"),col("ZONE"),col("COUNTRY")])

# COMMAND ----------

#Geo mapping for finance 
transpose_bucket_final_df_2 = transpose_bucket_final_df_1.alias("fact").join(dim_customer.alias("dim_customer"),on=[transpose_bucket_final_df_1['KUNNR'] == dim_customer['cust_id']],how='inner').select(
  [col("fact." + cols) for cols in transpose_bucket_final_df_1.columns] + [col("dim_customer.SALES_TYPE").alias("CUSTOMER_SALES_TYPE"),col("dim_customer.OUTSTANDING_SBU").alias("CUSTOMER_SBU"),col("dim_customer.OUTSTANDING_PRODUCT_LINE").alias("CUSTOMER_PRODUCT_LINE"),col("dim_customer.POSTAL_CODE").alias("CUSTOMER_POSTAL_CODE")]).withColumn("CUSTOMER_SALES_ORG",when(col("CUSTOMER_SBU")=="SBU 1","1000").when(col("CUSTOMER_SBU")=="SBU 2","2000").when(col("CUSTOMER_SBU")=="SBU 3","3000").otherwise("1000"))

# COMMAND ----------

final_geo_df1 = final_geo_df.withColumn("PINCODE_ALIAS",col("PINCODE"))
transpose_bucket_final_df = transpose_bucket_final_df_2.alias("fact1").join(final_geo_df1.alias("geo"), on = [
                                                                                    transpose_bucket_final_df_2["CUSTOMER_SALES_ORG"] == final_geo_df1["SBU"],
                                                                                    transpose_bucket_final_df_2["CUSTOMER_PRODUCT_LINE"] == final_geo_df1["product_line"],
                                                                                    transpose_bucket_final_df_2["CUSTOMER_SALES_TYPE"] == final_geo_df1["SALE_TYPE"],
                                                                                    transpose_bucket_final_df_2["CUSTOMER_POSTAL_CODE"] == final_geo_df1["PINCODE_ALIAS"] ], how ='left').select(
  [col("fact1." + cols) for cols in transpose_bucket_final_df_2.columns] + [col("geo.DISTRICT").alias("CUST_DISTRICT"),col("geo.SALES_GROUP").alias("CUST_SALES_GROUP"),col("geo.STATE").alias("CUST_STATE"),col("geo.ZONE").alias("CUST_ZONE"),col("geo.COUNTRY").alias("CUST_COUNTRY")])

# COMMAND ----------

pmu = ProcessMetadataUtility()
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# transpose_bucket_final_df.write.parquet("mnt/bi_datalake/dev/pro/"+'CUST_PAYMENT_OUTSTANDING', mode='overwrite')

# COMMAND ----------

#SURROGATE KEY IMPLEMENTATION FOR FACT_SALES_TARGETS - INCLUDING LOGIC FOR GEO-KEY--

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  sur_col_list = []
  join_condition=""
  count=0
  
  transpose_bucket_final_df.createOrReplaceTempView("{}".format(fact_name))
  
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
      tmp_unmapped_str += "A.{fact_column},\n".format(fact_column=fact_column, fact_name=fact_name)
      sur_col_list.append("{fact_surrogate}".format(fact_surrogate=fact_surrogate))
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
          
      join_condition += "\n left join {pro_db}.{dim_table} on A.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
  join_condition = join_condition + """ \n
  left join {pro_db}.dim_territory B
  ON 
     A.sales_org_type = B.SALE_TYPE AND
     ((A.PRODUCT_LINE = B.PRODUCT_LINE) OR (A.PRODUCT_LINE is NULL AND B.PRODUCT_LINE is NULL)) AND
     ((A.PSTLZ = B.PINCODE) OR (A.PSTLZ is NULL AND B.PINCODE is NULL)) AND
     ((A.SALES_ORG = B.SALES_ORG) OR (A.SALES_ORG is NULL AND B.SALES_ORG is NULL)) AND
     ((A.DISTRICT = B.DISTRICT) OR (A.DISTRICT is NULL AND B.DISTRICT is NULL)) AND
     ((A.SALES_GROUP = B.SALES_GROUP) OR (A.SALES_GROUP is NULL AND B.SALES_GROUP is NULL)) AND
     ((A.STATE = B.STATE) OR (A.STATE is NULL AND B.STATE is NULL)) AND
     ((A.ZONE = B.ZONE) OR (A.ZONE is NULL AND B.ZONE is NULL)) AND
     ((A.COUNTRY = B.COUNTRY) OR (A.COUNTRY is NULL AND B.COUNTRY is NULL))
     """.format(pro_db=processed_db_name)
  
  join_condition = join_condition + """ \n
  left join {pro_db}.dim_territory C
  ON 
     A.CUSTOMER_SALES_TYPE = C.SALE_TYPE AND
     ((A.CUSTOMER_PRODUCT_LINE = C.PRODUCT_LINE) OR (A.CUSTOMER_PRODUCT_LINE is NULL AND C.PRODUCT_LINE is NULL)) AND
     ((A.CUSTOMER_POSTAL_CODE = C.PINCODE) OR (A.CUSTOMER_POSTAL_CODE is NULL AND C.PINCODE is NULL)) AND
     ((A.CUSTOMER_SALES_ORG = C.SALES_ORG) OR (A.CUSTOMER_SALES_ORG is NULL AND C.SALES_ORG is NULL)) AND
     ((A.CUST_DISTRICT = C.DISTRICT) OR (A.CUST_DISTRICT is NULL AND C.DISTRICT is NULL)) AND
     ((A.CUST_SALES_GROUP = C.SALES_GROUP) OR (A.CUST_SALES_GROUP is NULL AND C.SALES_GROUP is NULL)) AND
     ((A.CUST_STATE = C.STATE) OR (A.CUST_STATE is NULL AND C.STATE is NULL)) AND
     ((A.CUST_ZONE = C.ZONE) OR (A.CUST_ZONE is NULL AND C.ZONE is NULL)) AND
     ((A.CUST_COUNTRY = C.COUNTRY) OR (A.CUST_COUNTRY is NULL AND C.COUNTRY is NULL))
     """.format(pro_db=processed_db_name)
  
  select_condition = select_condition[:-2]
  query = """select 
A.0_to_15_days,
A.106_to_120_days,
A.121_to_135_days,
A.136_to_150_days,
A.151_to_165_days,
A.166_to_180_days,
A.16_to_30_days,
A.181_to_195_days,
A.196_to_210_days,
A.211_to_225_days,
A.226_to_240_days,
A.241_to_255_days,
A.256_to_270_days,
A.271_to_285_days,
A.286_to_300_days,
A.301_to_315_days,
A.316_to_330_days,
A.31_to_45_days,
A.331_to_345_days,
A.346_to_360_days,
A.361_to_390_days,
A.391_to_420_days,
A.421_to_450_days,
A.451_to_480_days,
A.46_to_60_days,
A.481_to_510_days,
A.511_to_540_days,
A.541_to_570_days,
A.571_to_600_days,
A.601_to_630_days,
A.61_to_75_days,
A.631_to_660_days,
A.661_to_690_days,
A.691_to_720_days,
A.76_to_90_days,
A.91_to_105_days,
A.grtr_than_720_days,
  CASE WHEN (A.PSTLZ is NULL OR 
            A.product_line is NULL or 
            A.SALES_ORG is NULL or
            A.DISTRICT is NULL or
            A.SALES_GROUP is NULL or
            A.STATE is NULL or
            A.ZONE is NULL or
            A.COUNTRY is NULL or
            trim(A.product_line)='' or 
            trim(A.PSTLZ) = '' or
            trim(A.SALES_ORG)='' or
            trim(A.DISTRICT)='' or
            trim(A.SALES_GROUP) = '' or
            trim(A.STATE) = '' or
            trim(A.ZONE) = '' or
            trim (A.COUNTRY) = ''
            ) THEN -2 ELSE ifnull(B.GEO_KEY, -1) END as GEO_KEY,
            
            CASE WHEN (A.CUSTOMER_POSTAL_CODE is NULL OR 
            A.CUSTOMER_PRODUCT_LINE is NULL or 
            A.CUSTOMER_SALES_ORG is NULL or
            A.CUST_DISTRICT is NULL or
            A.CUST_SALES_GROUP is NULL or
            A.CUST_STATE is NULL or
            A.CUST_ZONE is NULL or
            A.CUST_COUNTRY is NULL or
            trim(A.CUSTOMER_PRODUCT_LINE)='' or 
            trim(A.CUSTOMER_POSTAL_CODE) = '' or
            trim(A.CUSTOMER_SALES_ORG)='' or
            trim(A.CUST_DISTRICT)='' or
            trim(A.CUST_SALES_GROUP) = '' or
            trim(A.CUST_STATE) = '' or
            trim(A.CUST_ZONE) = '' or
            trim (A.CUST_COUNTRY) = ''
            ) THEN -2 ELSE ifnull(C.GEO_KEY, -1) END as CUST_GEO_KEY
  
  
  ,{select_condition},to_timestamp(last_executed_time, 'yyyy-MM-dd HH:mm:ss') as LAST_EXECUTED_TIME, A.LAST_EXECUTED_DATE  from {fact_name} A {join_condition}""".format(
               join_condition=join_condition,fact_name=fact_name,select_condition=select_condition)
  
  print("\nFinal Query: " +query) 
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
#   unmapped_final_with_sur_df = spark.sql("select * from {fact_table}_busi_sur where {str_where_condition}".format(str_where_condition=str_where_condition,fact_table=fact_table))
  
  cols = []
  
  for item in fact_final_view_surrogate.columns:
    cols.append( "{} AS {}".format(item ,item.upper()))
  fact_final_view_surrogate = fact_final_view_surrogate.selectExpr(cols)
  #print(cols)
  return fact_final_view_surrogate, str_where_condition

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS pro_data;
# MAGIC DROP TABLE IF EXISTS data_cag;

# COMMAND ----------

data_cag, unmapped_final_df_query = surrogate_mapping_hil(csv_data, table_name, "CAG", processed_db_name)
data_cag.write.saveAsTable("data_cag")
# data_cag.write.insertInto("data_cag")

# COMMAND ----------

display(data_cag.select("snapshot_date_key").distinct())

# COMMAND ----------

unmapped_final_df = spark.sql("select * from data_cag where {str_where_condition}".format(str_where_condition=unmapped_final_df_query))

# COMMAND ----------

unmapped_final_df.write.mode('overwrite').parquet(processed_location + table_name + "_UNMAPPED")

# COMMAND ----------

data_cag = spark.sql("select A.0_to_15_days,A.106_to_120_days,A.121_to_135_days,A.136_to_150_days,A.151_to_165_days,A.166_to_180_days,A.16_to_30_days,A.181_to_195_days,A.196_to_210_days,A.211_to_225_days,A.226_to_240_days,A.241_to_255_days,A.256_to_270_days,A.271_to_285_days,A.286_to_300_days,A.301_to_315_days,A.316_to_330_days,A.31_to_45_days,A.331_to_345_days,A.346_to_360_days,A.361_to_390_days,A.391_to_420_days,A.421_to_450_days,A.451_to_480_days,A.46_to_60_days,A.481_to_510_days,A.511_to_540_days,A.541_to_570_days,A.571_to_600_days,A.601_to_630_days,A.61_to_75_days,A.631_to_660_days,A.661_to_690_days,A.691_to_720_days,A.76_to_90_days,A.91_to_105_days,A.grtr_than_720_days,A.GEO_KEY,A.PER_KEY,A.CUSTOMER_KEY,A.SNAPSHOT_DATE_KEY,A.SALES_TYPE_KEY,A.PRODUCT_LINE_KEY,A.SALES_ORG_KEY,A.LAST_EXECUTED_TIME,A.CUST_GEO_KEY, A.LAST_EXECUTED_DATE from data_cag A")

# COMMAND ----------

data_cag.repartition("product_line_key").write.insertInto(processed_db_name + "." + table_name, overwrite=True)

# COMMAND ----------

date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'INCREMENTAL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

date_now = datetime.utcnow()
vals = "('" +  table_name + "','" + sap_closing_year +  "','" +  sap_closing_month + "','" + "COMPLETED','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "SAP_CLOSING_RUN_META", "(TABLE_NAME, YEAR, MONTH, STATUS, RUN_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS pro_data;
# MAGIC DROP TABLE IF EXISTS data_cag;

# COMMAND ----------

dbutils.notebook.exit(1)
