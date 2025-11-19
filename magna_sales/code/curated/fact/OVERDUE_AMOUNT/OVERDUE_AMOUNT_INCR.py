# Databricks notebook source
# DBTITLE 1,Imports
# import statements
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

# defining current date
ist_zone = datetime.now() + timedelta(hours=5.5)
var_date = (ist_zone - timedelta(days=1)).strftime("%Y-%m-%d")
p_budat = F.to_date(F.lit(var_date),'yyyy-MM-dd')
p_budat_str = datetime.strptime(var_date, "%Y-%m-%d").date()
month_str = p_budat_str.strftime("%B")
print(p_budat_str)

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
table_name = "FACT_OVERDUE_AMOUNT"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

#variables to run the code manually
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
# table_name = "FACT_OVERDUE_AMOUNT"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# refreshing and reading the data from processed layer table
spark.catalog.refreshTable(processed_db_name + ".DIM_SALE_TYPE")
spark.catalog.refreshTable(processed_db_name + ".DIM_PRODUCT_LINE")
spark.catalog.refreshTable(processed_db_name + ".DIM_SALES_ORG")
spark.catalog.refreshTable(processed_db_name + ".FACT_SALES_INVOICE")

dim_sales_type = spark.read.table(processed_db_name + ".DIM_SALE_TYPE")
dim_product_line = spark.read.table(processed_db_name + ".DIM_PRODUCT_LINE")
dim_sales_org = spark.read.table(processed_db_name + ".DIM_SALES_ORG")
dim_customer = spark.read.table(processed_db_name + ".DIM_CUSTOMER")

# COMMAND ----------

# DBTITLE 1,Creation of lt_vbrp
# reading sales_invoice data from processed layer table
sales_invoice = spark.read.table(processed_db_name + ".FACT_SALES_INVOICE").selectExpr("INVOICE_ID as vbeln", "INVOICE_LINE_ID as posnr", "PSTLZ as pstlz", "PRODUCT_LINE_KEY", "SALES_TYPE_KEY", "(NET_VALUE + TAX_AMT) as total", "SALES_ORG_KEY")

# extracting sales_type
sales_invoice_sale_type = sales_invoice.alias('inv').join(dim_sales_type.alias('sale'), on=[sales_invoice['SALES_TYPE_KEY'] == dim_sales_type['SALES_TYPE_KEY']], how='inner').select('inv.vbeln', 'inv.posnr', 'inv.pstlz', 'inv.PRODUCT_LINE_KEY', 'inv.total', 'sale.SALES_TYPE', "inv.SALES_ORG_KEY")

# extracting sales org
sales_invoice_sales_org = sales_invoice_sale_type.alias('sales').join(dim_sales_org.alias('org'), on=[sales_invoice_sale_type['SALES_ORG_KEY'] == dim_sales_org['SALES_ORG_KEY']], how='inner').select('sales.vbeln', 'sales.posnr', 'sales.pstlz', 'org.SALES_ORG', 'sales.PRODUCT_LINE_KEY', 'sales.total', 'sales.SALES_TYPE')

# extracting product line
lt_vbrp_df = sales_invoice_sales_org.alias('final').join(dim_product_line.alias('prod'), on=[sales_invoice_sales_org['PRODUCT_LINE_KEY'] == dim_product_line['PRODUCT_LINE_KEY']], how='inner').select('final.vbeln', 'final.posnr', 'final.pstlz', 'prod.PRODUCT_LINE', 'final.total', 'final.SALES_TYPE', 'final.SALES_ORG')

lt_vbrp_df.createOrReplaceTempView("LT_VBRP")

# COMMAND ----------

# DBTITLE 1,Creation of lt_t001
# reading data from curated layer
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

# reading data from curated layer
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
# reading data from curated layer
lt_bsid_df = spark.sql("SELECT VBELN, BUKRS, KUNNR, UMSKS, UMSKZ, AUGDT, AUGBL, ZUONR, GJAHR, BELNR, BUZEI, BUDAT, BLDAT, WAERS, XBLNR, BLART, SHKZG, DMBTR, WRBTR, ZFBDT, ZTERM, ZBD1T, REBZG, REBZJ FROM {db}.BSID WHERE BUDAT <= '{p_budat}' AND (BSTAT NOT IN ('S') OR BSTAT is null) ORDER BY BUKRS,KUNNR".format(p_budat=p_budat_str, db=curated_db_name))

# COMMAND ----------

# DBTITLE 1,Creation of lt_bsad
# reading data from curated layer
lt_bsad_df = spark.sql("SELECT VBELN, BUKRS, KUNNR, UMSKS, UMSKZ, AUGDT, AUGBL, ZUONR, GJAHR, BELNR, BUZEI, BUDAT, BLDAT, WAERS, XBLNR, BLART, SHKZG, DMBTR, WRBTR, ZFBDT, ZTERM, ZBD1T, REBZG, REBZJ FROM {db}.BSAD WHERE BUDAT <= '{p_budat}' AND AUGDT > '{p_budat}' AND (BSTAT NOT IN ('S') or BSTAT is null) ORDER BY BUKRS,KUNNR".format(p_budat=p_budat_str, db=curated_db_name))

# COMMAND ----------

# DBTITLE 1,Merging lt_bsad to lt_bsid
lt_docs_df = lt_bsid_df.union(lt_bsad_df)

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

# reading disputes and legal cases data from curated layer
dic_issue = spark.sql("SELECT lpad(SAP_CODE, 10, '0') as KUNNR, ISSUE_AMOUNT FROM {db}.DIC_ISSUE".format(db=curated_db_name))
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

  # reading hysil data
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

# COMMAND ----------

it_data_temp = it_data.where((F.col("BLART") != "JV") & (F.col("BLART") != "AB"))

item_details_temp = it_data_temp.alias("it_data").join(lt_vbrp_df.alias("vbrp"), on=[F.col('it_data.VBELN')==F.col('vbrp.vbeln')], how='left').select([F.col("it_data." + cols) for cols in it_data_temp.columns] + [F.col("vbrp.POSNR"), F.col("vbrp.product_line"), F.col("vbrp.sales_type"), F.col("total"), F.col("vbrp.SALES_ORG")])

item_details = it_data.where((F.col("BLART") == "JV") | (F.col("BLART") == "AB")).withColumn("POSNR", F.lit(None)).withColumn("product_line", F.lit(None)).withColumn("sales_type", F.lit(None)).withColumn("total", F.col("DMBTR")).withColumn("SALES_ORG", F.lit(None)).union(item_details_temp).withColumn("total", F.when(F.col("total").isNull(), F.col("DMBTR")).otherwise(F.col("total")))

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

#New Logic for Finance to add division id
spark.catalog.refreshTable(processed_db_name + ".DIM_DIVISION")
division = spark.read.table(processed_db_name + ".DIM_DIVISION")
div_knvv_df_query = """select KUNNR,min(SPART) as division_id from {db}.KNVV Group by KUNNR"""
div_knvv_df = spark.sql(div_knvv_df_query.format(db=curated_db_name))
div_knvv_df.createOrReplaceTempView("DIV_KNVV")

# COMMAND ----------

# calculating balance amount
balance_amount_n = balance_amount.alias("fact").join(dim_customer.alias("dim_customer"),on=[balance_amount['KUNNR'] == dim_customer['cust_id']],how='inner').select(
  [col("fact." + cols) for cols in balance_amount.columns] + [col("dim_customer.SALES_TYPE").alias("CUSTOMER_SALES_TYPE"),col("dim_customer.DIVISION").alias("CUSTOMER_DIVISION"),col("dim_customer.OUTSTANDING_SBU").alias("CUSTOMER_SBU"),col("dim_customer.OUTSTANDING_PRODUCT_LINE").alias("CUSTOMER_PRODUCT_LINE"),col("dim_customer.POSTAL_CODE").alias("CUSTOMER_POSTAL_CODE")]).withColumn("CUSTOMER_SALES_ORG",when(col("CUSTOMER_SBU")=="SBU 1","1000").when(col("CUSTOMER_SBU")=="SBU 2","2000").when(col("CUSTOMER_SBU")=="SBU 3","3000").when(col("CUSTOMER_SBU")=="SBU 4","4000").when(col("CUSTOMER_SBU")=="SBU 5", "5000").when(col("CUSTOMER_SBU")=="SBU 6", "6000").otherwise("1000")).withColumn("CUSTOMER_GEO_SALES_ORG",when(col("CUSTOMER_SBU")=="SBU 1","1000").when(col("CUSTOMER_SBU")=="SBU 2","2000").when(col("CUSTOMER_SBU")=="SBU 3","3000").otherwise("1000"))

# COMMAND ----------

# refreshing and reading outstanding and collections data from curated layer
spark.catalog.refreshTable(curated_db_name + ".OUTSTANDING_AND_COLLECTIONS")
overdues_m = spark.read.table(curated_db_name + ".OUTSTANDING_AND_COLLECTIONS")
overdues_m = overdues_m.drop('LAST_UPDATED_DT_TS')

# COMMAND ----------

#New Logic for Finance
#DIVISON ID
# OUTER JOIN balance_amount with overdue critera manual dataset on (VKORG, PRODUCT_LINE,SALE_TYPE )
# add a new column overdue_amount =  over_due ( if overdue_period_start_in_days > OVDUE from balance_amount table )
spark.catalog.refreshTable(curated_db_name + ".OUTSTANDING_AND_COLLECTIONS")
overdues_m = spark.read.table(curated_db_name + ".OUTSTANDING_AND_COLLECTIONS")
overdues_m = overdues_m.drop('LAST_UPDATED_DT_TS')
overdues_m= overdues_m.withColumn("SBU_N",when(col("SBU")=="SBU 1","1000").when(col("SBU")=="SBU 2","2000").when(col("SBU")=="SBU 3","3000").when(col("SBU")=="SBU 4","4000").when(col("SBU")=="SBU 5","5000").when(col("SBU")=="SBU 6","6000").otherwise(col("SBU")))

overdues_m = overdues_m.withColumn("sales_type_new",when(col("SALE_TYPE").isNull(),"<DEFAULT>").otherwise(col("SALE_TYPE")))

balance_amount_new = balance_amount_n.withColumn("sales_type_new",when(col("CUSTOMER_SALES_TYPE").isNull(),"<DEFAULT>").otherwise(col("CUSTOMER_SALES_TYPE")))

balance_amount_1 = balance_amount_new.alias("oc").join(overdues_m, on = [balance_amount_new["CUSTOMER_PRODUCT_LINE"] == overdues_m["PRODUCT_LINE_FOR_OUTSTANDING"],
                                                                         balance_amount_new["sales_type_new"] == overdues_m["sales_type_new"],
                                                                         balance_amount_new["CUSTOMER_SALES_ORG"] == overdues_m["SBU_N"],
                                                                        balance_amount_new["CUSTOMER_DIVISION"] == overdues_m["DIVISION"]],
                                   how='left').select([col("oc." + cols) for cols in balance_amount_new.columns]+ [col("OVERDUE_PERIOD_START_IN_DAYS")]).fillna({'OVERDUE_PERIOD_START_IN_DAYS': '0'})

balance_amount_2 =balance_amount_1.withColumn("overdue_amount",when(col("OVDEU") >= col("OVERDUE_PERIOD_START_IN_DAYS"),col("balance")).otherwise("0"))
balance_amount_3 = balance_amount_2.withColumn("overdue_amount", balance_amount_2['overdue_amount'].cast(DecimalType(36,2)))

# COMMAND ----------

# aggregaring data upon customer
transpose_bucket = balance_amount_3.groupBy("KUNNR", "CUSTOMER_POSTAL_CODE", "CUSTOMER_SALES_TYPE", "CUSTOMER_PRODUCT_LINE", "CUSTOMER_SALES_ORG", "CUSTOMER_DIVISION","CUSTOMER_GEO_SALES_ORG").sum("overdue_amount").na.fill(0).withColumn("snapshot_date", F.lit(var_date)).withColumnRenamed("sum(overdue_amount)", "overdue_amount")

# COMMAND ----------

# adding last executed time column
transpose_bucket = transpose_bucket.withColumn("last_executed_time", F.lit(last_processed_time_str)).withColumn("last_executed_date", F.to_date("last_executed_time"))

# COMMAND ----------

# generating dim_territory from reading territory data from curated layer
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

# adding all required columns to generate geo keys
transpose_bucket_final_df1 = transpose_bucket.alias("fact").join(final_geo_df.alias("Geo_data"), on = [
                                                                  transpose_bucket["CUSTOMER_GEO_SALES_ORG"] == final_geo_df["SBU"],
                                                                                    transpose_bucket["CUSTOMER_PRODUCT_LINE"] == final_geo_df["product_line"],
  transpose_bucket["CUSTOMER_SALES_TYPE"] == final_geo_df["SALE_TYPE"],
  transpose_bucket["CUSTOMER_POSTAL_CODE"] == final_geo_df["PINCODE"]
], how = 'left').select(
  [col("fact." + cols) for cols in transpose_bucket.columns] + [col("DISTRICT"),col("SALES_GROUP"),col("STATE"),col("ZONE"),col("COUNTRY")])
transpose_bucket_final_df = transpose_bucket_final_df1.withColumn("PERNR",lit('')).withColumnRenamed("CUSTOMER_SALES_TYPE","sales_type").withColumnRenamed("CUSTOMER_PRODUCT_LINE","product_line").withColumnRenamed("CUSTOMER_SALES_ORG","sales_org").withColumnRenamed("CUSTOMER_DIVISION","division_id")

# COMMAND ----------

# reading surrogate metadata from SQL
pmu = ProcessMetadataUtility()
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

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
     A.SALES_TYPE = B.SALE_TYPE AND
     ((A.PRODUCT_LINE = B.PRODUCT_LINE) OR (A.PRODUCT_LINE is NULL AND B.PRODUCT_LINE is NULL)) AND
     ((A.CUSTOMER_POSTAL_CODE = B.PINCODE) OR (A.CUSTOMER_POSTAL_CODE is NULL AND B.PINCODE is NULL)) AND
     ((A.CUSTOMER_GEO_SALES_ORG = B.SALES_ORG) OR (A.CUSTOMER_GEO_SALES_ORG is NULL AND B.SALES_ORG is NULL)) AND
     ((A.DISTRICT = B.DISTRICT) OR (A.DISTRICT is NULL AND B.DISTRICT is NULL)) AND
     ((A.SALES_GROUP = B.SALES_GROUP) OR (A.SALES_GROUP is NULL AND B.SALES_GROUP is NULL)) AND
     ((A.STATE = B.STATE) OR (A.STATE is NULL AND B.STATE is NULL)) AND
     ((A.ZONE = B.ZONE) OR (A.ZONE is NULL AND B.ZONE is NULL)) AND
     ((A.COUNTRY = B.COUNTRY) OR (A.COUNTRY is NULL AND B.COUNTRY is NULL))
     """.format(pro_db=processed_db_name)
  
  select_condition = select_condition[:-2]
  query = """select 
A.overdue_amount,
  CASE WHEN (A.CUSTOMER_POSTAL_CODE is NULL OR 
            A.PRODUCT_LINE is NULL or 
            A.CUSTOMER_GEO_SALES_ORG is NULL or
            A.DISTRICT is NULL or
            A.SALES_GROUP is NULL or
            A.STATE is NULL or
            A.ZONE is NULL or
            A.COUNTRY is NULL or
            trim(A.PRODUCT_LINE)='' or 
            trim(A.CUSTOMER_POSTAL_CODE) = '' or
            trim(A.CUSTOMER_GEO_SALES_ORG)='' or
            trim(A.DISTRICT)='' or
            trim(A.SALES_GROUP) = '' or
            trim(A.STATE) = '' or
            trim(A.ZONE) = '' or
            trim (A.COUNTRY) = '') THEN -2 ELSE ifnull(B.GEO_KEY, -1) END as GEO_KEY
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
# MAGIC
# MAGIC DROP TABLE IF EXISTS pro_data_overdue;
# MAGIC DROP TABLE IF EXISTS data_overdue;

# COMMAND ----------

# generating surrogate keys
data_overdue, unmapped_final_df_query = surrogate_mapping_hil(csv_data, table_name, "OVERDUE", processed_db_name)
data_overdue.createOrReplaceTempView("data_overdue")

# COMMAND ----------

print(unmapped_final_df_query)

# COMMAND ----------

# extracting unmapped dataframe
unmapped_final_df = spark.sql("select * from data_overdue where {str_where_condition}".format(str_where_condition=unmapped_final_df_query))

# COMMAND ----------

# writing unmapped data to processed layer 
unmapped_final_df.write.mode('overwrite').parquet(processed_location + table_name + "_UNMAPPED")

# COMMAND ----------

# extracting data in required columns order
data_overdue = spark.sql("SELECT A.OVERDUE_AMOUNT,A.GEO_KEY,A.PER_KEY,A.CUSTOMER_KEY,A.SNAPSHOT_DATE_KEY,A.SALES_TYPE_KEY,A.PRODUCT_LINE_KEY,A.SALES_ORG_KEY,A.DIVISION_KEY,A.LAST_EXECUTED_TIME, A.LAST_EXECUTED_DATE FROM DATA_OVERDUE A")

# COMMAND ----------

# writing data to processed layer table
data_overdue.repartition("product_line_key").write.insertInto(processed_db_name + "." + table_name, overwrite=True)

# COMMAND ----------

# inserting log record to last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
