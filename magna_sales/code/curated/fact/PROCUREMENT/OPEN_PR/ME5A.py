# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,upper,concat,sum
from pyspark.sql import functions as sf
from pyspark.sql import types as T
from ProcessMetadataUtility import ProcessMetadataUtility
pmu = ProcessMetadataUtility()

# COMMAND ----------

# code to fetch the data from ADF parameters
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
table_name = "FACT_PR"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the notebook manually
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata_prod"
# curated_db_name = "cur_prod"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# scope = "AZR-DBR-KV-SCOPE-300"
# processed_location = "mnt/bi_datalake/prod/pro/"
# processed_schema_name = "global_semantic_prod"
# processed_db_name = "pro_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_PR"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
EBAN = spark.sql("Select * from {}.{} where (trim(LOEKZ) <> 'X' or LOEKZ is null)".format(curated_db_name,"EBAN"))

# COMMAND ----------

# changing column names 
final_1 = EBAN.alias('df').select([col('df.WERKS').alias('PLANT')] + [col('df.BANFN').alias('PURCHASE_REQUISTION')] + [col('df.BADAT').alias('REQUISTION_DATE').cast(T.DateType())] + [col('df.ERDAT').alias('CHANGED_ON').cast(T.DateType())] + [col('df.FRGDT').alias('RELEASE_DATE').cast(T.DateType())] + [col('df.BNFPO').alias('ITEM_OF_REQUISTION')] + [col('df.MATNR').alias('MATERIAL')] + [col('df.TXZ01').alias('SHORT_TEXT')] + [col('df.MENGE').alias('QUANTITY_REQUESTED')] + [col('df.MEINS').alias('UOM')] + [col('df.EBELN').alias('PURCHASE_ORDER')] + [col('df.BSMNG').alias('QUANTITY_ORDERED')] + [col('df.AFNAM').alias('REQUISITIONER')] + [col('df.ERNAM').alias('CREATED_BY')] + [col('df.FRGKZ').alias('RELEASE_INDICATOR')] + [col('df.PREIS').alias('VALUATION_PRICE')] + [col('df.BSART').alias('DOCUMENT_TYPE')] + [col('df.LOEKZ').alias('DELETION_INDICATOR')] + [col('df.STATU').alias('PROCESSING_STATUS')] + [col('df.ESTKZ').alias('CREATION_INDICATOR')] + [col('df.FRGST').alias('RELEASE_STRATEGY')] + [col('df.EKGRP').alias('PURCHASING_GROUP')] + [col('df.EMATN').alias('MPN_MATERIAL')] + [col('df.LGORT').alias('STORAGE_LOCATION')] + [col('df.BEDNR').alias('REQUEST_TRACKING_NO')] + [col('df.MATKL').alias('MATL_GRP')] + [col('df.RESWK').alias('SUPPLYING_PLANT')] + [col('df.BUMNG').alias('SHORTAGE_QUANTITY')] + [col('df.LPEIN').alias('DELIVERY_DATE_CAEGORY')] + [col('df.LFDAT').alias('DELIVERY_DATE').cast(T.DateType())] + [col('df.WEBAZ').alias('GR_PROCESSING_TIME')] + [col('df.PEINH').alias('PRICE_UNIT')] + [col('df.PSTYP').alias('ITEM_CATEGORY')] + [col('df.KNTTP').alias('ACCT_ASSSIGNMENT_CATEGORY')] + [col('df.KZVBR').alias('CONSUMPTION')] + [col('df.VRTKZ').alias('DISTRIBUTION_INDICATOR')] + [col('df.TWRKZ').alias('PARTIAL_INVOCIE')] + [col('df.WEPOS').alias('GOODS_RECEIPT')] + [col('df.WEUNB').alias('GR_NON_VALUATED')] + [col('df.REPOS').alias('INVOICE_RECEIPT')] + [col('df.LIFNR').alias('DESIRED_VENDOR')] + [col('df.FLIEF').alias('FIXED_VENDOR')] + [col('df.EKORG').alias('PURCHASE_ORGANIZATION')] + [col('df.KONNR').alias('OUTLINE_AGREEMENT')] + [col('df.KTPNR').alias('PRINCIPAL_AGREEMENT_ITEM')] + [col('df.INFNR').alias('PURCHASE_INFO_RECORD')] + [col('df.DISPO').alias('MRP_CONTROLLER')] + [col('df.EBELP').alias('PURCHASE_ORDER_ITEM')] + [col('df.BEDAT').alias('PURCHASE_ORDER_DATE')] + [col('df.LBLNI').alias('ENTRY_SHEET')] + [col('df.BWTAR').alias('VALUATION_TYPE')] + [col('df.EBAKZ').alias('CLOSED')] + [col('df.RSNUM').alias('RESERVATION')] + [col('df.SOBKZ').alias('SPECIAL_STOCK')] + [col('df.ARSNR').alias('SETTLEMENT_RES_NO')] + [col('df.ARSPS').alias('ITEM_SETTLEMENT_RESER')] + [col('df.FIXKZ').alias('FIXED_INDICATOR')] + [col('df.BMEIN').alias('ORDER_UNIT')] + [col('df.REVLV').alias('REVISION_LEVEL')] + [col('df.FRGGR').alias('RELEASE_GROUP')] + [col('df.AKTNR').alias('PROMOTION')] + [col('df.CHARG').alias('BATCH')] + [col('df.FIPOS').alias('COMMITMENT_ITEM')] + [col('df.FISTL').alias('FUNDS_CENTER')] + [col('df.GEBER').alias('PR_FUND')] + [col('df.KZKFG').alias('ORIGIN_OF_CONFIGURATION')] + [col('df.SATNR').alias('CROSS_PLANT_CM')] + [col('df.MNG02').alias('COMMITTED_QUANTITY')] + [col('df.DAT01').alias('COMMITTED_DATE')] + [col('df.ATTYP').alias('MATERIAL_CATEGORY')] + [col('df.KUNNR').alias('CUSTOMER')] + [col('df.EMLIF').alias('VENDOR')] + [col('df.LBLKZ').alias('SC_VENDOR')] + [col('df.KZBWS').alias('SPECIAL_STOCK_VALUATION')] + [col('df.RLWRT').alias('TOTAL_VALUE')] + [col('df.WAERS').alias('CURRENCY')] + [col('df.IDNLF').alias('VENDOR_MATERIAL_NUMBER')] + [col('df.GSFRG').alias('OVERALL_RELEASE_OF_REQUISTIONS')] + [col('df.MPROF').alias('MFR_PART_PROFILE')] + [col('df.SPRAS').alias('LANGUAGE_KEY')] + [col('df.MFRPN').alias('MANUFACTURER_PART_NO')] + [col('df.MFRNR').alias('MANUFACTURER')] + [col('df.EMNFR').alias('EXTERNAL_MANUFACTURER')] + [col('df.FORDN').alias('FRAMEWORK_ORDER')] + [col('df.FORDP').alias('FRAMEWORK_ORDER_ITEM')] + [col('df.PLIFZ').alias('PLANNED_DELIVERY_TIME')]  + [col('df.RESLO').alias('ISSUING_STORAGE_LOCATION')] + [col('df.PRIO_URG').alias('REQUIREMENT_URGENCY')] + [col('df.PRIO_REQ').alias('REQUIREMENT_PRIORITY')] + [col('df.MHDRZ').alias('MIN_REM_SHELF_LIFE')] + [col('df.IPRKZ').alias('PERIOD_IND_SLED')] + [col('df.SRM_CONTRACT_ID').alias('CENTRAL_CONTRACT')] + [col('df.SRM_CONTRACT_ITM').alias('CENTRAL_CONTRACT_ITEM')] + [col('df.BUDGET_PD').alias('BUDGET_PERIOD')] + [col('df.FMFGUS_KEY').alias('US_GOVERNMENT_FIELDS')] + [col('df.EKORG').alias('RESPONSIBLE_ORGANIZATIONAL_UNIT')] + [col('df.EPROFILE').alias('PROCUREMENT_PROFILE')] + [col('df.IUID_RELEVANT').alias('IUID_RELEVANT')] + [col('df.SGT_SCAT').alias('STOCK_SEGMENT')] + [col('df.SGT_RCAT').alias('REQUIREMENT_SEGMENT')] + [col('df.BESWK').alias('PROCURING_PLANT')] + [col('df.BANPR').alias('PURCHASE_REQUISTION_PROCESSING_STATE')] + [col('df.BLCKD').alias('BLOCKING_INDICATOR')] + [col('df.BLCKT').alias('BLOCKING_TEXT')]) 

# COMMAND ----------

# replacing nulls with 0's and filtering only open pr's
final_df = final_1.fillna({'QUANTITY_ORDERED':0}).where(col("QUANTITY_REQUESTED") > col("QUANTITY_ORDERED"))

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'OPEN_PR', mode='overwrite')

# COMMAND ----------

# surrogate key mapping
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
    
    if((fact_table_name==fact_table or fact_table_name.lower()==fact_table)):
      count += 1
      tmp_unmapped_str = "CASE WHEN "
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull(A{count}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table, count=count)     
      
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} A{count} on {fact_table}.{fact_column} = A{count}.{dim_column} ".format(
            dim_table=dim_table, count=count, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
      
  select_condition = select_condition[:-2]
  query = """select 
PURCHASE_REQUISTION,
ITEM_OF_REQUISTION,
SHORT_TEXT,
QUANTITY_REQUESTED,
UOM,
PURCHASE_ORDER,
QUANTITY_ORDERED,
REQUISITIONER,
RELEASE_INDICATOR,
VALUATION_PRICE,
DOCUMENT_TYPE,
DELETION_INDICATOR,
PROCESSING_STATUS,
CREATION_INDICATOR,
RELEASE_STRATEGY,
MPN_MATERIAL,
STORAGE_LOCATION,
REQUEST_TRACKING_NO,
MATL_GRP,
SHORTAGE_QUANTITY,
DELIVERY_DATE_CAEGORY,
GR_PROCESSING_TIME,
PRICE_UNIT,
ITEM_CATEGORY,
ACCT_ASSSIGNMENT_CATEGORY,
CONSUMPTION,
DISTRIBUTION_INDICATOR,
PARTIAL_INVOCIE,
GOODS_RECEIPT,
GR_NON_VALUATED,
INVOICE_RECEIPT,
OUTLINE_AGREEMENT,
PRINCIPAL_AGREEMENT_ITEM,
PURCHASE_INFO_RECORD,
MRP_CONTROLLER,
PURCHASE_ORDER_ITEM,
ENTRY_SHEET,
VALUATION_TYPE,
CLOSED,
RESERVATION,
SPECIAL_STOCK,
SETTLEMENT_RES_NO,
ITEM_SETTLEMENT_RESER,
FIXED_INDICATOR,
ORDER_UNIT,
REVISION_LEVEL,
RELEASE_GROUP,
PROMOTION,
BATCH,
COMMITMENT_ITEM,
FUNDS_CENTER,
PR_FUND,
ORIGIN_OF_CONFIGURATION,
CROSS_PLANT_CM,
COMMITTED_QUANTITY,
MATERIAL_CATEGORY,
SPECIAL_STOCK_VALUATION,
TOTAL_VALUE,
CURRENCY,
VENDOR_MATERIAL_NUMBER,
OVERALL_RELEASE_OF_REQUISTIONS,
MFR_PART_PROFILE,
LANGUAGE_KEY,
MANUFACTURER_PART_NO,
MANUFACTURER,
EXTERNAL_MANUFACTURER,
FRAMEWORK_ORDER,
FRAMEWORK_ORDER_ITEM,
PLANNED_DELIVERY_TIME,
ISSUING_STORAGE_LOCATION,
REQUIREMENT_URGENCY,
REQUIREMENT_PRIORITY,
MIN_REM_SHELF_LIFE,
PERIOD_IND_SLED,
CENTRAL_CONTRACT,
CENTRAL_CONTRACT_ITEM,
BUDGET_PERIOD,
US_GOVERNMENT_FIELDS,
RESPONSIBLE_ORGANIZATIONAL_UNIT,
PROCUREMENT_PROFILE,
IUID_RELEVANT,
STOCK_SEGMENT,
REQUIREMENT_SEGMENT,
PURCHASE_REQUISTION_PROCESSING_STATE,
BLOCKING_INDICATOR,
BLOCKING_TEXT,
CHANGED_ON,
RELEASE_DATE,
DELIVERY_DATE,
PURCHASE_ORDER_DATE,
COMMITTED_DATE,
  {select_condition}  from {fact_name}  {join_condition}
  """.format(
              join_condition=join_condition,fact_name=fact_name,select_condition=select_condition)

  print("\nFinal Query for {fact_table}\n (Total Surrogate Keys = {count}) :\n {query}".format(count=count, query=query,fact_table=fact_table))
  fact_final_view_surrogate = spark.sql(query)
  cols = []
  
  for item in fact_final_view_surrogate.columns:
    cols.append( "{} AS {}".format(item ,item.upper()))
  fact_final_view_surrogate = fact_final_view_surrogate.selectExpr(cols)
  #print(cols)
  return fact_final_view_surrogate

# COMMAND ----------

# creating surrogate keys
d = surrogate_mapping_hil(csv_data,table_name,"OPEN_PR",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+"FACT_PR")

# COMMAND ----------

# insert log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
