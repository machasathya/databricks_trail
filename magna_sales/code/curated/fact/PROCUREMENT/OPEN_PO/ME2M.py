# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,upper,concat,to_date,trim,max,sum
from pyspark.sql import functions as sf
from pyspark.sql import types as T
from ProcessMetadataUtility import ProcessMetadataUtility
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
table_name = "FACT_PO"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the notebook manually
# spark.conf.set("spark.sql.broadcastTimeout",36000)
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
# table_name = "FACT_PO"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer and handling reversal movement types
ekko = spark.sql("Select * from {}.{} where (trim(bsart) <>'ZN' or trim(bsart) is null) and ((trim(LOEKZ) is null) or trim(LOEKZ)<>'L')".format(curated_db_name,"EKKO"))
ekpo = spark.sql("Select * from {}.{} where ((trim(LOEKZ) is null) or trim(LOEKZ)<>'L')".format(curated_db_name,"EKPO"))
ekbe = spark.sql("""select EBELN, EBELP,
                 max(case when BWART in ('101') then BUDAT
                     else '' end ) as BUDAT,
                 sum(case when BWART in ('101','123') then MENGE
                      else 0 end) as 101_menge, 
                 sum(case when BWART in ('102','122') then -MENGE
                      else 0 end) as 102_menge,
                sum(case when BWART in ('101','123') then WRBTR
                      else 0 end) as 101_wrbtr, 
                 sum(case when BWART in ('102','122') then -WRBTR
                      else 0 end) as 102_wrbtr
                 from {}.{} where bwart in ('101','102','122','123') group by EBELN,EBELP """.format(curated_db_name,"EKBE"))
t161t = spark.sql("select * from {}.{} WHERE SPRAS='E'".format(curated_db_name,"T161T"))
ekko = ekko.alias("df1").join(t161t.alias("df2"),on=[col("df1.BSART") == col("df2.BSART"),col("df1.BSTYP") == col("df2.BSTYP")]).select(["df1." + cols for cols in ekko.columns] + [col('df2.BATXT')])
eket = spark.sql("select ebeln,ebelp,max(eindt) as eindt from {}.{} group by ebeln,ebelp".format(curated_db_name,"eket"))
eban = spark.sql("select banfn,bnfpo,FRGDT,LOEKZ from {}.{} where (loekz<>'L' or loekz is null)".format(curated_db_name,"eban"))
eket_rel_date = spark.sql("select ebeln,ebelp,banfn,bnfpo from {}.{}".format(curated_db_name,"EKET"))
esll = spark.sql("select * from {}.{} where (DEL<>'X' or DEL is NULL)".format(curated_db_name,"ESLL"))

# COMMAND ----------

#logic for getting release date

rel_date1 = eket_rel_date.alias("l_df").join(eban.alias("r_df"),on=[col("l_df.banfn") == col("r_df.banfn"),
                                                                 col("l_df.bnfpo") == col("r_df.bnfpo")],how= 'left').select([col("l_df.ebeln"),col("l_df.ebelp"),col("l_df.banfn"),col("l_df.bnfpo"),col("r_df.FRGDT"),col("r_df.LOEKZ")])

rel_date2 = rel_date1.groupBy("ebeln","ebelp").agg(max(col("frgdt")).alias("frgdt"))

# COMMAND ----------

# reading data from curated layer and handling debit credit values
ekbe_inv = spark.sql("""select EBELN, EBELP,
                  sum(case when SHKZG like '%S%' then MENGE
                       else 0 end) as s_menge, 
                  sum(case when SHKZG like '%H%' then -MENGE
                       else 0 end) as h_menge,
                 sum(case when SHKZG like '%S%' then WRBTR
                      else 0 end) as s_wrbtr, 
                  sum(case when SHKZG like '%H%' then -WRBTR
                       else 0 end) as h_wrbtr
                  from {}.{} where vgabe like '%2%' group by EBELN,EBELP """.format(curated_db_name,"EKBE"))
ekbe_dow_pay = spark.sql("""select EBELN,EBELP,sum(WRBTR) as WRBTR from {}.{} where trim(VGABE) = '4' group by EBELN,EBELP""".format(curated_db_name,"EKBE"))

# COMMAND ----------

# defining delivered and invoiced quantity
ekbe_del = ekbe.withColumn("DELIVERED_QUANTITY",lit(col('101_menge') + col('102_menge'))).withColumn("DELIVERED_VALUE",lit(col('101_wrbtr') + col('102_wrbtr'))).drop('101_menge','102_menge','101_wrbtr','102_wrbtr')


ekbe_inv_fin = ekbe_inv.withColumn("INVOICED_QUANTITY",lit(col('s_menge') + col('h_menge'))).withColumn("INVOICED_VALUE",lit(col('s_wrbtr') + col('h_wrbtr'))).drop('s_menge','h_menge','s_wrbtr','h_wrbtr')

# COMMAND ----------

# changing column names
df1 = ekko.alias('ekko').join(ekpo.alias('ekpo'),on=[trim(col('ekko.EBELN')) == trim(col('ekpo.EBELN'))],how='inner').select([col('ekpo.werks').alias('PLANT_ID'),
                                                                                                                 col('ekko.lifnr').alias('VENDOR_ID'),
                                                                                                                 col('ekko.EBELN').alias('PURCHASE_DOCUMENT'),
                                                                                                                 col('ekko.aedat').alias('DOCUMENT_DATE'),
                                                                                                                 col('ekpo.EBELP').alias('ITEM'),
                                                                                                                 col('ekko.BATXT').alias('PO_TYPE'),
                                                                                                                 col('ekpo.ELIKZ').alias('DELIVERY_COMPLETION_FLAG'),
                                                                                                                 ((col('ekpo.UNTTO')*(col('ekpo.MENGE')))/100).alias('UNDER_TOLERANCE'),
                                                                                                                 ((col('ekpo.UEBTO')*(col('ekpo.MENGE')))/100).alias('OVER_TOLERANCE'),
                                                                                                                 col('ekpo.MATNR').alias('MATERIAL_ID'),
                                                                                                                 col('ekpo.MENGE').alias('ORDER_QUANTITY'),
                                                                                                                 col('ekpo.MEINS').alias('ORDER_UNIT'),
                                                                                                                 col('ekpo.NETPR').alias('NET_PRICE'),
                                                                                                                 col('ekpo.NETWR').alias('NET_ORDER_VALUE'),
                                                                                                                 col('ekko.WAERS').alias('CURRENCY'),
                                                                                                                 col('ekpo.DPTYP').alias('DOWN_PAYMENT_TYPE'),
                                                                                                                 col('ekpo.DPPCT').alias('DOWN_PAYMENT_PERC'),
                                                                                                                 col('ekpo.DPDAT').alias('DOWN_PAYMENT_DUE_DATE'),
                                                                                                                 col('ekko.SUBMI').alias('COLLECTIVE_NUMBER'),
                                                                                                                 col('ekko.EKORG').alias('PURCHASE_ORGANIZATION'),
                                                                                                                 col('ekpo.LOEKZ').alias('DELETION_INDICATOR'),
                                                                                                                 col('ekko.FRGKE').alias('RELEASE_INDICATOR'),
                                                                                                                 col('ekko.FRGGR').alias('RELEASE_GROUP'),
                                                                                                                 col('ekko.FRGZU').alias('RELEASE_STATUS'),
                                                                                                                 col('ekko.FRGSX').alias('RELEASE_STRATEGY'),
                                                                                                                 col('ekko.BSTYP').alias('PO_DOC_CATEGORY'),
                                                                                                                 col('ekko.EKGRP').alias('PURCHASING_GROUP'),
                                                                                                                 col('ekpo.PSTYP').alias('ITEM_CATEGORY'),
                                                                                                                 col('ekpo.KNTTP').alias('ACCT_ASSIGNMENT_CATEGORY'),
                                                                                                                 col('ekpo.BEDNR').alias('REQ_TRACKING_NUMBER'),
                                                                                                                 col('ekpo.LGORT').alias('STORAGE_LOCATION'),
                                                                                                                 col('ekpo.PEINH').alias('PRICE_UNIT'),
                                                                                                                 col('ekpo.KONNR').alias('OUTLINE_AGREEMENT'),
                                                                                                                 col('ekpo.KTPNR').alias('PRINCIPAL_AGREEMENT_ITEM'),
                                                                                                                 col('ekko.KTWRT').alias('TARGET_VAL_HEADER'),
                                                                                                                 col('ekpo.KTMNG').alias('TARGET_QUANTITY'),
                                                                                                                 col('ekko.KDATB').alias('VALIDITY_PERIOD_START'),
                                                                                                                 col('ekko.KDATE').alias('VALIDITY_PERIOD_END'),
                                                                                                                 col('ekko.ANGDT').alias('QUOTATION_DEADLINE'),
                                                                                                                 col('ekpo.STATU').alias('RFQ_STATUS'),
                                                                                                                 col('ekko.BSAKZ').alias('CONTROL_INDICATOR'),
                                                                                                                 col('ekpo.PACKNO').alias('PACKAGE_NUMBER'),
                                                                                                                 col('ekpo.RESLO').alias('ISSUING_STORAGE_LOC'),
                                                                                                                 col('ekpo.BPRME').alias('PURCHASE_ORDER_UNIT'),
                                                                                                                 col('ekpo.MWSKZ').alias('TAX_CODE'),
                                                                                                                 col('ekpo.TXJCD').alias('TAX_JURISDICTION'),
                                                                                                                 col('ekko.MEMORY').alias('INCOMPLETE'),
                                                                                                                 col('ekpo.SGT_SCAT').alias('STOCK_SEGMENT'),
                                                                                                                 col('ekpo.SGT_RCAT').alias('REQUIREMENT_SEGMENT'),
                                                                                                                 col('ekpo.EXLIN').alias('CONFIGURABLE_ITEM_NUMBER'),
                                                                                                                 col('ekpo.EXSNR').alias('EXTERNAL_SORT_NUMBER'),
                                                                                                                 col('ekpo.EHTYP').alias('EXTERNAL_HIERARCHY_CATEGORY'),
                                                                                                                 col('ekpo.PRIO_URG').alias('REQUIREMENT_URGENCY'),
                                                                                                                 col('ekpo.PRIO_REQ').alias('REQUIREMENT_PRIORITY'),
                                                                                                                ])

# defining delivered qty and value and actual delivery date
df2 = df1.alias('df1').join(ekbe_del.alias('df2'),on=[col('df1.PURCHASE_DOCUMENT') == col('df2.EBELN'),col("df1.ITEM") == col("df2.EBELP")],how='left').select(["df1." + cols for cols in df1.columns] + [col("DELIVERED_QUANTITY"),col("DELIVERED_VALUE"),to_date(col("df2.BUDAT")).alias('ACTUAL_DELIVERY_DATE')]).fillna({ 'DELIVERED_QUANTITY':0,'DELIVERED_VALUE':0})

# defining expected delivery date
df3 = df2.alias('l_df').join(eket.alias("r_df"),on=[col("r_df.ebeln") == col("l_df.PURCHASE_DOCUMENT"),
                                                   col("r_df.ebelp") == col("l_df.ITEM")],how='left').select(["l_df." + cols for cols in df2.columns] + [col("eindt").alias("EXPECTED_DELIVERY_DATE")])

# defining still to be delivered qty
df4 = df3.withColumn("STILL_TO_BE_DELIVERED_QTY",col("ORDER_QUANTITY")-col("DELIVERED_QUANTITY")).withColumn("STILL_TO_BE_DELIVERED_VALUE",col("NET_ORDER_VALUE")-col("DELIVERED_VALUE"))

# defining invoice qty and value
df5 = df4.alias("l_df").join(ekbe_inv_fin.alias("r_df"),on=[col("l_df.PURCHASE_DOCUMENT") == col("r_df.EBELN"),col("l_df.ITEM") == col("r_df.EBELP")],how='left').select(["l_df." + cols for cols in df4.columns]+[col("INVOICED_QUANTITY"),col("INVOICED_VALUE")])

# defining still to be invoiced qty and down payment amount
df5 = df5.withColumn("STILL_TO_BE_INVOICED_QTY",col("DELIVERED_QUANTITY") - col("INVOICED_QUANTITY")).withColumn("STILL_TO_BE_INVOICED_VALUE",col("DELIVERED_VALUE")-col("INVOICED_VALUE")).fillna({ 'STILL_TO_BE_INVOICED_QTY':0,'STILL_TO_BE_INVOICED_VALUE':0})

df_down_paymnt = df5.alias("l_df").join(ekbe_dow_pay.alias("r_df"),on=[col('l_df.PURCHASE_DOCUMENT') == col('r_df.EBELN'),col("l_df.ITEM") == col("r_df.EBELP")],how='left').select(["l_df." + cols for cols in df5.columns] + [col("WRBTR").alias("DOWN_PAYMENT_AMOUNT")]).fillna({'DOWN_PAYMENT_AMOUNT':0})


# Logic For handling Service PO's

ser_pos =df_down_paymnt.where("PO_TYPE in ('HIL Capital Services','HIL Planned Serv POs','HIL UnPld Serv POs')").select("PURCHASE_DOCUMENT","ITEM","PACKAGE_NUMBER")

inter_1 = ser_pos.alias("l_df").join(esll.alias("r_df"),on = col("l_df.PACKAGE_NUMBER") == col("r_df.PACKNO"),how = 'left').select(["l_df." + cols for cols in ser_pos.columns] + [col("SUB_PACKNO")])


inter_2 = inter_1.alias("l_df").join(esll.alias("r_df"),on=[col("l_df.SUB_PACKNO") == col ("r_df.PACKNO")],how='left').select(["l_df." + cols for cols in inter_1.columns] + [col("MENGE"),col("NETWR")])

inter_3 = inter_2.groupBy("PURCHASE_DOCUMENT","ITEM").agg(sum("MENGE").alias("SERVICE_HEADER_QUANTITY"),sum("NETWR").alias("SERVICE_HEADER_VALUE"))

inter_4 = df_down_paymnt.alias("l_df").join(inter_3.alias("r_df"),on = [col("l_df.PURCHASE_DOCUMENT") == col ("r_df.PURCHASE_DOCUMENT"),
                                                                  col("l_df.ITEM") == col("r_df.ITEM")],how='left').select(["l_df." + cols for cols in df_down_paymnt.columns] + [col("SERVICE_HEADER_QUANTITY"),col("SERVICE_HEADER_VALUE")])

inter_5 = inter_4.withColumn("STILL_TO_BE_DELIVERED_QTY",when((col("PO_TYPE").isin(['HIL Capital Services','HIL Planned Serv POs','HIL UnPld Serv POs'])),(col("SERVICE_HEADER_QUANTITY")-col("DELIVERED_QUANTITY"))).otherwise(col("STILL_TO_BE_DELIVERED_QTY"))).withColumn("STILL_TO_BE_DELIVERED_VALUE",when((col("PO_TYPE").isin(['HIL Capital Services','HIL Planned Serv POs','HIL UnPld Serv POs'])),(col("SERVICE_HEADER_VALUE")-col("DELIVERED_VALUE"))).otherwise(col("STILL_TO_BE_DELIVERED_VALUE")))

# COMMAND ----------

# defining still to be delivered qty
final_df1 = inter_5.withColumn("STILL_TO_BE_DELIVERED_QTY",when(((col("DELIVERY_COMPLETION_FLAG") == 'X') & (col("DELIVERED_QUANTITY") < (col("ORDER_QUANTITY") - col("UNDER_TOLERANCE")))),lit(0)).otherwise(col("STILL_TO_BE_DELIVERED_QTY"))).withColumn("STILL_TO_BE_DELIVERED_VALUE",when(((col("DELIVERY_COMPLETION_FLAG") == 'X') & (col("DELIVERED_QUANTITY") < (col("ORDER_QUANTITY") - col("UNDER_TOLERANCE")))),lit(0)).otherwise(col("STILL_TO_BE_DELIVERED_VALUE")))

# defining po final status
final_df2 = final_df1.withColumn("PO_STATUS",when((((col("DELIVERY_COMPLETION_FLAG").isNull()) | (col("DELIVERY_COMPLETION_FLAG")!= 'X')) & (col("DELIVERED_QUANTITY")< (col("ORDER_QUANTITY") - col("UNDER_TOLERANCE")))),"OPEN").when(((col("DELIVERY_COMPLETION_FLAG") == 'X') & (col("DELIVERED_QUANTITY")< (col("ORDER_QUANTITY") - col("UNDER_TOLERANCE")))),"SHORT_CLOSED").otherwise("CLOSED"))

final_df3 = final_df2.withColumn("PO_STATUS",when(((col("PO_TYPE").isin(['HIL Capital Services','HIL UnPld Serv POs'])) & (((col("SERVICE_HEADER_QUANTITY")) <= (col("DELIVERED_QUANTITY"))) | ((col("SERVICE_HEADER_VALUE")) <= (col("DELIVERED_VALUE"))))),"CLOSED")
                                 .when(((col("PO_TYPE").isin(['HIL Capital Services','HIL UnPld Serv POs'])) & (trim(col("deletion_indicator")) =='S') ),"SHORT_CLOSED")
                                 .when((((col("DELIVERY_COMPLETION_FLAG").isNull()) | (col("DELIVERY_COMPLETION_FLAG")!= 'X')) & (col("PO_TYPE").isin(['HIL Capital Services','HIL UnPld Serv POs'])) & ((col("DELIVERED_QUANTITY")< (col("SERVICE_HEADER_QUANTITY") - col("UNDER_TOLERANCE"))) | (col("DELIVERED_VALUE")< (col("SERVICE_HEADER_VALUE") - col("UNDER_TOLERANCE"))))),"OPEN").otherwise(col("PO_STATUS")))


# defining po type
final_df4 = final_df3.withColumn("PO_STATUS",when(((col("PO_TYPE").isin(['HIL Planned Serv POs'])) & ((((col("SERVICE_HEADER_QUANTITY")) <= (col("DELIVERED_QUANTITY"))) & (col("STILL_TO_BE_INVOICED_VALUE") == 0)) | ((col("SERVICE_HEADER_VALUE")) <= (col("DELIVERED_VALUE"))))),"CLOSED")
                                 .when(((col("PO_TYPE").isin(['HIL Planned Serv POs'])) & (trim(col("deletion_indicator")) =='S') ),"SHORT_CLOSED")
                                 .when((((col("DELIVERY_COMPLETION_FLAG").isNull()) | (col("DELIVERY_COMPLETION_FLAG")!= 'X')) & (col("PO_TYPE").isin(['HIL Planned Serv POs'])) & ((col("DELIVERED_QUANTITY")< (col("SERVICE_HEADER_QUANTITY") - col("UNDER_TOLERANCE"))) | (col("STILL_TO_BE_INVOICED_VALUE")!=0) | (col("DELIVERED_VALUE")< (col("SERVICE_HEADER_VALUE") - col("UNDER_TOLERANCE"))))),"OPEN").otherwise(col("PO_STATUS")))

# defining pr release date                                 
final_df = final_df4.alias("l_df").join(rel_date2.alias("r_df"),on = [col("l_df.PURCHASE_DOCUMENT") == col("r_df.EBELN"),
                                                                    col("l_df.ITEM") == col("r_df.EBELP")],how='left').select(["l_df." + cols for cols in final_df3.columns] + [col("r_df.frgdt").alias("PR_RELEASE_DATE")])

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'OPEN_PO', mode='overwrite')

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
PURCHASE_DOCUMENT,
ITEM,
PO_TYPE,
DELIVERY_COMPLETION_FLAG,
UNDER_TOLERANCE,
OVER_TOLERANCE,
ORDER_QUANTITY,
ORDER_UNIT,
NET_PRICE,
NET_ORDER_VALUE,
CURRENCY,
DOWN_PAYMENT_TYPE,
DOWN_PAYMENT_PERC,
DOWN_PAYMENT_AMOUNT,
DOWN_PAYMENT_DUE_DATE,
COLLECTIVE_NUMBER,
DELETION_INDICATOR,
RELEASE_INDICATOR,
RELEASE_GROUP,
RELEASE_STATUS,
RELEASE_STRATEGY,
PO_DOC_CATEGORY,
ITEM_CATEGORY,
ACCT_ASSIGNMENT_CATEGORY,
REQ_TRACKING_NUMBER,
STORAGE_LOCATION,
PRICE_UNIT,
OUTLINE_AGREEMENT,
PRINCIPAL_AGREEMENT_ITEM,
TARGET_VAL_HEADER,
TARGET_QUANTITY,
VALIDITY_PERIOD_START,
VALIDITY_PERIOD_END,
QUOTATION_DEADLINE,
RFQ_STATUS,
CONTROL_INDICATOR,
PACKAGE_NUMBER,
ISSUING_STORAGE_LOC,
PURCHASE_ORDER_UNIT,
TAX_CODE,
TAX_JURISDICTION,
INCOMPLETE,
STOCK_SEGMENT,
REQUIREMENT_SEGMENT,
CONFIGURABLE_ITEM_NUMBER,
EXTERNAL_SORT_NUMBER,
EXTERNAL_HIERARCHY_CATEGORY,
REQUIREMENT_URGENCY,
REQUIREMENT_PRIORITY,
ACTUAL_DELIVERY_DATE,
EXPECTED_DELIVERY_DATE,
PR_RELEASE_DATE,
DELIVERED_QUANTITY,
DELIVERED_VALUE,
STILL_TO_BE_DELIVERED_QTY,
STILL_TO_BE_DELIVERED_VALUE,
INVOICED_QUANTITY,
INVOICED_VALUE,
STILL_TO_BE_INVOICED_QTY,
STILL_TO_BE_INVOICED_VALUE,
SERVICE_HEADER_QUANTITY,
PO_STATUS,
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
d = surrogate_mapping_hil(csv_data,table_name,"OPEN_PO",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+"FACT_PO")

# COMMAND ----------

# insert log data to last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
