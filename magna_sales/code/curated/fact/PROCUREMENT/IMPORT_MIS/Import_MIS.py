# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,max as _max,min,first,sum,trim
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

# enabling cross join
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

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
table_name = "FACT_IMPORT_MIS"
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
# processed_schema_name = "sales_semantic_prod"
# processed_db_name = "pro_prod"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_IMPORT_MIS"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# class object definition
pmu = ProcessMetadataUtility()

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
ekpo = spark.sql("select ebeln,werks,ebelp,TXZ01,menge,meins,matnr,netpr from {}.{}".format(curated_db_name, "EKPO"))
ekko = spark.sql("select ebeln,aedat,lifnr,waers,knumv from {}.{} where BSART = 'ZIM'".format(curated_db_name, "EKKO"))
ekbe_inv_dat = spark.sql("select ebeln,ebelp,belnr,gjahr,budat from {}.{} where BEWTP='Q'".format(curated_db_name,"ekbe"))
ekbe_actl_plnt_del_dat = spark.sql("select ebeln,ebelp,budat from {}.{} where (trim(VGABE)='1')".format(curated_db_name,"ekbe"))
ekbz_boe = spark.sql("select ebeln,ebelp,belnr,gjahr from {}.{} where lifnr like '%1501682%' and bewtp='M'".format(curated_db_name,"ekbz"))
ekbe_bl_no = spark.sql("select ebeln,ebelp,belnr,gjahr from {}.{} where vgabe='A'".format(curated_db_name,"ekbe"))
ekbe_ship_line = spark.sql("select ebeln,ebelp,belnr,gjahr from {}.{} where vgabe='4'".format(curated_db_name,"ekbe"))
ekbe_boe_xr = spark.sql("select ebeln,ebelp,belnr,gjahr from {}.{} where vgabe='2'".format(curated_db_name,"ekbe"))
rbkp = spark.sql("Select * from {}.{}".format(curated_db_name,"rbkp"))
bkpf= spark.sql("select belnr,gjahr,bktxt from {}.{}".format(curated_db_name, "BKPF"))
bseg =spark.sql("select * from {}.{} where umsks in ('A','V') and vorgn = 'AZBU' and fwbas != 0".format(curated_db_name,"BSEG"))
konv = spark.sql("select * from {}.{} where kschl in ('JCDB','ZFBN','JSWS','ZLOF','ZFBG','ZFBH','ZFBF','ZFBB','ZLHC','ZFBM','ZFBO')".format(curated_db_name,"KONV"))
lfa1 = spark.sql("select lifnr,name1 from {}.{}".format(curated_db_name,"LFA1"))

# COMMAND ----------

# joining PO header and item table
df1 = ekko.alias("df1").join(ekpo.alias("df2"),on=[col("df1.ebeln") == col("df2.ebeln")],how='inner').select( [col("df2.ebeln"),col("df2.ebelp"),col("df1.aedat"),col("df1.lifnr"),col("df1.knumv"),col("df2.werks"),col("df2.matnr"),col("df2.menge"),col("df2.meins"),col("df2.netpr"),col("df1.waers")])

# COMMAND ----------

#logic for invoice date

inter_1 = ekbe_inv_dat.alias("l_df").join(rbkp.alias("r_df"),on=[col("l_df.belnr") == col("r_df.belnr"),col("l_df.gjahr") == col("r_df.gjahr")]).select(["l_df." + cols for cols in ekbe_inv_dat.columns] + [col("r_df.bldat")])

inter_2 = inter_1.groupBy("ebeln","ebelp").agg(min("bldat").alias("bldat"))

df2 = df1.alias("l_df").join(inter_2.alias("r_df"),on=[col("l_df.ebeln") == col("r_df.ebeln"),col("l_df.ebelp") == col("r_df.ebelp")],how='left').select(["l_df." + cols for cols in df1.columns] + [col("r_df.bldat").alias("INVOICE_DATE")])

# COMMAND ----------

#Logic for Bill of entry No and date

inter_3 = ekbz_boe.alias("l_df").join(rbkp.alias("r_df"),on=[col("l_df.belnr") == col("r_df.belnr"),col("l_df.gjahr") == col("r_df.gjahr")]).select(["l_df." + cols for cols in ekbz_boe.columns] + [col("r_df.xblnr"),col("r_df.bldat")]).orderBy("bldat")

inter_4 = inter_3.groupBy("ebeln","ebelp").agg(first("bldat").alias("bldat"),first("xblnr").alias("xblnr"))


df3 = df2.alias("l_df").join(inter_4.alias("r_df"),on=[col("l_df.ebeln") == col("r_df.ebeln"),col("l_df.ebelp") == col("r_df.ebelp")],how='left').select(["l_df." + cols for cols in df2.columns] + [col("r_df.bldat").alias("BILL_OF_ENTRY_DATE"),col("r_df.xblnr").alias("BILL_OF_ENTRY_NO")])

# COMMAND ----------

#Logic for BL No

inter_5 = ekbe_bl_no.alias("l_df").join(bkpf.alias("r_df"),on=[col("l_df.belnr") == col("r_df.belnr"),col("l_df.gjahr") == col("r_df.gjahr")]).select(["l_df." + cols for cols in ekbz_boe.columns] + [col("r_df.bktxt")])


inter_6 = inter_5.groupBy("ebeln","ebelp").agg(first("bktxt").alias("bktxt"))


df4 = df3.alias("l_df").join(inter_6.alias("r_df"),on=[col("l_df.ebeln") == col("r_df.ebeln"),col("l_df.ebelp") == col("r_df.ebelp")],how='left').select(["l_df." + cols for cols in df3.columns] + [col("r_df.bktxt").alias("BL_NO")])


# COMMAND ----------

#logic for shipping line

inter_ship1 = ekbe_ship_line.alias("l_df").join(bseg.alias("r_df"),on = [col("l_df.belnr") == col("r_df.belnr"),col("l_df.gjahr") == col("r_df.gjahr")]).select(["l_df." + cols for cols in ekbe_ship_line.columns] + [col("r_df.lifnr")])

inter_ship2 = inter_ship1.groupBy("ebeln","ebelp").agg(first("lifnr").alias("lifnr"))

fin_ship = df4.alias("l_df").join(inter_ship2.alias("r_df"),on=[col("l_df.ebeln") == col("r_df.ebeln"),
                                                               col("l_df.ebelp") == col("r_df.ebelp")],how='left').select(["l_df." + cols for cols in df4.columns] + [col("r_df.lifnr").alias("SHIPPING_LINE_ID")])

# COMMAND ----------

#logic for plant delivery date

inter_7 = ekbe_actl_plnt_del_dat.groupBy("ebeln","ebelp").agg(min("budat").alias("budat"))

df5 = fin_ship.alias("l_df").join(inter_7.alias("r_df"),on=[col("l_df.ebeln") == col("r_df.ebeln"),col("l_df.ebelp") == col("r_df.ebelp")],how='left').select(["l_df." + cols for cols in fin_ship.columns] + [col("r_df.budat").alias("ACTUAL_PLANT_DELIVERY_DATE")])


# COMMAND ----------

#logic for BOE Exchange rate

inter_8 = ekbe_boe_xr.alias("l_df").join(rbkp.alias("r_df"),on=[col("l_df.belnr") == col("r_df.belnr"),col("l_df.gjahr") == col("r_df.gjahr")]).select(["l_df." + cols for cols in ekbe_boe_xr.columns] + [col("r_df.kursf")]).orderBy("bldat")

inter_9 = inter_8.groupBy("ebeln","ebelp").agg(first("kursf").alias("kursf"))

df6 = df5.alias("l_df").join(inter_9.alias("r_df"),on=[col("l_df.ebeln") == col("r_df.ebeln"),col("l_df.ebelp") == col("r_df.ebelp")],how='left').select(["l_df." + cols for cols in df5.columns] + [col("r_df.kursf").alias("BOE_EXCHANGE_RATE")]).withColumn('ebelp',sf.regexp_replace('ebelp', r'^[0]*', ''))


# COMMAND ----------

inter_10 = konv.groupBy("knumv","kposn","kschl").agg(sum("KBETR").alias("KBETR"),sum("KWERT").alias("KWERT")).withColumn('kposn',sf.regexp_replace('kposn', r'^[0]*', ''))

#logic for CONDITIONAL TYPE COLUMNS
df7 = df6.alias("l_df").join(inter_10.where("kschl = 'JCDB'").alias("r_df"),on=[col("l_df.knumv") == col("r_df.knumv"),col("l_df.ebelp") == col("r_df.kposn")],how='left').select(["l_df." + cols for cols in df6.columns] + [(col("r_df.kbetr")/10).alias("BASIC_CUSTOM_DUTY")])

df8 = df7.alias("l_df").join(inter_10.where("kschl = 'ZFBN'").alias("r_df"),on=[col("l_df.knumv") == col("r_df.KNUMV"),col("l_df.ebelp") == col("r_df.KPOSN")],how='left').select(["l_df." + cols for cols in df7.columns] + [col("r_df.kbetr").alias("ANTI_DUMPING_DUTY")])

df9 = df8.alias("l_df").join(inter_10.where("kschl = 'JSWS'").alias("r_df"),on=[col("l_df.knumv") == col("r_df.KNUMV"),col("l_df.ebelp") == col("r_df.KPOSN")],how='left').select(["l_df." + cols for cols in df8.columns] + [col("r_df.kbetr").alias("SOCIAL_WELFARE")])

df10 = df9.alias("l_df").join(inter_10.where("kschl = 'ZLOF'").alias("r_df"),on=[col("l_df.knumv") == col("r_df.KNUMV"),col("l_df.ebelp") == col("r_df.KPOSN")],how='left').select(["l_df." + cols for cols in df9.columns] + [col("r_df.kbetr").alias("DELIVERY_ORDER_FEES")])

df11 = df10.alias("l_df").join(inter_10.where("kschl in ('ZFBG')").alias("r_df"),on=[col("l_df.knumv") == col("r_df.KNUMV"),col("l_df.ebelp") == col("r_df.KPOSN")],how='left').select(["l_df." + cols for cols in df10.columns] + [col("r_df.kbetr").alias("ALL_CONCORE_CHARGES")])

CFS = df11.alias("l_df").join(inter_10.where("kschl in ('ZFBH')").alias("r_df"),on=[col("l_df.knumv") == col("r_df.KNUMV"),col("l_df.ebelp") == col("r_df.KPOSN")],how='left').select(["l_df." + cols for cols in df11.columns] + [col("r_df.kbetr").alias("CFS_CHARGES")]).withColumn("CONCORE_CHARGES",col("ALL_CONCORE_CHARGES")+col("CFS_CHARGES"))

df12 = CFS.alias("l_df").join(inter_10.where("kschl = 'ZFBM'").alias("r_df"),on=[col("l_df.knumv") == col("r_df.KNUMV"),col("l_df.ebelp") == col("r_df.KPOSN")],how='left').select(["l_df." + cols for cols in CFS.columns] + [col("r_df.kbetr").alias("CLRG_AGENT_SERVICES")])

df13 = df12.alias("l_df").join(inter_10.where("kschl = 'ZFBO'").alias("r_df"),on=[col("l_df.knumv") == col("r_df.KNUMV"),col("l_df.ebelp") == col("r_df.KPOSN")],how='left').select(["l_df." + cols for cols in df12.columns] + [col("r_df.kbetr").alias("INLAND_FREIGHT_VALUE")])

df14 = df13.alias("l_df").join(inter_10.where("kschl = 'ZFBF'").alias("r_df"),on=[col("l_df.knumv") == col("r_df.KNUMV"),col("l_df.ebelp") == col("r_df.KPOSN")],how='left').select(["l_df." + cols for cols in df13.columns] + [col("r_df.kbetr").alias("CLEARING_CHARGES")])

df15 = df14.alias("l_df").join(inter_10.where("kschl = 'ZFBB'").alias("r_df"),on=[col("l_df.knumv") == col("r_df.KNUMV"),col("l_df.ebelp") == col("r_df.KPOSN")],how='left').select(["l_df." + cols for cols in df14.columns] + [col("r_df.kbetr").alias("OCEAN_FREIGHT")])

df16 = df15.alias("l_df").join(inter_10.where("kschl = 'ZLHC'").alias("r_df"),on=[col("l_df.knumv") == col("r_df.KNUMV"),col("l_df.ebelp") == col("r_df.KPOSN")],how='left').select(["l_df." + cols for cols in df15.columns] + [col("r_df.kbetr").alias("INLAND_HAULAGE")])


df17 = df16.alias("l_df").join(lfa1.alias("r_df"),on = [col("l_df.SHIPPING_LINE_ID") == col("r_df.lifnr")],how='left').select(["l_df." + cols for cols in df16.columns] + [col("r_df.name1").alias("SHIPPING_LINE")])

df18 = df17.withColumn("menge",when(trim(col("meins")) == 'KG',col("menge")/1000).otherwise(col("menge")))

df19 = df18.withColumn("meins",when(trim(col("meins")) == 'KG',"MT").otherwise(col('meins')))

# COMMAND ----------

# selecting final columns
final_df = df19.selectExpr("ebeln as PURCHASE_DOC","ebelp as ITEM","aedat as CREATED_DATE","lifnr as SUPPLIER_ID","SHIPPING_LINE","werks as PLANT_ID","matnr as MATERIAL_ID","menge as QUANTITY","meins as UOM","netpr as NET_PRICE","waers as CURRENCY","INVOICE_DATE","BILL_OF_ENTRY_DATE","BILL_OF_ENTRY_NO","BL_NO","ACTUAL_PLANT_DELIVERY_DATE","BOE_EXCHANGE_RATE","BASIC_CUSTOM_DUTY","ANTI_DUMPING_DUTY","SOCIAL_WELFARE","DELIVERY_ORDER_FEES","CONCORE_CHARGES","CLRG_AGENT_SERVICES","INLAND_FREIGHT_VALUE","CLEARING_CHARGES","OCEAN_FREIGHT","INLAND_HAULAGE")

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'IMPORT_MIS', mode='overwrite')

# COMMAND ----------

# surrogate key implementation
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
PURCHASE_DOC,
ITEM,
SHIPPING_LINE,
QUANTITY,
UOM,
NET_PRICE,
CURRENCY,
INVOICE_DATE,
BILL_OF_ENTRY_DATE,
BILL_OF_ENTRY_NO,
BL_NO,
ACTUAL_PLANT_DELIVERY_DATE,
BOE_EXCHANGE_RATE,
BASIC_CUSTOM_DUTY,
ANTI_DUMPING_DUTY,
SOCIAL_WELFARE,
DELIVERY_ORDER_FEES,
CONCORE_CHARGES,
CLRG_AGENT_SERVICES,
INLAND_FREIGHT_VALUE,
CLEARING_CHARGES,
OCEAN_FREIGHT,
INLAND_HAULAGE,
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
d = surrogate_mapping_hil(csv_data,table_name,"IMPORT_MIS",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# inserting log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
