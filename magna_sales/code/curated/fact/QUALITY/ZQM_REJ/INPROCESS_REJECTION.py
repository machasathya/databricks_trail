# Databricks notebook source
# DBTITLE 1,Import Session
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from ProcessMetadataUtility import ProcessMetadataUtility
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
spark.conf.set("spark.sql.broadcastTimeout",  36000)

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
table_name = "FACT_QUALITY_INPROCESS_REJECTION"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the code manually
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
# table_name = "FACT_QUALITY_INPROCESS_REJECTION"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading data from curated layer
it_t001w = spark.sql("select werks,name1 from {}.{}".format(curated_db_name,"T001w"))

# COMMAND ----------

# reading data from curated layer
afru = spark.sql("SELECT RUECK, RMZHL, BUDAT, ARBID, WERKS, WABLNR,AUFPL,APLZL, MYEAR, AUFNR, VORNR, KAPTPROG FROM {}.{}".format(curated_db_name,"AFRU"))

# COMMAND ----------

# reading data from curated layer
crhd = spark.sql("select * from {}.{} where OBJTY='A'".format(curated_db_name,"CRHD"))

# COMMAND ----------

# joining afru and crhd
it_afru = afru.alias('afru').join(crhd.alias('crhd'),on=[col("afru.ARBID") == col("crhd.OBJID")],how='left').select([col("afru." + cols) for cols in afru.columns]+[col("crhd.ARBPL")])

# COMMAND ----------

# reading data from curated layer
afwi = spark.sql("select * from {}.{}".format(curated_db_name,"afwi"))

# COMMAND ----------

# reading data from curated layer
mseg = spark.sql("select MBLNR,MJAHR,ZEILE,BWART,MATNR,WERKS,ERFMG,ERFME,BUDAT_MKPF,AUFPL,APLZL,AUFNR from {}.{}".format(curated_db_name,"MSEG"))

# COMMAND ----------

# reading data from curated layer
mara = spark.sql("select MATNR,MEINS,BRGEW,VOLUM,SPART from {}.{}".format(curated_db_name,"MARA"))

# COMMAND ----------

# reading data from curated layer
marm = spark.sql("select * FROM {}.{}".format(curated_db_name, "MARM")).where(col("MEINH").isin(["KG"]))

# COMMAND ----------

# reading data from curated layer
qamb = spark.sql("select MBLNR,MJAHR,ZEILE,PRUEFLOS from {}.{}".format(curated_db_name,"QAMB"))

# COMMAND ----------

# reading data from curated layer
qals = spark.sql("select PRUEFLOS,WERK,ART,AUFNR,LMENGEIST,MATNR from {}.{}".format(curated_db_name,"QALS"))

# COMMAND ----------

# reading data from curated layer
qpct = spark.sql("select CODEGRUPPE,CODE,SPRACHE,KURZTEXT from {}.{}".format(curated_db_name,"QPCT"))

# COMMAND ----------

# reading data from curated layer
qmfel = spark.sql("select PRUEFLOS,FENUM,FEGRP,FECOD,ANZFEHLER,KZLOESCH from {}.{} where KZLOESCH is null".format(curated_db_name,"QMFEL"))

# COMMAND ----------

# reading data from curated layer
zpp_cond = spark.sql("select * from {}.{} where ZPROG_NAME like '%ZQM_REJ%'".format(curated_db_name,"ZPP_COND_VALUES"))

# COMMAND ----------

# joining afru with crhd data
it_crhd = it_afru.alias('it_afru').join(crhd.alias('crhd'), on=[col("it_afru.ARBID") == col("crhd.objid"),
                                                               col("it_afru.werks") == col("crhd.werks")], how='left').select([col("it_afru." + cols) for cols in it_afru.columns])

# COMMAND ----------

# joining afru with mseg data
inter_wablnr_aplzl = it_afru.filter("WABLNR is not NULL and APLZL is not NULL").alias('afru').join(mseg.alias('mseg'),on=[col("mseg.mblnr") == col("afru.wablnr"),
                                                                      col("mseg.aufpl") == col("afru.aufpl"),
                                                                      col("mseg.aplzl") == col("afru.aplzl")],how='left').select([col("mseg." + cols) for cols in mseg.columns] + [col("afru.RUECK")]+[col("afru.RMZHL")]+[col("afru.KAPTPROG")]+[col("afru.ARBPL")])

# COMMAND ----------

# joining afru with mseg data
inter_wablnr_aufnr = it_afru.filter("WABLNR is not NULL and APLZL is NULL").alias('afru').join(mseg.alias('mseg'),on=[col("mseg.mblnr") == col("afru.wablnr"),
                                                                      col("mseg.aufnr") == col("afru.aufnr")],how='left').select([col("mseg." + cols) for cols in mseg.columns] + [col("afru.RUECK")]+[col("afru.RMZHL")]+[col("afru.KAPTPROG")]+[col("afru.ARBPL")])

# COMMAND ----------

# joining afru with afwi data
afwi_tab = it_afru.alias('afru').join(afwi.alias('afwi'),on = [col("afru.rueck") == col("afwi.rueck"),
                                                               col("afru.rmzhl") == col("afwi.rmzhl")],how='left').select([col("afwi." + cols) for cols in afwi.columns]+[col("afru.KAPTPROG")]+[col("afru.ARBPL")]).where(col("afwi.mjahr") > 0)

# COMMAND ----------

# joining afwi with mseg data
inter_afwi = afwi_tab.alias('afwi').join(mseg.alias('mseg'), on =[col("mseg.mjahr") == col("afwi.mjahr"),
                                                                  col("mseg.mblnr") == col("afwi.mblnr"),
                                                                  col("mseg.zeile") == col("afwi.mblpo")],how='left').select([col("mseg." + cols) for cols in mseg.columns] + [col("afwi.RUECK")]+[col("afwi.RMZHL")]+[col("afwi.KAPTPROG")]+[col("afwi.ARBPL")])

# COMMAND ----------

# union of data frames
it_gmov = inter_wablnr_aplzl.union(inter_wablnr_aufnr).union(inter_afwi).distinct()

# COMMAND ----------

# filtering data
it_cgmov = it_gmov.filter("bwart in (101,102) and matnr<>'000000000001050009'")
it_cgmov = it_cgmov.orderBy("WERKS")

# COMMAND ----------

# joining cgmov with mara data
it_mara = it_cgmov.alias("cgmov").join(mara.alias('mara'), on=[col("cgmov.MATNR") == col("mara.MATNR")],how='left').select([col("cgmov." + cols) for cols in it_cgmov.columns]+ [col("mara.BRGEW")]+[col("mara.VOLUM")])

# COMMAND ----------

# defining erfmg data
it_mara_1 = it_mara.withColumn("ERFMG",when((col("bwart") == "101"),col("ERFMG")).when((col("bwart") == "102"),(-col("ERFMG"))))

# COMMAND ----------

# defining production qty in mt
it_mara_2 = it_mara_1.withColumn("PRODUCTION_QTY_MT",when(((col("werks").isin(["2010","2013","2018","2022"])) & (col("ERFME") == "EA")),(col("ERFMG")*col("VOLUM"))/1000)
                                           .when((col("ERFME") == 'EA'),(col("ERFMG")*col("BRGEW"))/1000))

# COMMAND ----------

# joining mara with qamb data
it_qamb = it_mara_2.alias('mara').join(qamb.alias('qamb'), on=[col('mara.MBLNR') == col("qamb.MBLNR"),
                                                               col('mara.MJAHR') == col("qamb.MJAHR"),
                                                               col('mara.ZEILE') == col('qamb.ZEILE')],how='inner').select([col("mara." + cols) for cols in it_mara_2.columns] + [col("qamb.PRUEFLOS")])

# COMMAND ----------

# joining qamb with qmfel data
it_qmfel = it_qamb.alias('qamb').join(qmfel.alias('qmfel'),on = [col("qamb.PRUEFLOS") == col("qmfel.PRUEFLOS")],how='inner').select([col("qamb." + cols) for cols in it_qamb.columns] + [col("qmfel.FEGRP")] + [col("qmfel.FECOD")] + [col("ANZFEHLER")])

# COMMAND ----------

# defining defect qty in mt
it_final1 = it_qmfel.withColumn("DEFECT_QTY_MT",when(((col("werks").isin(["2010","2013","2018","2022"])) & (col("ERFME") == "EA")),(col("ANZFEHLER")*col("VOLUM"))/1000)
                                           .when((col("ERFME") == 'EA'),(col("ANZFEHLER")*col("BRGEW"))/1000))

# COMMAND ----------

# joining with zpp_cond values
it_final2 = it_final1.alias('df1').join(zpp_cond.alias('zpp'),on=[col('df1.ARBPL') == col("zpp.ZVALUE")],how='left').select([col("df1." + cols) for cols in it_final1.columns] + 
                                                                                                                          [col("zpp.ZVAR")])

# COMMAND ----------

# defining shift column
it_final3 = it_final2.withColumn("SHIFT",when(col("KAPTPROG") == 'HIL1','SHIFT A')
                                 .when(col("KAPTPROG") == 'HIL2','SHIFT B')
                                 .when(col("KAPTPROG") == 'HIL3','SHIFT C')
                                )

# COMMAND ----------

# selecting columns and pivot based on shift
final_df1 = it_final3.select(col("AUFNR").alias("PRODUCTION_ORDER"),col("PRUEFLOS").alias("INSPECTION_LOT"),col("FEGRP").alias("DEFECT_GROUP"),col("FECOD").alias("DEFECT_CODE"),col("ZVAR").alias("WORK_CENTER"),col("WERKS").alias("PLANT_ID"),col("MATNR").alias("MATERIAL_ID"),col("SHIFT"),col("BUDAT_MKPF").alias("POSTING_DATE"),col("ANZFEHLER").alias("DEFECT_QTY_EA"),col("PRODUCTION_QTY_MT"),col("DEFECT_QTY_MT"))


final_defect_qty_ea_pivot = final_df1.groupBy("DEFECT_GROUP","DEFECT_CODE","WORK_CENTER","PLANT_ID","MATERIAL_ID","POSTING_DATE").pivot("SHIFT",["SHIFT A","SHIFT B","SHIFT C"]).sum("DEFECT_QTY_EA").fillna(0).withColumnRenamed("SHIFT A","SHIFT_A_DEFECT_QTY_EA").withColumnRenamed("SHIFT B","SHIFT_B_DEFECT_QTY_EA").withColumnRenamed("SHIFT C","SHIFT_C_DEFECT_QTY_EA")

final_defect_qty_mt_pivot = final_df1.groupBy("DEFECT_GROUP","DEFECT_CODE","WORK_CENTER","PLANT_ID","MATERIAL_ID","POSTING_DATE").pivot("SHIFT",["SHIFT A","SHIFT B","SHIFT C"]).sum("DEFECT_QTY_MT").fillna(0).withColumnRenamed("SHIFT A","SHIFT_A_DEFECT_QTY_MT").withColumnRenamed("SHIFT B","SHIFT_B_DEFECT_QTY_MT").withColumnRenamed("SHIFT C","SHIFT_C_DEFECT_QTY_MT")


final_prod_qty_mt_pivot = final_df1.groupBy("DEFECT_GROUP","DEFECT_CODE","WORK_CENTER","PLANT_ID","MATERIAL_ID","POSTING_DATE").pivot("SHIFT",["SHIFT A","SHIFT B","SHIFT C"]).sum("PRODUCTION_QTY_MT").fillna(0).withColumnRenamed("SHIFT A","SHIFT_A_PRODUCTION_QTY_MT").withColumnRenamed("SHIFT B","SHIFT_B_PRODUCTION_QTY_MT").withColumnRenamed("SHIFT C","SHIFT_C_PRODUCTION_QTY_MT")

# display(final_prod_qty_mt_pivot)

final_df2 = final_defect_qty_ea_pivot.join(final_defect_qty_mt_pivot,on=["DEFECT_GROUP","DEFECT_CODE","WORK_CENTER","PLANT_ID","MATERIAL_ID","POSTING_DATE"],how='left').select(["DEFECT_GROUP","DEFECT_CODE","WORK_CENTER","PLANT_ID","MATERIAL_ID","POSTING_DATE","SHIFT_A_DEFECT_QTY_EA","SHIFT_B_DEFECT_QTY_EA","SHIFT_C_DEFECT_QTY_EA","SHIFT_A_DEFECT_QTY_MT","SHIFT_B_DEFECT_QTY_MT","SHIFT_C_DEFECT_QTY_MT"])

final_df = final_df2.join(final_prod_qty_mt_pivot,on=["DEFECT_GROUP","DEFECT_CODE","WORK_CENTER","PLANT_ID","MATERIAL_ID","POSTING_DATE"],how='left').select(["DEFECT_GROUP","DEFECT_CODE","WORK_CENTER","PLANT_ID","MATERIAL_ID","POSTING_DATE","SHIFT_A_DEFECT_QTY_EA","SHIFT_B_DEFECT_QTY_EA","SHIFT_C_DEFECT_QTY_EA","SHIFT_A_DEFECT_QTY_MT","SHIFT_B_DEFECT_QTY_MT","SHIFT_C_DEFECT_QTY_MT","SHIFT_A_PRODUCTION_QTY_MT","SHIFT_B_PRODUCTION_QTY_MT","SHIFT_C_PRODUCTION_QTY_MT"])


# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'INPROCESS_REJECTION', mode='overwrite')

# COMMAND ----------

# reading surrogate metadata from SQL
pmu = ProcessMetadataUtility()
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

#--------------------SURROGATE IMPLEMENTATION------------------------
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition_sl="\n left join {pro}.DIM_DEFECT on".format(pro=processed_db_name)
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
  
    if( (fact_table_name==fact_table or fact_table_name.lower()==fact_table) and (dim_table!="DIM_DEFECT") ):
      
      count += 1
      tmp_unmapped_str = "CASE WHEN "
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull({dim_table}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} on {fact_table}.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
    
    if((fact_table_name==fact_table or fact_table_name.lower()==fact_table) and dim_table=='DIM_DEFECT'):
      count +=1
      fact_column_temp = fact_column.split("|")
      dim_column_temp = dim_column.split("|")
      
    #for dim_storage location  
      for frow,drow in zip(fact_column_temp,dim_column_temp):
  
        join_condition_sl += """\n {fact_name}.{frow} = {dim_table}.{drow} AND""".format(fact_name=fact_name,dim_table=dim_table,frow=frow,drow=drow)
      join_condition_sl = join_condition_sl[:-3]
      
  select_condition = select_condition[:-2]   
  join_condition += join_condition_sl
  
  select_condition = select_condition + """
    ,DIM_DEFECT.DEFECT_KEY AS DEFECT_KEY
    """    
  query = """select 
WORK_CENTER,
SHIFT_A_DEFECT_QTY_EA,
SHIFT_B_DEFECT_QTY_EA,
SHIFT_C_DEFECT_QTY_EA,
SHIFT_A_DEFECT_QTY_MT,
SHIFT_B_DEFECT_QTY_MT,
SHIFT_C_DEFECT_QTY_MT,
SHIFT_A_PRODUCTION_QTY_MT,
SHIFT_B_PRODUCTION_QTY_MT,
SHIFT_C_PRODUCTION_QTY_MT
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

# creating surrogate keys
d = surrogate_mapping_hil(csv_data,table_name,"INPROCESS_REJECTION", processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location + table_name)

# COMMAND ----------

# insert log data into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
