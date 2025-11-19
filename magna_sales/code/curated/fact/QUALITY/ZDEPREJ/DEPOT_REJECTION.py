# Databricks notebook source
# DBTITLE 1,Import Session
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr,when
from ProcessMetadataUtility import ProcessMetadataUtility
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
spark.conf.set("spark.sql.broadcastTimeout",  36000)

# COMMAND ----------

# class object definition
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
table_name = "FACT_QUALITY_DEPOT_REJECTION"
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
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")
# table_name = "FACT_QUALITY_DEPOT_REJECTION"

# COMMAND ----------

# DBTITLE 1,Variables Declaration

ekko = "ekko"
ekbe = "ekbe"
ekpo = "ekpo"
mseg = "mseg"
makt = "makt"
mkpf = "mkpf"
t001l= "t001l"
t001w="t001w"


# COMMAND ----------

# DBTITLE 1,Read the table EKKO - Purchasing Document Header
#EKKO       Purchasing Document Header
#RESWK 	Supplying Plant information
#EBELN 	Purchasing Document information

ekko_df = spark.sql("select reswk, ebeln,knumv from {}.{} where BSART='ZST'".format(curated_db_name, ekko))
ekko_df.createOrReplaceTempView("ekko_tmp")

# COMMAND ----------

# DBTITLE 1,Read the table EKBE - History per Purchasing Document data
## EKBE 	History per Purchasing Document data
## MENGE 	Quantity information
# BUDAT 	Posting Date information
# XBLNR 	Reference information
# BELNR 	Docxxument Number information
# BUZEI 	Line item information
# GJAHR 	Fiscal Year information
# EBELN 	Purchasing Document information
# EBELP	    Item information
# BWART 	Movement Type information
# LSMEH 	Delivery Note Unit information
# MATNR 	Material information
# WERKS 	Plant information

ekbe_df = spark.sql("""select 
                    werks, 
                    xblnr, 
                    belnr, 
                    buzei, 
                    budat, 
                    bwart, 
                    matnr, 
                    menge,
                    lsmeh, 
                    gjahr,
                    ebeln,
                    ebelp
                    from
                    {}.{}""".format(curated_db_name, ekbe))
ekbe_df.createOrReplaceTempView("ekbe_tmp")

# COMMAND ----------

# DBTITLE 1,Read the table EKPO - Purchasing Document Item
# EKPO	Purchasing Document Item
# MTART 	Material Type information
# EBELN 	Purchasing Document information

ekpo_df = spark.sql("select mtart, ebeln from {}.{}".format(curated_db_name,ekpo))
ekpo_df.createOrReplaceTempView("ekpo_tmp")
# display(ekpo_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Create Internal Table ITAB - FACT Base Table

# COMMAND ----------

# DBTITLE 1,Create Internal Table called ITAB 
itab = spark.sql("""
            SELECT DISTINCT 
               ekko_tmp.reswk,
               ekko_tmp.knumv,
               ekbe_tmp.werks,
               ekbe_tmp.xblnr,
               ekbe_tmp.belnr,
               ekbe_tmp.buzei,
               ekbe_tmp.budat,
               ekbe_tmp.bwart,
               ekbe_tmp.matnr,
               ekpo_tmp.mtart,
               ekbe_tmp.menge,
               ekbe_tmp.lsmeh,
               ekbe_tmp.gjahr,
               ekbe_tmp.ebeln,
               ekbe_tmp.ebelp
           FROM ( ekko_tmp INNER JOIN ekbe_tmp ON ekko_tmp.ebeln = ekbe_tmp.ebeln )
             INNER JOIN ekpo_tmp ON 
             ekko_tmp.ebeln = ekpo_tmp.ebeln
             WHERE ekbe_tmp.bwart IN ('101','102')
             AND ekpo_tmp.mtart IN ('FERT','HAWA') 
           """)
# and ekko_tmp.reswk = 2009 and ekbe_tmp.budat='2020-11-01' and ekbe_tmp.werks = 6012

itab = itab.withColumn("menge",when(col("bwart") =='102',-col("menge")).otherwise(col("menge")))
itab.createOrReplaceTempView("itab_tmp")
itab.createOrReplaceTempView("l_dummy")
# display(itab.filter("belnr = '5000540645'"))

# COMMAND ----------

# DBTITLE 1,Read the table MSEG - Document Segment: Material data
mseg_df = spark.sql("select matnr,menge,mblnr,mjahr,bwart,lgort,werks,ebeln,zeile from {}.{}".format(curated_db_name,mseg))
mseg_df.createOrReplaceTempView("mseg_tmp")
# display(mseg_df.filter("mblnr = '5000540645'"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create ITSTOCK Table

# COMMAND ----------

# DBTITLE 1,Create ITSTOCK from MESG Document Segment: Material data and ITAB (RIGHT JOIN) - #Need
#Need information on Join condition & Primary keys

it_stock = spark.sql("""		   		  		   
		   SELECT  
			    mseg_tmp.matnr,
				mseg_tmp.menge,
				mseg_tmp.mblnr,
				mseg_tmp.mjahr,
				mseg_tmp.lgort,
				mseg_tmp.werks,
				mseg_tmp.ebeln,
				mseg_tmp.zeile,
                mseg_tmp.bwart
		  FROM mseg_tmp INNER JOIN itab_tmp
			 ON mseg_tmp.mblnr = itab_tmp.belnr
			 AND mseg_tmp.werks = itab_tmp.werks
			 AND mseg_tmp.matnr = itab_tmp.matnr
			 AND mseg_tmp.ebeln = itab_tmp.ebeln
             AND mseg_tmp.zeile = itab_tmp.buzei
			""")
# 
#Temp view for the IT_STOCK

it_stock1 = it_stock.withColumn("menge",when(col("bwart") == '102',-col("menge")).otherwise(col("menge")))

it_stock1.createOrReplaceTempView('it_stock_tmp')

# COMMAND ----------

# DBTITLE 1,Read Table MAKT for MATERIAL DESCRIPTION
makt_df = spark.sql("select maktx, matnr, spras from {}.{}".format(curated_db_name,makt))
makt_df.createOrReplaceTempView("makt_tmp")
# display(makt_df)

# COMMAND ----------

# DBTITLE 1,Add MATERIAL DESCRIPTION to the FACT L_DUMMY
l_dummy_withmatdesc = spark.sql("""
              SELECT l_dummy.*, makt_tmp.maktx 
              FROM l_dummy LEFT JOIN makt_tmp
              ON l_dummy.matnr = makt_tmp.matnr
              AND makt_tmp.spras = 'E'
        """)

l_dummy_withmatdesc.createOrReplaceTempView('l_dummy_withmatdesc_tmp')

# COMMAND ----------

# DBTITLE 1,Update the ITSTOCK - To be Reviewed
# Wrote for READ TABLE logic -- To be reviewes
# Gave more records for below logic 318932  --> 319293

#  READ TABLE it_stock WITH KEY mblnr = l_dummy-belnr
#                            zeile = l_dummy-buzei
#                            matnr = l_dummy-matnr
#                            menge = l_dummy-menge.

it_stock2 = spark.sql("""
      SELECT it_stock_tmp.* 
      FROM it_stock_tmp INNER JOIN l_dummy_withmatdesc_tmp
      ON it_stock_tmp.mblnr = l_dummy_withmatdesc_tmp.belnr
      AND it_stock_tmp.zeile = l_dummy_withmatdesc_tmp.buzei
      AND it_stock_tmp.matnr = l_dummy_withmatdesc_tmp.matnr
      AND it_stock_tmp.menge = l_dummy_withmatdesc_tmp.menge
      """)

it_stock2.createOrReplaceTempView('it_stock_tmp')

# COMMAND ----------

# DBTITLE 1,Add STORAGE LOCATION INFORMATION to the FACT L_DUMMY Table
l_dummy_withloc = spark.sql("""
    SELECT l_dummy_withmatdesc_tmp.*, it_stock_tmp.lgort as stock_lgort
    FROM l_dummy_withmatdesc_tmp LEFT JOIN it_stock_tmp
    ON l_dummy_withmatdesc_tmp.belnr = it_stock_tmp.MBLNR
	AND l_dummy_withmatdesc_tmp.menge = it_stock_tmp.menge
	AND l_dummy_withmatdesc_tmp.matnr = it_stock_tmp.matnr
	AND l_dummy_withmatdesc_tmp.buzei = it_stock_tmp.zeile
    AND l_dummy_withmatdesc_tmp.werks = it_stock_tmp.werks
    AND l_dummy_withmatdesc_tmp.ebeln = it_stock_tmp.ebeln
""")
l_dummy_withloc.createOrReplaceTempView('l_dummy_withloc_tmp')

# COMMAND ----------

# MAGIC %md
# MAGIC # DETERMINE THE GOOD STOCK AND DEFECTIVE STOCK
# MAGIC

# COMMAND ----------

l_dummy_withstock = spark.sql("""
  SELECT l_dummy_withloc_tmp.*, 
  CASE
    WHEN (LEFT(it_stock_tmp.lgort, 1) = '4' or LEFT(it_stock_tmp.lgort, 1) = '2') 
    THEN it_stock_tmp.menge 
    ELSE 0
  END AS gstock,
  CASE
    WHEN (LEFT(it_stock_tmp.lgort, 1) IN ('B', 'C', 'b', 'c') 
         OR ( LEFT(it_stock_tmp.lgort, 1) != '2' AND LEFT(it_stock_tmp.lgort, 1) != '4')) 
    THEN it_stock_tmp.menge 
    ELSE 0
  END AS dstock
  FROM l_dummy_withloc_tmp left join it_stock_tmp 
  ON l_dummy_withloc_tmp.belnr = it_stock_tmp.mblnr
	AND l_dummy_withloc_tmp.menge = it_stock_tmp.menge
	AND l_dummy_withloc_tmp.matnr = it_stock_tmp.matnr
	AND l_dummy_withloc_tmp.buzei = it_stock_tmp.zeile
    AND l_dummy_withloc_tmp.werks = it_stock_tmp.werks
    AND l_dummy_withloc_tmp.ebeln = it_stock_tmp.ebeln
""")

l_dummy_withstock.createOrReplaceTempView('l_dummy_withstock_tmp')

# COMMAND ----------

# MAGIC %md
# MAGIC # Add Document Header Text information to FACT

# COMMAND ----------

# DBTITLE 1,Read Table MKPF for Document Header Text information
mkpf_df = spark.sql("select * from {}.{}".format(curated_db_name, mkpf))
mkpf_df.createOrReplaceTempView("mkpf_tmp")
# display(mkpf_df)

# COMMAND ----------

# DBTITLE 1,Fetch the common column between the FACT (L_DUMMY) and the MKPF dataframe
list(set(l_dummy_withstock.columns).intersection(set(mkpf_df.columns)))

# COMMAND ----------

# DBTITLE 1,Add Document Header Text information (BKTXT) to the FACT 
l_dummy_withdocheader = spark.sql("""
    SELECT l_dummy_withstock_tmp.*, mkpf_tmp.bktxt
    FROM l_dummy_withstock_tmp 
    LEFT JOIN mkpf_tmp
    ON l_dummy_withstock_tmp.belnr = mkpf_tmp.mblnr
	AND l_dummy_withstock_tmp.gjahr = mkpf_tmp.mjahr
""")
l_dummy_withdocheader.createOrReplaceTempView('l_dummy_withdocheader_tmp')

# COMMAND ----------

# MAGIC %md
# MAGIC # FOR STORAGE LOCATION DESCRIPTION

# COMMAND ----------

# DBTITLE 1,Read Table T001L  for Storage Locations data
t001l_df = spark.sql("select lgobe, lgort, werks from {}.{}".format(curated_db_name, t001l ))
t001l_df.createOrReplaceTempView("t001l_tmp")
# display(t001l_df)

# COMMAND ----------

# DBTITLE 1,Join with FACT (L_DUMMY) with T001L on lgort, werks  - #Need information on Join condition & Primary keys
l_dummy_withlocdesc = spark.sql("""
    SELECT l_dummy_withdocheader_tmp.*, t001l_tmp.lgobe
    FROM l_dummy_withdocheader_tmp 
    LEFT JOIN t001l_tmp
    ON l_dummy_withdocheader_tmp.stock_lgort = t001l_tmp.lgort
	AND l_dummy_withdocheader_tmp.werks = t001l_tmp.werks
""")
l_dummy_withlocdesc.createOrReplaceTempView('l_dummy_withlocdesc_tmp')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # RECEIVING PLANT NAME

# COMMAND ----------

# DBTITLE 1,Read Table T001W for Plants/Branches data
t001w_df = spark.sql("select name1,werks from {}.{}".format(curated_db_name, t001w ))
t001w_df.createOrReplaceTempView("t001w_tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## RECEIVING PLANT NAME

# COMMAND ----------

# DBTITLE 1,Join with FACT (L_DUMMY) with T001W on werks  - #Need information on Join condition & Primary keys
l_dummy_withlrecvplant = spark.sql("""
    SELECT l_dummy_withlocdesc_tmp.*, t001w_tmp.name1
    FROM l_dummy_withlocdesc_tmp 
    LEFT JOIN t001w_tmp
	ON l_dummy_withlocdesc_tmp.werks = t001w_tmp.werks
""")
l_dummy_withlrecvplant.createOrReplaceTempView('l_dummy_withlrecvplant_tmp')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## SENDING PLANT NAME

# COMMAND ----------

# DBTITLE 1,Sending Plant Name
l_dummy_withsendplant = spark.sql("""
    SELECT l_dummy_withlrecvplant_tmp.*, t001w_tmp.name1 as name
    FROM l_dummy_withlrecvplant_tmp 
    LEFT JOIN t001w_tmp
	ON l_dummy_withlrecvplant_tmp.reswk = t001w_tmp.werks
""")
l_dummy_withsendplant.createOrReplaceTempView('l_dummy_withsendplant_tmp')

# COMMAND ----------

# DBTITLE 1,Logic for the Quantity Weight into Tons 
#To be added
marm_df = spark.sql("select * from {}.{}".format(curated_db_name, 'marm' ))
marm_df.createOrReplaceTempView("marm_tmp")
# display(marm_df)

# COMMAND ----------

# %sql

# -- select l_dummy_withsendplant_tmp.*, 
# --     ((marm_tmp.UMREN/marm_tmp.UMREZ)*l_dummy_withsendplant_tmp.gstock)/1000 as gstock_inmt, 
# --     ((marm_tmp.UMREN/marm_tmp.UMREZ)*l_dummy_withsendplant_tmp.dstock)/1000 as dstock_inmt
# --     from l_dummy_withsendplant_tmp left join marm_tmp
# --     on l_dummy_withsendplant_tmp.matnr = marm_tmp.matnr
# --     where marm_tmp.meinh = 'KG'
    
l_dummy_withmt = spark.sql("""
      select l_dummy_withsendplant_tmp.*, 
          ((marm_tmp.UMREN/marm_tmp.UMREZ)*l_dummy_withsendplant_tmp.gstock)/1000 as gstock_inmt, 
          ((marm_tmp.UMREN/marm_tmp.UMREZ)*l_dummy_withsendplant_tmp.dstock)/1000 as dstock_inmt,
          marm_tmp.meinh
      from l_dummy_withsendplant_tmp left join marm_tmp
      on l_dummy_withsendplant_tmp.matnr = marm_tmp.matnr
      where marm_tmp.meinh = 'KG'
""") 
l_dummy_withmt.createOrReplaceTempView("l_dummy_withmt_tmp")

# COMMAND ----------

# DBTITLE 1,Create IT_FINAL from L_DUMMY
l_dummy_withsendplant.createOrReplaceTempView('itfinal_tmp')

# COMMAND ----------

# DBTITLE 1,Delete ADJACENT DUPLICATES FROM it_final COMPARING xblnr buzei
#To be updated 

# COMMAND ----------

# DBTITLE 1,Create FACT for TBR based on L_DUMMY
fact_tbr = spark.sql("""
        select 
          reswk as SUPPLYING_PLANT,
          knumv,
          werks as RECEIVING_PLANT,
          belnr as ack_num,
          budat as sending_date,
          matnr as material_code,
          maktx as material_desc,
          gjahr as financial_year,
          gstock as good_stock,
          bwart,
          gstock_inmt as quantity_in_tons,
          dstock as defective_stock,
          dstock_inmt as rejections_in_tons,
          (dstock/(gstock+dstock)) as rejection,
          (dstock/(gstock+dstock))*100 as rejection_per,
          stock_lgort as storage_location,
          lgobe as storage_location_desc,
          bktxt as vehicle_num
      from l_dummy_withmt_tmp
      
""")
# where not (gstock=0 and dstock=0)

# COMMAND ----------

konv=spark.sql("select knumv,first(lifnr) as lifnr from {}.{} where kvsl1 in ('fre','FRE') group by knumv".format(curated_db_name,"KONV"))

# COMMAND ----------

lfa1 = spark.sql("select lifnr,name1 from {}.{}".format(curated_db_name,"lfa1"))

# COMMAND ----------

# adding vendor
final_1 = fact_tbr.alias('ft').join(konv.alias("konv"),fact_tbr["knumv"] == konv["knumv"],
                                              how='left').select(
  [col("ft." + cols) for cols in fact_tbr.columns] + [col("lifnr")])

# COMMAND ----------

final_2 = final_1.alias('f1').join(lfa1.alias("lfa1"),final_1["lifnr"] == lfa1["lifnr"],
                                              how='left').select(
  [col("f1." + cols) for cols in final_1.columns] + [col("name1")]
).drop("lifnr")

# COMMAND ----------

# writing data to processed layer before generating surrogate key
final_2.write.parquet(processed_location+'DEPOT_REJECTION', mode='overwrite')

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

#--------------------SURROGATE IMPLEMENTATION------------------------
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition_sl="\n left join {pro}.DIM_STORAGE_LOCATION on".format(pro=processed_db_name)
  join_condition=""
  count=0
  
  final_2.createOrReplaceTempView("{}".format(fact_name))
  
  for row_dim_fact_mapping in dim_fact_mapping:
    
    fact_table = row_dim_fact_mapping["fact_table"]
    dim_table = row_dim_fact_mapping["dim_table"]
    fact_surrogate = row_dim_fact_mapping["fact_surrogate"]
    dim_surrogate = row_dim_fact_mapping["dim_surrogate"]
    fact_column = row_dim_fact_mapping["fact_column"]
    dim_column = row_dim_fact_mapping["dim_column"]
    
    spark.sql("refresh table {processed_db_name}.{dim_table}".format(processed_db_name=processed_db_name,
                                                                   dim_table=dim_table))
  
    if( (fact_table_name==fact_table or fact_table_name.lower()==fact_table) and (dim_table!="DIM_STORAGE_LOCATION") ):
      
      count += 1
      tmp_unmapped_str = "CASE WHEN "
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull(A{count}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table,count=count)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} A{count} on {fact_table}.{fact_column} = A{count}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name,count=count)
    
    if((fact_table_name==fact_table or fact_table_name.lower()==fact_table) and dim_table=='DIM_STORAGE_LOCATION'):
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
    ,DIM_STORAGE_LOCATION.STORAGE_LOCATION_KEY AS STORAGE_LOCATION_KEY
    """    
  
  query = """select 
ack_num as ACK_NUM,
financial_year as FINANCIAL_YEAR,
good_stock as GOOD_STOCK,
quantity_in_tons as QUANTITY_MT,
defective_stock as DEFECTIVE_STOCK,
rejections_in_tons as REJECTION_MT,
rejection as REJECTION,
rejection_per as REJECTION_PERC,
vehicle_num as VEHICLE_NUM,
name1 as TRANSPORTER_DETAILS
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
d = surrogate_mapping_hil(csv_data,table_name,"DEPOT_REJECTION",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate key
d.write.mode('overwrite').parquet(processed_location+ table_name)

# COMMAND ----------

# insert log data into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
