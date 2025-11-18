# Databricks notebook source
# import statements
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, split, concat, when, count, concat,upper
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import functions as F
import pandas as pd
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
spark.conf.set("spark.sql.broadcastTimeout",  36000)

# COMMAND ----------

# class object definition
pmu = ProcessMetadataUtility()

# COMMAND ----------

# code to fetch the data from ADF prameters
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
table_name = "FACT_COST_COLLECTIONS"
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
# table_name = "FACT_COST_COLLECTIONS"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
bkpf = spark.sql("select bukrs,belnr,gjahr,blart,bldat,budat,xblnr,bktxt,waers,kursf,cpudt,stblg,stjah from {}.{} ".format(curated_db_name,'bkpf'))

lt_bkpf = spark.sql("select bukrs,belnr,gjahr,blart,bldat,budat,xblnr,bktxt,waers,kursf,cpudt,stblg,stjah from {}.{} where bukrs='1000' and blart IN ('DZ','Z3') ".format(curated_db_name,"BKPF"))
ltmp_bkpf = lt_bkpf.where("(stblg is not null and trim(stblg) <> '')").where("trim(blart) <> 'Z3'")


it_bkpf = ltmp_bkpf.alias("l_df").join(bkpf.alias("r_df"),on=[col("l_df.stblg") == col('r_df.belnr'),
                                                             col("l_df.stjah") == col("r_df.gjahr")]).select([col("r_df." + cols) for cols in bkpf.columns])

fin_bkpf = lt_bkpf.union(it_bkpf)

fin_bkpf = fin_bkpf.dropDuplicates(["bukrs","belnr","gjahr"])


# COMMAND ----------

# DBTITLE 0,BSEG Table
# reading data from curated layer
bseg_df = spark.sql("""SELECT bukrs,belnr,gjahr,bschl,umskz,gsber,mwskz,dmbtr,shkzg,zuonr,sgtxt,kostl,aufnr,vbel2,hkont,kunnr,zfbdt,ebeln,prctr,bupla,secco,projk,fipos FROM {}.{} where koart='D'""".format(curated_db_name,"bseg"))

# COMMAND ----------

# DBTITLE 0,KNA1 Table
# reading data from curated layer
cust_df = spark.sql("""SELECT * FROM {}.{} """.format(processed_db_name,"dim_customer"))

# COMMAND ----------

# reading data from curated layer
tvkbt_df = spark.sql("""SELECT * FROM {}.{} where SPRAS='E'""".format(curated_db_name,"tvkbt"))

# COMMAND ----------

# reading data from curated layer
knvv_df = spark.sql("select kunnr,min(vkbur) as vkbur from {}.{} group by kunnr".format(curated_db_name,"KNVV"))

# COMMAND ----------

#joining bkpf and bseg and fetching  req columns
lt_bseg1 = fin_bkpf.alias('l_df').join(bseg_df.alias("r_df"),on=[col("l_df.bukrs") == col("r_df.bukrs"),
                                                                 col("l_df.belnr") == col("r_df.belnr"),
                                                                col("l_df.gjahr") == col("r_df.gjahr")],how='left').select(["l_df.belnr","l_df.gjahr","kunnr","budat","DMBTR","shkzg"])

# handling debit credit indicator
lt_bseg = lt_bseg1.withColumn("AMOUNT",when(col("shkzg") == 'H',-(col("DMBTR"))).otherwise(col("DMBTR")))

# fetching SBU and product line
lt_cust = lt_bseg.alias("l_df").join(cust_df.alias("r_df"),on=[col("l_df.kunnr") == col("r_df.cust_id")],how='left').select([col("l_df." + cols) for cols in lt_bseg.columns]+ [col("POSTAL_CODE"),col("OUTSTANDING_SBU"),col("OUTSTANDING_PRODUCT_LINE")])


lt_knvv = lt_cust.alias("l_df").join(knvv_df.alias("r_df"),on = [col('l_df.kunnr') == col("r_df.kunnr")],how='left').select([col("l_df." + cols) for cols in lt_cust.columns]+ [col("vkbur")])


lt_tvkbt = lt_knvv.alias("l_df").join(tvkbt_df.alias("r_df"),on = [col('l_df.VKBUR') == col("r_df.VKBUR")],how='left').select([col("l_df." + cols) for cols in lt_knvv.columns]+ [col("BEZEI")])

# deriving sale type
lt_final = lt_tvkbt.withColumn("SALES_TYPE",when(col("BEZEI").like("%Export%"),"EXPORTS")
                               .when(col("BEZEI").like("%Project%"),"PROJECT")
                               .when(col("BEZEI").like("%South%"),"RETAIL")
                               .when(col("BEZEI").like("%North%"),"RETAIL")
                               .when(col("BEZEI").like("%East%"),"RETAIL")
                               .when(col("BEZEI").like("%West%"),"RETAIL").otherwise("<DEFAULT>"))

lt_sales_org = lt_final.withColumn("SALES_ORG",when(col("OUTSTANDING_SBU")=="SBU 1","1000").when(col("OUTSTANDING_SBU")=="SBU 2","2000").when(col("OUTSTANDING_SBU")=="SBU 3","3000").when(col("OUTSTANDING_SBU")=="SBU 4","4000").when(col("OUTSTANDING_SBU")=="SBU 5","5000").when(col("OUTSTANDING_SBU")=="SBU 6","6000").otherwise(col("OUTSTANDING_SBU")))

final_fact_df = lt_sales_org.selectExpr("belnr as DOCUMENT_NO","gjahr as YEAR","budat as POSTING_DATE","KUNNR as CUST_ID","AMOUNT","POSTAL_CODE","OUTSTANDING_SBU","OUTSTANDING_PRODUCT_LINE","SALES_ORG","SALES_TYPE")

# COMMAND ----------

# reading territory data from curated layer and flattening the hierarchy
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

# joining geo data with final fact data 
final_df = final_fact_df.alias("fact").join(final_geo_df.alias("Geo_data"), on = [
                                                                  final_fact_df["SALES_ORG"] == final_geo_df["SBU"],
                                                                                    final_fact_df["OUTSTANDING_PRODUCT_LINE"] == final_geo_df["product_line"],
  final_fact_df["SALES_TYPE"] == final_geo_df["SALE_TYPE"],
  final_fact_df["POSTAL_CODE"] == final_geo_df["PINCODE"]
], how = 'left').select(
  [col("fact." + cols) for cols in final_fact_df.columns] + [col("DISTRICT"),col("SALES_GROUP"),col("STATE"),col("ZONE"),col("COUNTRY")])

# COMMAND ----------

# writing data to processed location before generating surrogate keys
final_df.write.parquet(processed_location+'COST_COLLECTIONS', mode='overwrite')

# COMMAND ----------

#SURROGATE KEY IMPLEMENTATION FOR FACT_SALES_TARGETS - INCLUDING LOGIC FOR GEO-KEY--

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
  
    if(fact_table_name==fact_table or fact_table_name.lower()==fact_table ):
      
      count += 1
      tmp_unmapped_str = "CASE WHEN "
      tmp_unmapped_str += "(A.{fact_column} is NULL OR trim(A.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull({dim_table}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
          
      join_condition += "\n left join {pro_db}.{dim_table} on A.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)
  join_condition = join_condition + """ \n
  left join {pro_db}.dim_territory B
  ON A.OUTSTANDING_PRODUCT_LINE = B.PRODUCT_LINE AND
     A.SALES_ORG = B.SALES_ORG AND
     A.SALES_TYPE = B.SALE_TYPE AND
     A.POSTAL_CODE = B.PINCODE AND
     A.DISTRICT = B.DISTRICT AND
     A.SALES_GROUP = B.SALES_GROUP AND
      A.STATE = B.STATE AND
      A.ZONE = B.ZONE AND
      A.COUNTRY = B.COUNTRY
     """.format(pro_db=processed_db_name)
  
  select_condition = select_condition[:-2]
  query = """select 
    A.DOCUMENT_NO,
    A.YEAR,
    A.AMOUNT,
    case when( 
     A.OUTSTANDING_PRODUCT_LINE is NULL OR
     A.SALES_ORG is NULL OR
     A.SALES_TYPE is NULL OR
     A.POSTAL_CODE is NULL OR
     A.DISTRICT is NULL OR
     A.SALES_GROUP is NULL OR
     A.STATE is NULL OR
     A.ZONE is NULL OR
     A.COUNTRY is NULL OR
     trim(A.OUTSTANDING_PRODUCT_LINE)='' OR
     trim(A.SALES_ORG)='' OR
     trim(A.SALES_TYPE)='' OR
     trim(A.POSTAL_CODE)='' OR
     trim(A.DISTRICT)='' OR
     trim(A.SALES_GROUP)=''  OR
     trim(A.STATE)=''  OR
     trim(A.ZONE)='' OR
     trim(A.COUNTRY)='') THEN -2 ELSE ifnull(B.GEO_KEY, -1) END as GEO_KEY
  ,{select_condition}  from {fact_name} A {join_condition}""".format(
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
d = surrogate_mapping_hil(csv_data,table_name,"COST_COLLECTIONS",processed_db_name)

# COMMAND ----------

# writing data to processed location after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# insert log record into last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
