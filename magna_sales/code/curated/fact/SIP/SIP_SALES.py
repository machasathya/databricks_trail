# Databricks notebook source
# import statements
from datetime import datetime 
from pyspark.sql.functions import *
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, lit, expr, to_date, to_timestamp, sum, min, when, count, concat,upper

# COMMAND ----------

# object creaion for processmetadatautility class
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
table_name = "FACT_SIP_SALES"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the code manually
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata_prod"
# curated_db_name = "cur_prod"
# db_url = "tcp:azr-hil-sql-srvr.database.windows.net"
# scope = "AZR-DBR-KV-SCOPE-300"
# processed_location = "mnt/bi_datalake/prod/pro/"
# processed_schema_name = "sales_semantic_prod"
# processed_db_name = "pro_prod"
# db = "bi_analytics"
# user_name = "hil-admin@azr-hil-sql-srvr"
# password = dbutils.secrets.get(scope, key = user_name.split("@")[0])
# table_name = "FACT_SIP_SALES"


# COMMAND ----------

# fetching surrogate meta from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading sip sales data from curated layer
sip_df = spark.sql("SELECT * FROM {}.{}".format(curated_db_name, "SIP_SALES")).drop("LAST_UPDATED_DT_TS")
sip_df = sip_df.withColumn("SAP_ID",lpad(col("SAP_ID"),10,'0'))

# COMMAND ----------

# reading customer master data from curated layer
kna1 = spark.sql("select kunnr,pstlz FROM {}.{}".format(curated_db_name, "KNA1"))

# COMMAND ----------

# joining customer master data with sip sales data to extract the pincode
sip_fin = sip_df.alias("sip").join(kna1.alias("kn"), on=[col("sip.SAP_ID") == col("kn.kunnr")], how='left').select([col("sip." + cols) for cols in sip_df.columns] + [col('pstlz')])

# COMMAND ----------

# aggregating data upon retailer and date level to find the first time sale customer 
first_cust = sip_df.select("RETAILER_ID","INVOICE_DATE").groupBy("RETAILER_ID").agg(min("INVOICE_DATE").alias("FIRST_TIME_SALES")).withColumn("FIRST_CUST_FLAG", lit(1))

# COMMAND ----------

#aggregaring data on customer date and product line level to fetch the first time sale customer at product line level
first_cust_prd = sip_df.select("RETAILER_ID","INVOICE_DATE","PRODUCT_LINE").groupBy("RETAILER_ID","PRODUCT_LINE").agg(min("INVOICE_DATE").alias("FIRST_TIME_SALES")).withColumn("FIRST_CUST_PRD_FLAG", lit(1))

# COMMAND ----------

# joining first cust and first cust prod dataframees to define first time sales flag
first_cust_prd_sales = first_cust.alias("f_cust").join(first_cust_prd.alias("f_cust_prd"), on = [first_cust["RETAILER_ID"] == first_cust_prd["RETAILER_ID"],
                                                                                                first_cust["FIRST_TIME_SALES"] == first_cust_prd["FIRST_TIME_SALES"]] , how = "full").selectExpr("f_cust.RETAILER_ID","f_cust.FIRST_TIME_SALES","f_cust.FIRST_CUST_FLAG as first_cust_flag","f_cust_prd.PRODUCT_LINE as prd_lines", "f_cust_prd.FIRST_CUST_PRD_FLAG as first_cust_prd_sales_flag", "0 as first_prd_sales_flag")

# COMMAND ----------

# generating final dataframe by filling nulls with 0 and selecting columns
final_sales = sip_fin.alias("s_union").join(first_cust_prd_sales.alias("f_cust_prd_sales") ,on = [col("s_union.RETAILER_ID") == col("f_cust_prd_sales.RETAILER_ID"),
                                                                                                     col("s_union.PRODUCT_LINE") == col("f_cust_prd_sales.prd_lines"),
                                                                                                     col("s_union.INVOICE_DATE") == col("f_cust_prd_sales.FIRST_TIME_SALES")],how = "left").select([col("s_union." + cols) for cols in sip_fin.columns] +[col("f_cust_prd_sales.first_cust_flag"),col("f_cust_prd_sales.first_cust_prd_sales_flag"),col("f_cust_prd_sales.first_prd_sales_flag")]).fillna({'first_cust_flag': 0,'first_prd_sales_flag': 0,'first_cust_prd_sales_flag': 0})

final_sales = final_sales.withColumn("SALE_TYPE",when(col("PRODUCT_LINE") == "PIPES & FITTINGS","RETAIL").otherwise("<DEFAULT>")).withColumnRenamed('PINCODE', 'RETAILER_PINCODE').withColumnRenamed('pstlz', 'CUSTOMER_PINCODE').withColumn("SALES_ORG_TYPE",when(col("SALE_TYPE")=="<DEFAULT>","OTHER").otherwise(col("SALE_TYPE")))

# COMMAND ----------

# changing column names to upper case
cols = []
for item in final_sales.columns:
  cols.append( "{} AS {}".format(item ,item.upper()))
final_sales = final_sales.selectExpr(cols)

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_sales.write.format("parquet").mode("overwrite").save(processed_location+"SIP_SALES")

# COMMAND ----------

# reading territory data from curated layer and flattening the entire data
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

#joining sip sales final dataframe with geo territory dataframe to get the district sales group state zone and country for generating geo key
final_sales_df = final_sales.alias("fact").join(final_geo_df.alias("Geo_data"), on = [
                                                                                    final_sales["product_line"] == final_geo_df["product_line"],
  final_sales["SALES_ORG_TYPE"] == final_geo_df["SALE_TYPE"],
  final_sales["CUSTOMER_PINCODE"] == final_geo_df["PINCODE"]
], how = 'left').select(
  [col("fact." + cols) for cols in final_sales.columns] + [col("Geo_data.DISTRICT"),col("Geo_data.SALES_GROUP"),col("Geo_data.STATE").alias("geo_state"),col("Geo_data.ZONE"),col("Geo_data.COUNTRY")])

# COMMAND ----------

#------------------SURROGATE IMPLEMENTATION-----------------

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  sur_col_list = []
  join_condition=""
  join_condition_fts="\n left join {pro}.DIM_FIRST_TIME_SALES on".format(pro=processed_db_name) #for first_time_sales
  count=0
  
  final_sales_df.createOrReplaceTempView("{}".format(fact_name))
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
      (({fact_name}.CUSTOMER_PINCODE = B.PINCODE) OR ({fact_name}.CUSTOMER_PINCODE is NULL AND B.PINCODE is NULL)) AND
      (({fact_name}.DISTRICT = B.DISTRICT) OR ({fact_name}.DISTRICT is NULL AND B.DISTRICT is NULL)) AND
      (({fact_name}.SALES_GROUP = B.SALES_GROUP) OR ({fact_name}.SALES_GROUP is NULL AND B.SALES_GROUP is NULL)) AND
      (({fact_name}.geo_state = B.STATE) OR ({fact_name}.geo_state is NULL AND B.STATE is NULL)) AND
      (({fact_name}.ZONE = B.ZONE) OR ({fact_name}.ZONE is NULL AND B.ZONE is NULL)) AND
      (({fact_name}.COUNTRY = B.COUNTRY) OR ({fact_name}.COUNTRY is NULL AND B.COUNTRY is NULL))
     """.format(pro_db=processed_db_name,fact_name=fact_name)
  
  select_condition = select_condition[:-2]
  join_condition += join_condition_fts
  
  select_condition = select_condition + """
    ,dim_first_time_sales.FIRST_TIME_SALES_KEY AS FIRST_TIME_SALES_KEY
    """
  query = """select
  SERIAL_NO
,AMOUNT
,QUANTITY
,RETAILER_ID
,OUTLET_NAME
,PRIMARY_MOBILE
,RETAILER_NAME
,FIRM_NAME
,SAP_ID
,CITY
,{fact_name}.STATE
,RETAILER_PINCODE
,CUSTOMER_PINCODE
,SCHEME
,CASE WHEN ({fact_name}.CUSTOMER_PINCODE is NULL OR 
            {fact_name}.product_line is NULL or 
            {fact_name}.DISTRICT is NULL or
            {fact_name}.SALES_GROUP is NULL or
            {fact_name}.geo_state is NULL or
            {fact_name}.ZONE is NULL or
            {fact_name}.COUNTRY is NULL or
            trim({fact_name}.product_line)='' or 
            trim({fact_name}.CUSTOMER_PINCODE) = '' or
            trim({fact_name}.DISTRICT)='' or
            trim({fact_name}.SALES_GROUP) = '' or
            trim({fact_name}.STATE) = '' or
            trim({fact_name}.ZONE) = '' or
            trim ({fact_name}.COUNTRY) = '') THEN -2 ELSE ifnull(B.GEO_KEY, -1) END as GEO_KEY,
   {select_condition}  from {fact_name} {join_condition}""".format(
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

# creating surroagte keys
d = surrogate_mapping_hil(csv_data,table_name,"SIP_SALES",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.format("parquet").mode("overwrite").save(processed_location+"FACT_SIP_SALES")

# COMMAND ----------

# inserting log record to last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
