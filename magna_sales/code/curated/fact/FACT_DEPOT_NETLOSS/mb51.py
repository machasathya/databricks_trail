# Databricks notebook source
#Import Statements
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date
from ProcessMetadataUtility import ProcessMetadataUtility

# COMMAND ----------

#Timeout in seconds for the broadcast wait time in broadcast joins
spark.conf.set("spark.sql.broadcastTimeout", 36000)

# COMMAND ----------

# Variables that are used to get and store the information coming from ADF Pipeline
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
table_name = "FACT_LOGISTICS_DEPOT_NETLOSS"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# Variables to run the notebook manually
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
# table_name = "FACT_LOGISTICS_DEPOT_NETLOSS"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# Establishing connection to SQL DB to fetch the Surrogate key meta from fact_surrogate_meta table
pmu = ProcessMetadataUtility()
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# Reading data from MSEG Table
MSEG = spark.sql("select * FROM {}.{}".format(curated_db_name, "MSEG")).select("MATNR", "MANDT", "WERKS","LGORT","BWART", "SOBKZ", "MBLNR", "MJAHR", "ZEILE", "ERFMG", "ERFME", "LIFNR")

# COMMAND ----------

#Reading data from MKPF Table
MKPF = spark.sql("select * FROM {}.{}".format(curated_db_name, "MKPF")).select("BUDAT","VGART", "MBLNR", "MJAHR","MANDT")

# COMMAND ----------

# Reading MARM data for records which contains KG as an alternate UoM
marm_kg = spark.sql("select * FROM {}.{}".format(curated_db_name, "MARM")).where(col("MEINH").isin(["KG"]))

# Reading MARM data for records which contains M3 as an alternate UoM - To handle conversion value of BLOCK
marm_m3 = spark.sql("select * from {}.{}".format(curated_db_name,"MARM")).where(col("meinh").isin(["M3"]))

# COMMAND ----------

# Joining Material Movement Item table with header table and filtering out required data
mast_df = MKPF.alias('mk').join(MSEG.alias('ms'), on=[
                                               col('mk.MBLNR') == col('ms.MBLNR'),
                                               col('mk.MJAHR') == col('ms.MJAHR'),
                                               col('mk.MANDT') == col('ms.MANDT')],
                            how='inner').select([col("ms."+cols) for cols in MSEG.columns] + [col("BUDAT")]).where((col("BWART").isin(["Z61", "Z62","Z63","Z64","655","656"]))| (((col("BWART").isin(["261","262"])) & col("LGORT").isin(["0081"])) | ((col("BWART").isin(["531","532"])) & col("LGORT").isin(["0061"])))).drop("MANDT")

# COMMAND ----------

# Fetching conversion values for both MT/M3 seperately and doing an union
df_kg = mast_df.where("werks not in (2010,2013,2018,2022)").join(marm_kg, on=['MATNR'], how='left').select([col(cols) for cols in mast_df.columns] +[col("UMREZ"),col("UMREN")])
df_m3 = mast_df.where("werks in (2010,2013,2018,2022) ").join(marm_m3, on=['MATNR'], how='left').select([col(cols) for cols in mast_df.columns] +[col("UMREZ"),col("UMREN")])
df = df_kg.union(df_m3)

# COMMAND ----------

# changing column names
df1 = df.alias('ms').select([col("ms.MATNR").alias("matl_key"), col("ms.WERKS").alias("plant_key"), col("ms.LGORT").alias("storage_loc"),  col("ms.BWART").alias("movement_type"), col("ms.SOBKZ").alias("special_stock"), col("ms.MBLNR").alias("matl_doc"), col("ms.MJAHR").alias("matl_doc_year"), col("ms.ZEILE").alias("doc_item"), col("ms.ERFMG").alias("qty_key"), col("ms.ERFME").alias("UoE"), col("ms.LIFNR").alias("vendor_info"), col("ms.BUDAT").alias("post_date_key"), col("ms.UMREZ"),col("ms.UMREN")])

# calculating conversion values
df2 = df1.selectExpr("matl_key", "plant_key", "storage_loc","movement_type","special_stock","matl_doc","matl_doc_year","doc_item","qty_key","UoE","vendor_info","post_date_key", "UMREZ", "UMREN","(UMREN/UMREZ) as unit_weights", "((UMREN/UMREZ) * qty_key) as total_unit_weights", "(((UMREN/UMREZ) * qty_key)/1000) as weights_mt")
 
# Selecting final columns that are required
df3_1 = df2.selectExpr("matl_key", "plant_key", "storage_loc","movement_type","matl_doc","matl_doc_year","doc_item","qty_key","UoE","post_date_key","unit_weights", "total_unit_weights","weights_mt")

# Adding cubic meter value for blocks plants and MT values for remianing plants
df3 = df3_1.withColumn("weights_mt",when((col("UoE") == 'M3'),col("qty_key")).when((col("plant_key").isin(['2010','2013','2018','2022'])),(col("total_unit_weights"))).otherwise(col("weights_mt")))

# COMMAND ----------

# calculating depot netloss value 
df4 = df3.withColumn("depot_net_loss_mt", when((col("movement_type").isin(["Z61","Z64"])), (col("weights_mt"))).when(col("movement_type").isin(["Z62","Z63"]),- col("weights_mt")))

# COMMAND ----------

#deriving Plant Netloss Quantity for Non Blocks Plants
df5 = df4.withColumn("plant_net_loss_mt", when(((col("storage_loc").isin(["0081","0061"])) & ((col("plant_key").isin(['2010','2013','2018','2022'])) == False) & (col("movement_type").isin(["262","531"]))),-(col("weights_mt")))
                          .when(((col("storage_loc").isin(["0081","0061"])) & ((col("plant_key").isin(['2010','2013','2018','2022'])) == False) & (col("movement_type").isin(["261","532"]))),(col("weights_mt"))))

#Deriving Plant Netloss Quantity for Blocks Plants
plant_df  = df5.withColumn("plant_net_loss_mt", when(((col("storage_loc").isin(["0081","0061"])) & (col("plant_key").isin(['2010','2013','2018','2022'])) & (col("movement_type").isin(["262","531"]))),(col("weights_mt")))
                          .when(((col("storage_loc").isin(["0081","0061"])) & (col("plant_key").isin(['2010','2013','2018','2022'])) & (col("movement_type").isin(["261","532"]))),-(col("weights_mt"))).otherwise(col("plant_net_loss_mt")))

# COMMAND ----------

#Reading Plant and Material Master data from Processed layer
plant_mast = spark.sql("select * from {}.{}".format(processed_db_name,"DIM_PLANT_DEPOT"))
matl_mast = spark.sql("select * from {}.{}".format(processed_db_name,"DIM_PRODUCT"))

# COMMAND ----------

#Doing a left join plant_df dataframe with plant mast to bring in the sales org against plant
final_1 = plant_df.alias('df1').join(plant_mast, on=[ col('plant_id') == col('df1.plant_key')],
                            how='left').select([col("df1."+cols) for cols in plant_df.columns] + [col("is_plant"),col("sales_org").alias("plant_sales_org")])

# COMMAND ----------

#Deriving Sent Reclamation quantity for both plants and depots. if is_plant = 0 then it denotes as depot, is_plant = 1 then it denotes as plant.
sent_recl_df = final_1.withColumn("SENT_RECLAMATION_QTY_MT",
                                   when(((col("is_plant") == 0) & (col("movement_type") == "Z61")),col("weights_mt"))
                                   .when(((col("is_plant") == 0) & (col("movement_type") == "Z62")),-(col("weights_mt")))
                                   .when(((col("is_plant") == 1) & ((col("plant_key").isin(['2010','2013','2018','2022'])) == False) & (col("movement_type") == "261")),col("weights_mt"))
                                   .when(((col("is_plant") == 1) & ((col("plant_key").isin(['2010','2013','2018','2022'])) == False) & (col("movement_type") == "262")),-(col("weights_mt"))))

# COMMAND ----------

#Deriving recovered Reclamation quantity for both plants and depots. if is_plant = 0 then it denotes as depot, is_plant = 1 then it denotes as plant.
rec_recl_df = sent_recl_df.withColumn("RECEIVED_RECLAMATION_QTY_MT",
                                   when(((col("is_plant") == 0) & (col("movement_type") == "Z63")),col("weights_mt"))
                                   .when(((col("is_plant") == 0) & (col("movement_type") == "Z64")),-(col("weights_mt")))
                                   .when(((col("is_plant") == 1) & (col("movement_type") == "531")),col("weights_mt"))
                                   .when(((col("is_plant") == 1) & (col("movement_type") == "532")),-(col("weights_mt"))))

# COMMAND ----------

#Deriving Market Netloss
market_netloss_df = rec_recl_df.withColumn("MARKET_NETLOSS",when((col('movement_type') == '655'),col('weights_mt')).when(col('movement_type') == '656',-(col('weights_mt'))))

# COMMAND ----------

#Creating new column called SUPPLYING PLANT which is used in SYSTEM NETLOSS Report.
final1 = market_netloss_df.withColumn("SUPPLYING_PLANT",when(((col("is_plant") == 0) & (col("storage_loc").isin(["2001","8101"]))),"2001")
                                      .when(((col("is_plant") == 0) & (col("storage_loc").isin(["2003","8103"]))),"2003")
                                      .when(((col("is_plant") == 0) & (col("storage_loc").isin(["2004","8104"]))),"2004")
                                      .when(((col("is_plant") == 0) & (col("storage_loc").isin(["2005","8105"]))),"2005")
                                      .when(((col("is_plant") == 0) & (col("storage_loc").isin(["2009","8109"]))),"2009")
                                      .when(((col("is_plant") == 0) & (col("storage_loc").isin(["2012","8112"]))),"2012")
                                     )

# COMMAND ----------

# Doing a left join of final1 dataframe with material master on material number and fetching material type.
final2 = final1.alias("l_df").join(matl_mast.alias("r_df"),on=[col("r_df.material_number") == col("l_df.matl_key")],how='left').select([col("l_df."+cols) for cols in final1.columns] + [col("material_type")])

# COMMAND ----------

#Exlcusion of plant netloss value for records whose material id's belongs to scrap and sales org is 2000
final3 = final2.withColumn("plant_net_loss_mt",when(((col("plant_sales_org") == '2000')&(col("material_type") == 'ZSCR')),lit(0)).otherwise(col("plant_net_loss_mt"))).withColumn("RECEIVED_RECLAMATION_QTY_MT",when(((col("plant_sales_org") == '2000')&(col("material_type") == 'ZSCR')),lit(0)).otherwise(col("RECEIVED_RECLAMATION_QTY_MT"))).withColumn("SENT_RECLAMATION_QTY_MT",when(((col("plant_sales_org") == '2000')&(col("material_type") == 'ZSCR')),lit(0)).otherwise(col("SENT_RECLAMATION_QTY_MT")))

#Exclusion of plant netloss value for records belongs to Blocks plants and Unit of Entry is not equals to M3
final4 = final3.withColumn("plant_net_loss_mt",when((col("plant_key").isin(['2010','2013','2018','2022'])) & (col("UoE") == 'M3'),col("plant_net_loss_mt"))
                          .when((col("plant_key").isin(['2010','2013','2018','2022'])) & (col("UoE") != 'M3'),lit(0))
                          .otherwise(col("plant_net_loss_mt")))

#Exclsuion of sent reclamation quantity for records belongs to blocks plants and unit of entry not equals to EA
final5 = final4.withColumn("SENT_RECLAMATION_QTY_MT",when((col("plant_key").isin(['2010','2013','2018','2022'])) & (col("UoE") == 'EA'),col("WEIGHTS_MT"))
                          .when((col("plant_key").isin(['2010','2013','2018','2022'])) & (col("UoE") != 'EA'),lit(0))
                          .otherwise(col("SENT_RECLAMATION_QTY_MT")))

#Logic for adding of negation sign in sent reclamation quantity value for reversal movement types of Blocks plants
final = final5.withColumn("SENT_RECLAMATION_QTY_MT",when(((col("storage_loc").isin(["0081","0061"])) & (col("plant_key").isin(['2010','2013','2018','2022'])) & (col("movement_type").isin(["262","531"]))),-(col("SENT_RECLAMATION_QTY_MT")))
                          .when(((col("storage_loc").isin(["0081","0061"])) & (col("plant_key").isin(['2010','2013','2018','2022'])) & (col("movement_type").isin(["261","532"]))),(col("SENT_RECLAMATION_QTY_MT"))).otherwise(col("SENT_RECLAMATION_QTY_MT")))

# COMMAND ----------

#writing the final dataframe data to processed location before implementing surrogate key
final.write.parquet(processed_location+'MB51', mode='overwrite')

# COMMAND ----------

#--------------------SURROGATE KEY IMPLEMENTATION------------------------
# From the surrogate table, we will be mapping the columns(which have keys) to the columns in the final table
# All the columns such as Data, Plant, Material will be changed to distinct keys
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition_sl="\n left join {pro}.DIM_STORAGE_LOCATION on".format(pro=processed_db_name)
  join_condition=""
  count=0
  
  final.createOrReplaceTempView("{}".format(fact_name))
  
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
      
      for frow,drow in zip(fact_column_temp,dim_column_temp):
  
        join_condition_sl += """\n {fact_name}.{frow} = {dim_table}.{drow} AND""".format(fact_name=fact_name,dim_table=dim_table,frow=frow,drow=drow)
      join_condition_sl = join_condition_sl[:-3]
      
  select_condition = select_condition[:-2]   
  join_condition += join_condition_sl
  
  select_condition = select_condition + """
    ,DIM_STORAGE_LOCATION.STORAGE_LOCATION_KEY AS STORAGE_LOCATION_KEY
    """    
  query = """select 
 matl_doc as MATL_DOC
,matl_doc_year as MATL_DOC_YEAR
,doc_item as DOC_ITEM
,movement_type as MOVEMENT_TYPE
,qty_key as QUANTITY
,unit_weights as UNIT_WEIGHTS
,total_unit_weights as TOTAL_UNIT_WEIGHTS
,weights_mt as WEIGHTS_MT
,depot_net_loss_mt as DEPOT_NETLOSS_MT
,plant_net_loss_mt as PLANT_NETLOSS_MT
,SENT_RECLAMATION_QTY_MT
,RECEIVED_RECLAMATION_QTY_MT
,MARKET_NETLOSS
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

# Calling surrogate key function 
# Table name contains the name of final fact table name
# processed_db_name - Processed Database where the table with surrogate columns are stored(after converting)
d = surrogate_mapping_hil(csv_data,table_name,"MB51", processed_db_name)

# COMMAND ----------

#writing the final transformed dataframe data to processed location with surrogate keys
d.write.mode('overwrite').parquet(processed_location + table_name)

# COMMAND ----------

#updating LAST_EXECUTION_DETAILS table in SQL
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
