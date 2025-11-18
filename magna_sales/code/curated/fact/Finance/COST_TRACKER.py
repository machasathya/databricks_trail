# Databricks notebook source
# Import Statements
import time
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql.types import DecimalType, IntegerType, DateType
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql import Window
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.functions import current_date, col
from pyspark.sql.functions import year, month, last_day

# COMMAND ----------

# Enabling Cross Join
spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

# Variables that are used to get and store the information coming from ADF Pipeline
from ProcessMetadataUtility import ProcessMetadataUtility
pmu = ProcessMetadataUtility()
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
table_name = "FACT_COST_TRACKER"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# Establishing connection to SQL DB to fetch the Surrogate key meta from fact_surrogate_meta table
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)


# COMMAND ----------

# from ProcessMetadataUtility import ProcessMetadataUtility
# pmu = ProcessMetadataUtility()
# meta_table = "fact_surrogate_meta"
# schema_name = "metadata"
# db_url = "tcp:hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# table_name = "FACT_COST_TRACKER"
# processed_location = "mnt/bi_datalake/dev/pro/"
# curated_db_name_dev= "cur_dev"
# curated_db_name = "cur_prod"
# processed_db_name = "pro_prod"
# processed_db_name_dev = "pro_dev"

# db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
# csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
# pmu.close(db_connector, db_cursor)


# COMMAND ----------

# Reading data from COST_TRACKER Table
df=spark.sql("select * from {}.{}".format(curated_db_name, "COST_TRACKER"))

# COMMAND ----------

# Modifying the names in PARTICULARS as required 
df = df.withColumn("PARTICULARS", when((col("PARTICULARS")=="C. Freight"),"Freight").when(col("PARTICULARS")=="D. Packing","Packing").when(col("PARTICULARS")=="I. Indigenous Raw Material","Indigenous Raw Material").when(col("PARTICULARS")=="H. Imported Raw Material","Imported Raw Material").when(col("PARTICULARS")=="K. Stores & Spares","Stores & Spares").when(col("PARTICULARS")=="L. Power & Fuel","Power & Fuel").when(col("PARTICULARS")=="M. Wages & benefits","Wages & benefits").when(col("PARTICULARS")=="N. General Repairs & Maintenance","General Repairs & Maintenance").when(col("PARTICULARS")=="O. Salary & benefits","Salary & benefits").when(col("PARTICULARS")=="P. Repairs to Building","Repairs to Building").when(col("PARTICULARS")=="Q. Insurance","Insurance").when(col("PARTICULARS")=="S. Communication Exp","Communication Exp").when(col("PARTICULARS")=="T. General Charges","General Charges").when(col("PARTICULARS")=="V. Rent, Rates & Taxes","Rent, Rates & Taxes").when(col("PARTICULARS")=="Y. Advertisement","Advertisement").when(col("PARTICULARS")=="U. Travelling & Conveyance","Travelling & Conveyance").when(col("PARTICULARS")=="Q. Vehicle Expenses","Vehicle Expenses").when(col("PARTICULARS")=="Production Volume","Production Volume").when(col("PARTICULARS")=="Despatched Volume","Despatched Volume"))

# COMMAND ----------

# dropping last updated details
df = df.drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

df = df.withColumn("SBU",lit("SBU 1"))

# COMMAND ----------

# Making list of plants and depots and 
column_plant = ['PLANT_1','PLANT_2','PLANT_3','PLANT_4','PLANT_5','PLANT_6','PLANT_7','PLANT_8','PLANT_9','PLANT_10','PLANT_11','PLANT_12','PLANT_13','PLANT_14','PLANT_15','PLANT_16','PLANT_17','PLANT_18','PLANT_19','PLANT_20','PLANT_21','PLANT_22','PLANT_23','PLANT_24','PLANT_25','PLANT_26','PLANT_27','PLANT_28','PLANT_29','PLANT_30','PLANT_31']


df_plant = df.drop('DEPOT_1','DEPOT_2','DEPOT_3','DEPOT_4','DEPOT_5','DEPOT_6','DEPOT_7','DEPOT_8','DEPOT_9','DEPOT_10','DEPOT_11','DEPOT_12','DEPOT_13','DEPOT_14','DEPOT_15','DEPOT_16','DEPOT_17','DEPOT_18','DEPOT_19','DEPOT_20','DEPOT_21','DEPOT_22','DEPOT_23','DEPOT_24','DEPOT_25','DEPOT_26','DEPOT_27','DEPOT_28','DEPOT_29','DEPOT_30','DEPOT_31')
 

pivot_df1 = df_plant.selectExpr('SBU','PARTICULARS', 'GL', 'UOM','PLANT_ID','PLANT_PREVIOUS_MONTH_TOTAL','DEPOT_PREVIOUS_MONTH_TOTAL', "stack({}, {})"
.format(len(column_plant), ', '.join(("'{}', {}".format(i, i) for i in column_plant))))
pivot_df1 = pivot_df1.withColumnRenamed("col0","DATE").withColumnRenamed("col1","METRIC_VALUE_PLANT")

# COMMAND ----------

column_depot =['DEPOT_1','DEPOT_2','DEPOT_3','DEPOT_4','DEPOT_5','DEPOT_6','DEPOT_7','DEPOT_8','DEPOT_9','DEPOT_10','DEPOT_11','DEPOT_12','DEPOT_13','DEPOT_14','DEPOT_15','DEPOT_16','DEPOT_17','DEPOT_18','DEPOT_19','DEPOT_20','DEPOT_21','DEPOT_22','DEPOT_23','DEPOT_24','DEPOT_25','DEPOT_26','DEPOT_27','DEPOT_28','DEPOT_29','DEPOT_30','DEPOT_31']

df_depot = df.drop('PLANT_1','PLANT_2','PLANT_3','PLANT_4','PLANT_5','PLANT_6','PLANT_7','PLANT_8','PLANT_9','PLANT_10','PLANT_11','PLANT_12','PLANT_13','PLANT_14','PLANT_15','PLANT_16','PLANT_17','PLANT_18','PLANT_19','PLANT_20','PLANT_21','PLANT_22','PLANT_23','PLANT_24','PLANT_25','PLANT_26','PLANT_27','PLANT_28','PLANT_29','PLANT_30','PLANT_31')

pivot_df2 = df_depot.selectExpr('SBU','PARTICULARS', 'GL', 'UOM','PLANT_ID', "stack({}, {})"
.format(len(column_depot), ', '.join(("'{}', {}".format(i, i) for i in column_depot))))
pivot_df2 = pivot_df2.withColumnRenamed("col0","Date").withColumnRenamed("col1","METRIC_VALUE_DEPOT")

# COMMAND ----------

b = pivot_df2.select("METRIC_VALUE_DEPOT")

# COMMAND ----------

pivot_df1 = pivot_df1.withColumn("x", row_number().over(Window.orderBy(monotonically_increasing_id())))
b = b.withColumn("x", row_number().over(Window.orderBy(monotonically_increasing_id())))

df_pre_final_1 = pivot_df1.join(b, pivot_df1.x == b.x).drop("x")

# COMMAND ----------

df_pre_final_1 = df_pre_final_1.withColumn("PLANT_PREVIOUS_MONTH_TOTAL",lit(None).cast(DecimalType(13,2))).withColumn("DEPOT_PREVIOUS_MONTH_TOTAL",lit(None).cast(DecimalType(13,2)))

# COMMAND ----------

df_ori_extra = df_pre_final_1
r = current_date()
df_ori_extra = df_ori_extra.withColumn("UNLESS",r)
split_date=sf.split(df_ori_extra['UNLESS'], '-')     
df_ori_extra= df_ori_extra.withColumn('Year', split_date.getItem(0))
df_ori_extra= df_ori_extra.withColumn('Month', split_date.getItem(1))


# COMMAND ----------

df_pre_final_1 = df_pre_final_1.withColumn("DATE", when((col("DATE")=="PLANT_1"),"2020-10-01").when(col("DATE")=="PLANT_2","2020-10-02").when(col("DATE")=="PLANT_3","2020-10-03").when(col("DATE")=="PLANT_4","2020-10-04").when(col("DATE")=="PLANT_5","2020-10-05").when(col("DATE")=="PLANT_6","2020-10-06").when(col("DATE")=="PLANT_7","2020-10-07").when(col("DATE")=="PLANT_8","2020-10-08").when(col("DATE")=="PLANT_9","2020-10-09").when(col("DATE")=="PLANT_10","2020-10-10").when(col("DATE")=="PLANT_11","2020-10-11").when(col("DATE")=="PLANT_12","2020-10-12").when(col("DATE")=="PLANT_13","2020-10-13").when(col("DATE")=="PLANT_14","2020-10-14").when(col("DATE")=="PLANT_15","2020-10-15").when(col("DATE")=="PLANT_16","2020-10-16").when(col("DATE")=="PLANT_17","2020-10-17").when(col("DATE")=="PLANT_18","2020-10-18").when(col("DATE")=="PLANT_19","2020-10-19").when(col("DATE")=="PLANT_20","2020-10-20").when(col("DATE")=="PLANT_21","2020-10-21").when(col("DATE")=="PLANT_22","2020-10-22").when(col("DATE")=="PLANT_23","2020-10-23").when(col("DATE")=="PLANT_24","2020-10-24").when(col("DATE")=="PLANT_25","2020-10-25").when(col("DATE")=="PLANT_26","2020-10-26").when(col("DATE")=="PLANT_27","2020-10-27").when(col("DATE")=="PLANT_28","2020-10-28").when(col("DATE")=="PLANT_29","2020-10-29").when(col("DATE")=="PLANT_30","2020-10-30").when(col("DATE")=="PLANT_31","2020-10-31"))
# display(df_pre_final_1)

# COMMAND ----------

df_previous = df.select("SBU",'PARTICULARS','GL','UOM','PLANT_ID','PLANT_PREVIOUS_MONTH_TOTAL','DEPOT_PREVIOUS_MONTH_TOTAL')
df_previous = df_previous.withColumn("METRIC_VALUE_PLANT",lit(None).cast(DecimalType(13,2))).withColumn("METRIC_VALUE_DEPOT",lit(None).cast(DecimalType(13,2)))
df_previous = df_previous.withColumn("DATE",lit("2020-09-30").cast(DateType()))

# COMMAND ----------

df_pre_final_1= df_pre_final_1.withColumn("DATE",df_pre_final_1['DATE'].cast(DateType()))

# COMMAND ----------

df_pre_final_1 = df_pre_final_1.select('SBU','PARTICULARS','GL','UOM','PLANT_ID','DATE','METRIC_VALUE_PLANT','METRIC_VALUE_DEPOT','PLANT_PREVIOUS_MONTH_TOTAL','DEPOT_PREVIOUS_MONTH_TOTAL')

df_previous = df_previous.select('SBU','PARTICULARS','GL','UOM','PLANT_ID','DATE','METRIC_VALUE_PLANT','METRIC_VALUE_DEPOT','PLANT_PREVIOUS_MONTH_TOTAL','DEPOT_PREVIOUS_MONTH_TOTAL')

# COMMAND ----------

df_final = df_pre_final_1.union(df_previous)

# COMMAND ----------

df_final = df_final.withColumn("PARTICULARS_NO", when((col("PARTICULARS")=="Freight"),lit(1)).when(col("PARTICULARS")=="Packing",lit(2)).when(col("PARTICULARS")=="Indigenous Raw Material",lit(3)).when(col("PARTICULARS")=="Imported Raw Material",lit(4)).when(col("PARTICULARS")=="Stores & Spares",lit(5)).when(col("PARTICULARS")=="Power & Fuel",lit(6)).when(col("PARTICULARS")=="Wages & benefits",lit(7)).when(col("PARTICULARS")=="General Repairs & Maintenance",lit(8)).when(col("PARTICULARS")=="Salary & benefits",lit(9)).when(col("PARTICULARS")=="Repairs to Building",lit(10)).when(col("PARTICULARS")=="Insurance",lit(11)).when(col("PARTICULARS")=="Communication Exp",lit(12)).when(col("PARTICULARS")=="General Charges",lit(13)).when(col("PARTICULARS")=="Rent, Rates & Taxes",lit(14)).when(col("PARTICULARS")=="Advertisement",lit(15)).when(col("PARTICULARS")=="Travelling & Conveyance",lit(16)).when(col("PARTICULARS")=="Vehicle Expenses",lit(17)).when(col("PARTICULARS")=="Production Volume",lit(18)).when(col("PARTICULARS")=="Despatched Volume",lit(19)))

# COMMAND ----------

df_final.write.parquet(processed_location+'COST_TRACKER', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
  df_final.createOrReplaceTempView("{}".format(fact_name))
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
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull(A{count}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table,count=count)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} A{count} on {fact_table}.{fact_column} = A{count}.{dim_column} ".format(
            dim_table=dim_table, count=count,fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)    
  select_condition = select_condition[:-2]      
  query = """select
   SBU,
   PARTICULARS,
   GL,
   UOM,
   PARTICULARS_NO,
   METRIC_VALUE_PLANT,
   METRIC_VALUE_DEPOT,
   PLANT_PREVIOUS_MONTH_TOTAL,
   DEPOT_PREVIOUS_MONTH_TOTAL
  ,{select_condition}  from {fact_name} {join_condition}
  """.format(
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

d = surrogate_mapping_hil(csv_data,table_name,"COST_TRACKER",processed_db_name)

# COMMAND ----------

d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
