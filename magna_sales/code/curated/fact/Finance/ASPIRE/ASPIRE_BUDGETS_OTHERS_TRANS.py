# Databricks notebook source
# Import Statements
import time
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,trim
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility
from pyspark.sql.types import DecimalType, IntegerType, DateType
from pyspark.sql.functions import monotonically_increasing_id, row_number, from_unixtime, unix_timestamp, to_date, first, count,concat,substring_index, upper, col, last_day
from pyspark.sql import Window
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.functions import current_date, col
from pyspark.sql.functions import year, month, last_day
import pandas as pd
pmu = ProcessMetadataUtility()

# COMMAND ----------

# Enabling Cross Join
spark.conf.set("spark.sql.crossJoin.enabled", "true")

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
table_name = "FACT_ASPIRE_BUDGETS_OTHERS_TRANS"
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
# table_name = "FACT_ASPIRE_BUDGETS_OTHERS_TRANS"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading surrogate metadata form SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
df=spark.sql("select * from {}.{}".format(curated_db_name, "ASPIRE_BUDGETS_OTHERS")).drop(*["LAST_UPDATED_DT_TS"])

# COMMAND ----------

# Summing up the record values for each month
sum1_df = df.agg(sf.sum('APRIL').alias('APRIL'),sf.sum('MAY').alias('MAY'),sf.sum('JUNE').alias('JUNE'),sf.sum('JULY').alias('JULY'),sf.sum('AUGUST').alias('AUGUST'),sf.sum('SEPTEMBER').alias('SEPTEMBER'),sf.sum('OCTOBER').alias('OCTOBER'),sf.sum('NOVEMBER').alias('NOVEMBER'),sf.sum('DECEMBER').alias('DECEMBER'),sf.sum('JANUARY').alias('JANUARY'),sf.sum('FEBRUARY').alias('FEBRUARY'),sf.sum('MARCH').alias('MARCH'))

num1_df = sum1_df.withColumn("MONTH",when(col('MARCH').isNotNull(),lit(16)).when(col('FEBRUARY').isNotNull(),lit(15)).when(col('JANUARY').isNotNull(),lit(14)).when(col('DECEMBER').isNotNull(),lit(13)).when(col('NOVEMBER').isNotNull(),lit(12)).when(col('OCTOBER').isNotNull(),lit(11)).when(col('SEPTEMBER').isNotNull(),lit(10)).when(col('AUGUST').isNotNull(),lit(9)).when(col('JULY').isNotNull(),lit(8)).when(col('JUNE').isNotNull(),lit(7)).when(col('MAY').isNotNull(),lit(6)).when(col('APRIL').isNotNull(),lit(5)))
rm_col = num1_df.select('MONTH').collect()[0][0]

trans_df = df.select(df.columns[:rm_col])

# COMMAND ----------

# extracting columns other than months
pivot_col = [c for c in trans_df.columns if c not in {"PLANT_ID","GROUP","PARTICULARS","FINANCIAL_YEAR"}]

# COMMAND ----------

# Pivot Transformations and adding Date to all the records
pivot_df = trans_df.selectExpr("PLANT_ID","GROUP","PARTICULARS","FINANCIAL_YEAR", "stack({}, {})".format(len(pivot_col), ', '.join(("'{}', {}".format(i, i) for i in pivot_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","TARGET")

# COMMAND ----------

# converting FY and month to start date of each calendar month
df1 = pivot_df.withColumn('MONTH_NUMBER', sf.date_format(to_date(col('MONTH'), 'MMMMM'), 'MM'))
df_yr = df1.withColumn('YEAR',when((col("MONTH_NUMBER")  == '01') | (col("MONTH_NUMBER")  == '02') |(col("MONTH_NUMBER")  == '03'),df1["FINANCIAL_YEAR"]).otherwise((df["FINANCIAL_YEAR"].cast(IntegerType())) - 1))
df2 = df_yr.withColumn('DATE_STR',sf.concat(sf.col('YEAR'),sf.lit('-'), sf.col('MONTH_NUMBER'),sf.lit('-01')))
df3 = df2.withColumn("DATE",df2['DATE_STR'].cast(DateType()))

# COMMAND ----------

# generating date range
l = pd.date_range(start="2020-04-01",end=df3.agg({'DATE':'max'}).collect()[0][0]).to_pydatetime().tolist()
df = spark.createDataFrame(l, DateType())
date_df = df.withColumn("MON_NUM",sf.date_format(to_date(col('value'), 'MMMMM'), 'MM'))
date_df = date_df.withColumn("YR_NUM",sf.date_format(to_date(col('value'),'YYYY'),'YYYY'))

# COMMAND ----------

# duplicating data for all days in the month
df4 = df3.alias("l_df").join(date_df.alias("r_df"),on=[col("l_df.MONTH_NUMBER") == col("r_df.MON_NUM"),
                                                               col("l_df.YEAR") == col("r_df.YR_NUM")],how='left').select(["l_df."+ i for i in df3.columns]+[col("r_df.value")]).drop(*["FINANCIAL_YEAR","MONTH","MONTH_NUMBER","YEAR","DATE_STR","DATE"]).withColumnRenamed("value","DATE").where("TARGET is not NULL")

# COMMAND ----------

# changinf column names for pivot
df5 = df4.withColumn("PIVOT_COL",when(((trim(col("GROUP")) == "POWER") & (trim(col("PARTICULARS")) == "No. of Units/Cu.M or Units/MT")),"POWER_NO_OF_UNITS_MT_OR_CUM")
                     .when(((trim(col("GROUP")) == "POWER") & (trim(col("PARTICULARS")) == "Avg. Variable Cost/Unit")),"POWER_AVG_VARIABLE_COST_UNIT")
                     .when(((trim(col("GROUP")) == "POWER") & (trim(col("PARTICULARS")) == "Cost (INR/Cu.M or INR/MT)")),"POWER_COST_MT_OR_CUM")
                     .when(((trim(col("GROUP")) == "BRIQUETTE / COAL / BIOMASS") & (trim(col("PARTICULARS")) == "No. of KG/Cu.M or KG/MT")),"BRIQUETTE_COAL_BIOMASS_NO_OF_KG_MT_OR_CUM")
                     .when(((trim(col("GROUP")) == "BRIQUETTE / COAL / BIOMASS") & (trim(col("PARTICULARS")) == "Avg. Cost (INR/KG)")),"BRIQUETTE_COAL_BIOMASS_AVG_COST_INR_KG")
                     .when(((trim(col("GROUP")) == "COAL") & (trim(col("PARTICULARS")) == "Cess Charges (INR/MT)")),"COAL_CESS_CHARGES_INR_MT_OR_CUM")
                     .when(((trim(col("GROUP")) == "FURNACE OIL") & (trim(col("PARTICULARS")) == "No. of L/Cu.M or L/MT")),"FURNACE_OIL_NO_OF_L_MT_OR_CUM")
                     .when(((trim(col("GROUP")) == "FURNACE OIL") & (trim(col("PARTICULARS")) == "Avg. Cost (INR/L)")),"FURNACE_OIL_AVG_COST_INR_L")
                     .when(((trim(col("GROUP")) == "BRIQUETTE / COAL / BIOMASS") & (trim(col("PARTICULARS")) == "Cost (INR/Cu.M or INR/MT)")),"BRIQUETTE_COAL_BIOMASS_COST_INR_MT_OR_CUM")
                     .when(((trim(col("GROUP")) == "NET LOSS %") & (trim(col("PARTICULARS")) == "NET LOSS %")),"NET_LOSS_PERCENTAGE")
                     .when(((trim(col("GROUP")) == "BRIQUETTE / COAL / BIOMASS") & (trim(col("PARTICULARS")) == "O & M Charges (INR)")),"BRIQUETTE_COAL_BIOMASS_O_M_CHARGES")
                     .when(((trim(col("GROUP")) == "PRODUCTION") & (trim(col("PARTICULARS")) == "PRODUCTION")),"PRODUCTION"))

# COMMAND ----------

# pivot on plant and date
pivot_trans_df = df5.groupby('PLANT_ID','DATE').pivot('PIVOT_COL').sum('TARGET')

# COMMAND ----------

# writing data to processed location before generating surrogate keys
pivot_trans_df.write.parquet(processed_location+'ASPIRE_BUDGETS_OTHERS_TRANS', mode='overwrite')

# COMMAND ----------

#SURROGATE IMPLEMENTATION
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
  pivot_trans_df.createOrReplaceTempView("{}".format(fact_name))
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
 BRIQUETTE_COAL_BIOMASS_AVG_COST_INR_KG,
BRIQUETTE_COAL_BIOMASS_COST_INR_MT_OR_CUM,
BRIQUETTE_COAL_BIOMASS_NO_OF_KG_MT_OR_CUM,
BRIQUETTE_COAL_BIOMASS_O_M_CHARGES,
COAL_CESS_CHARGES_INR_MT_OR_CUM,
FURNACE_OIL_AVG_COST_INR_L,
FURNACE_OIL_NO_OF_L_MT_OR_CUM,
NET_LOSS_PERCENTAGE,
POWER_AVG_VARIABLE_COST_UNIT,
POWER_COST_MT_OR_CUM,
POWER_NO_OF_UNITS_MT_OR_CUM,
PRODUCTION
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

# creating surrogate keys
d = surrogate_mapping_hil(csv_data,table_name,"ASPIRE_BUDGETS_OTHERS",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# insert log record into last execution details table 
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
