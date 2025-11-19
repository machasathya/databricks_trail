# Databricks notebook source
#Import Statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad,upper,concat, to_date, lower
from pyspark.sql import functions as sf
from pyspark.sql.types import DateType,IntegerType,DecimalType
from ProcessMetadataUtility import ProcessMetadataUtility
import pandas as pd
pmu = ProcessMetadataUtility()

# COMMAND ----------

# Enabling Cross Join
spark.conf.set("spark.sql.crossJoin.enabled", "true")

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
table_name = "FACT_PF_NSR_PERCENTAGE"
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
# table_name = "FACT_PF_NSR_PERCENTAGE"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

#  reading surrogate metadata from SQL
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

# reading data from curated layer
df=spark.sql("select * from {}.{}".format(curated_db_name, "NSR_SBU3_PF")).drop("LAST_UPDATED_DT_TS")

# COMMAND ----------

# Summing up the record values for each month
sum1_df = df.agg(sf.sum('APRIL').alias('APRIL'),sf.sum('MAY').alias('MAY'),sf.sum('JUNE').alias('JUNE'),sf.sum('JULY').alias('JULY'),sf.sum('AUGUST').alias('AUGUST'),sf.sum('SEPTEMBER').alias('SEPTEMBER'),sf.sum('OCTOBER').alias('OCTOBER'),sf.sum('NOVEMBER').alias('NOVEMBER'),sf.sum('DECEMBER').alias('DECEMBER'),sf.sum('JANUARY').alias('JANUARY'),sf.sum('FEBRUARY').alias('FEBRUARY'),sf.sum('MARCH').alias('MARCH'))

# Collecting all the months values with non-zero values
num1_df = sum1_df.withColumn("MONTH",when(col('MARCH').isNotNull(),lit(18)).when(col('FEBRUARY').isNotNull(),lit(17)).when(col('JANUARY').isNotNull(),lit(16)).when(col('DECEMBER').isNotNull(),lit(15)).when(col('NOVEMBER').isNotNull(),lit(14)).when(col('OCTOBER').isNotNull(),lit(13)).when(col('SEPTEMBER').isNotNull(),lit(12)).when(col('AUGUST').isNotNull(),lit(11)).when(col('JULY').isNotNull(),lit(10)).when(col('JUNE').isNotNull(),lit(9)).when(col('MAY').isNotNull(),lit(8)).when(col('APRIL').isNotNull(),lit(7)))

sbu1_col = num1_df.select('MONTH').collect()[0][0]

int_df1 = df.select(df.columns[:sbu1_col])

# COMMAND ----------

# seperating columns other than month
std_col = [c for c in int_df1.columns if c not in {"PRODUCT_LINE","SEGMENT","PRODUCT_MIX","PRODUCT_TYPE","ZONE","FINANCIAL_YEAR"}]

# COMMAND ----------

# Pivot Transformations and adding Date all the records
pivot_df = int_df1.selectExpr("PRODUCT_LINE","SEGMENT","PRODUCT_MIX","PRODUCT_TYPE","ZONE","FINANCIAL_YEAR", "stack({}, {})".format(len(std_col), ', '.join(("'{}', {}".format(i, i) for i in std_col)))).withColumnRenamed("col0","MONTH").withColumnRenamed("col1","PERCENTAGE")

# COMMAND ----------

# Modifying the Dates to Financial Dates where January, February, March are mapped to the the previous year
int_df2 = pivot_df.withColumn('MONTH_NUMBER', sf.date_format(to_date(col('MONTH'), 'MMMMM'), 'MM'))
df_yr = int_df2.withColumn('YEAR',when((col("MONTH_NUMBER")  == '01') | (col("MONTH_NUMBER")  == '02') |(col("MONTH_NUMBER")  == '03'),int_df2["FINANCIAL_YEAR"]).otherwise((int_df2["FINANCIAL_YEAR"].cast(IntegerType())) - 1))
int_df3 = df_yr.withColumn('DATE_STR',sf.concat(sf.col('YEAR'),sf.lit('-'), sf.col('MONTH_NUMBER'),sf.lit('-01')))
final_df = int_df3.withColumn("DATE",int_df3['DATE_STR'].cast(DateType())).drop('FINANCIAL_YEAR').drop('LAST_UPDATED_DT_TS').drop('MONTH').drop('MONTH_NUMBER').drop('YEAR').drop('DATE_STR')

# COMMAND ----------

# writing data to processed layer before generating surrogate keys
final_df.write.parquet(processed_location+'NSR_SBU3_PF', mode='overwrite')

# COMMAND ----------

SBU3_data = final_df

# Refreshing data manually to ensure consistent metadata.

spark.catalog.refreshTable(processed_db_name + ".DIM_PRODUCT")
dim_product = spark.read.table(processed_db_name + ".DIM_PRODUCT")
spark.catalog.refreshTable(processed_db_name + ".DIM_TERRITORY")
dim_territory = spark.read.table(processed_db_name + ".DIM_TERRITORY")

# COMMAND ----------

# Fetching columns from DIM_TERRITORY after refreshing and storing in a Dataframe
dim_territory_split = dim_territory.select("PRODUCT_LINE","SALE_TYPE","SALES_ORG","ZONE","STATE","COUNTRY")
dim_territory_split = dim_territory_split.filter("SALES_ORG = '3000'").distinct()

# COMMAND ----------

SBU3_data_product= SBU3_data.withColumn("SEGMENT",when(col("SEGMENT")=="PROJECT","PROJECT").when(col("SEGMENT")=="RETAIL","RETAIL").when(col("SEGMENT")=="EXPORTS","EXPORTS").when(col("SEGMENT")=="EXPORT","EXPORTS").otherwise("<DEFAULT>"))

# Creating a column PINCODE, DISTRICT, SALES_ORG with a value 3000 for SBU3 and Renaming columns - SEGMENT,SALES_GROUP
SBU3_data_product = SBU3_data_product.withColumn("SALES_ORG",lit("3000")).withColumn("SALES_GROUP",lit("<DEFAULT>")).withColumnRenamed("SEGMENT","SALE_TYPE").withColumn("SALES_GROUP",lit("<DEFAULT>")).withColumn("PINCODE",lit("<DEFAULT>")).withColumn("DISTRICT",lit("<DEFAULT>"))

# COMMAND ----------

# Doing an left join of SBU3_data_product & dim_territory_split dataframes by considering SALES_ORG,PRODUCT_LINE,SALE_TYPE,ZONE as PK's and filering out the data of which we are further processing
SBU3_data_product = SBU3_data_product.alias("pc1").join(dim_territory_split, on = [SBU3_data_product["SALES_ORG"] == dim_territory_split["SALES_ORG"], SBU3_data_product["PRODUCT_LINE"] == dim_territory_split["PRODUCT_LINE"],SBU3_data_product["SALE_TYPE"] == dim_territory_split["SALE_TYPE"], lower(SBU3_data_product["ZONE"]) == lower(dim_territory_split["ZONE"])],
                                   how='left').select([col("pc1." + cols) for cols in SBU3_data_product.columns] + [col("COUNTRY")] + [col("STATE")])

#Removing duplicates from SBU3_data_product
SBU3_data_product = SBU3_data_product.dropDuplicates()

# COMMAND ----------

# Replacing the null vaues with <DEFUALT>
SBU3_data_product=SBU3_data_product.fillna( { 'PINCODE':"<DEFAULT>", 'DISTRICT':"<DEFAULT>",'SALES_GROUP':"<DEFAULT>",'STATE':"<DEFAULT>",'ZONE':"<DEFAULT>",'COUNTRY':"<DEFAULT>"  } )
SBU3_data_product=SBU3_data_product.withColumn("PINCODE_TXT",when(col("PINCODE")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("PINCODE"))).withColumn("DISTRICT_TXT",when(col("DISTRICT")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("DISTRICT"))).withColumn("SALES_GROUP_TXT",when(col("SALES_GROUP")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("SALES_GROUP"))).withColumn("STATE_TXT",when(col("STATE")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("STATE"))).withColumn("ZONE_TXT",when(col("ZONE")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("ZONE"))).withColumn("COUNTRY_TXT",when(col("COUNTRY")=="<DEFAULT>",lit(None).cast("string")).otherwise(col("COUNTRY")))
SBU3_data_product.createOrReplaceTempView("SBU3_data_tmp")

# COMMAND ----------

# Adding RLS column which contain PRODUCT_LINE, STATE, SALE_TYPE
inter_df = SBU3_data_product.withColumn("RLS",concat(col("PRODUCT_LINE"),lit('_'),col("STATE"),lit('_'),col("SALE_TYPE"))).select("SALES_ORG","PRODUCT_LINE","SALE_TYPE","PINCODE","DISTRICT","SALES_GROUP","STATE","ZONE","COUNTRY",lit(None).cast("string").alias("GEO_DISTRICT"),lit(None).cast("string").alias("GEO_STATE"),"PINCODE_TXT","DISTRICT_TXT","SALES_GROUP_TXT","STATE_TXT","ZONE_TXT","COUNTRY_TXT","RLS",lit(None).cast("string").alias("GEO_KEY")).distinct()
# display(inter_df)

diff_df=inter_df.alias("df1").join(dim_territory.alias("df2"),on=[col("df1.SALES_ORG")==col("df2.SALES_ORG"),col("df1.PRODUCT_LINE")==col("df2.PRODUCT_LINE"),col("df1.SALE_TYPE")==col("df2.SALE_TYPE"),col("df1.PINCODE")==col("df2.PINCODE"),col("df1.DISTRICT")==col("df2.DISTRICT"),col("df1.SALES_GROUP")==col("df2.SALES_GROUP"),col("df1.STATE")==col("df2.STATE"),col("df1.ZONE")==col("df2.ZONE"),col("df1.COUNTRY")==col("df2.COUNTRY")],how='leftouter').where("df2.SALES_ORG is NULL").select([col("df1." +columns)for columns in inter_df.columns])

geo_temp =dim_territory.unionAll(diff_df)

# COMMAND ----------

primary_key = 'SALES_ORG|PRODUCT_LINE|SALE_TYPE|PINCODE|DISTRICT|SALES_GROUP|STATE|ZONE|COUNTRY'
processed_table_name = 'DIM_TERRITORY'
surrogate_key = 'GEO_KEY'

# COMMAND ----------

def create_surrogate_key(renamed_df):    
    merged_key_columns = ','.join(['processed_dim_table.{column} as {column}'.format(column = c) for c in primary_key.split('|')])
    merge_surrogate_columns = ''
    if surrogate_key:
      for c in surrogate_key.split('|'):
        processed_dim_table = spark.table("{processed_db_name}.{processed_table_name}".format(processed_db_name = processed_db_name, processed_table_name=processed_table_name))
        processed_dim_table.createOrReplaceTempView("processed_dim_table")
        max_surrogate_key = processed_dim_table.agg({"{c}".format(c=c): "max"}).collect()[0]
        max_surrogate_key = max_surrogate_key["max({c})".format(c=c)]
        if max_surrogate_key is None:
          max_surrogate_key = 0
        merge_surrogate_columns = ','+','.join(['ifnull(processed_dim_table.{column},monotonically_increasing_id()+1+{max_surrogate_key}) as {column}'.format(column = c,max_surrogate_key = max_surrogate_key)])
    create_primary_key_join_condition = ' and '.join(['curated_df.{c} = processed_dim_table.{c}'.format(c = c) for c in primary_key.split('|')])
    curated_df = renamed_df.drop('GEO_KEY')
    curated_df.createOrReplaceTempView("curated_df")
    merge_columns = ','.join(['curated_df.{column} as {column}'.format(column = c) for c in curated_df.columns])
    query = spark.sql("select {col} {surrogate_columns} from curated_df left join processed_dim_table on {join_condition}".format(col = merge_columns, surrogate_columns = merge_surrogate_columns, join_condition =create_primary_key_join_condition))
    return query

# COMMAND ----------

# creating surrogate keys for dimension
df_tert = create_surrogate_key(geo_temp)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
df_tert.write.mode('overwrite').parquet(processed_location+"DIM_TERRITORY")

# COMMAND ----------

spark.catalog.refreshTable("{db_name}.{table_name}".format(db_name = processed_db_name, table_name = "DIM_TERRITORY"))
spark.sql("refresh table {db_name}.{table_name}".format(db_name = processed_db_name, table_name = "DIM_TERRITORY"))

# COMMAND ----------

#SURROGATE IMPLEMENTATION

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
#   transpose_bucket.createOrReplaceTempView("zfi_cag")
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
      tmp_unmapped_str += "({fact_name}.{fact_column} is NULL OR trim({fact_name}.{fact_column}) = '')" \
                        " THEN -2 ELSE ifnull({dim_table}.{dim_surrogate}, -1) END as {fact_surrogate},\n".format(
            fact_surrogate=fact_surrogate, dim_surrogate=dim_surrogate,fact_column=fact_column, fact_name=fact_name,dim_table=dim_table)
    
      if tmp_unmapped_str not in select_condition:
            select_condition += tmp_unmapped_str
      
      join_condition += "\n left join {pro_db}.{dim_table} on {fact_table}.{fact_column} = {dim_table}.{dim_column} ".format(
            dim_table=dim_table, fact_column=fact_column,dim_column=dim_column,fact_table=fact_name,pro_db=processed_db_name)    
  select_condition = select_condition[:-2]      
  query = """select
  {fact_name}.PRODUCT_MIX,
  {fact_name}.PRODUCT_TYPE,
  {fact_name}.ZONE,
  {fact_name}.PERCENTAGE,
  {select_condition}  from {fact_name} {join_condition}
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
d = surrogate_mapping_hil(csv_data,table_name,"NSR_SBU3_PF",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate keys
d.write.mode('overwrite').parquet(processed_location+table_name)

# COMMAND ----------

# inserting log record into LAST_EXECUTION_DETAILS table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
