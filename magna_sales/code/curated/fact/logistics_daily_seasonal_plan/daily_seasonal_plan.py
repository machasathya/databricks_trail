# Databricks notebook source
# Import statements
from pyspark.sql.functions import count, col, when, sum, create_map, date_add, to_date,date_format, lit, date_sub, max,lpad
from pyspark.sql.types import StructType, DateType, StructField, ArrayType,StringType,IntegerType,LongType,DecimalType
from pyspark.sql.window import Window
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange
import time
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,lpad
from pyspark.sql import functions as sf
from ProcessMetadataUtility import ProcessMetadataUtility

# COMMAND ----------

# enabling cross join
spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

# creating class object
pmu = ProcessMetadataUtility()

# COMMAND ----------

# code to fetch the data from adf parameters
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
table_name = "DAILY_SEASONAL_PLAN"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# variables to run the notebook manually
# meta_table = "fact_surrogate_meta"
# processed_schema_name = "global_semantic_prod"
# schema_name = "metadata_prod"
# db_url = "hil-azr-sql-srve.database.windows.net"
# db = "bi_analytics"
# user_name = "hil-admin@hil-azr-sql-srve"
# scope = "AZR-DBR-KV-SCOPE-300"
# password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])
# processed_location = "mnt/bi_datalake/prod/pro/"
# curated_db_name = "cur_prod"
# processed_db_name = "pro_prod"
# table_name = "DAILY_SEASONAL_PLAN"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# creating required date values
now = datetime.now()
ist_zone = datetime.now() + timedelta(hours=5.5)
today = now

current_month = today.month
current_day = today.day

start_date = today.replace(day=1)
start_date_for_projection=now
day_n = start_date_for_projection.day
if day_n==1: 
  end_date_for_actuals = start_date_for_projection.replace(day=day_n)
else:  
  end_date_for_actuals = start_date_for_projection.replace(day=day_n-1)
  
end_date_for_actuals_str = end_date_for_actuals.strftime("%Y-%m-%d")

previous_day = (ist_zone - timedelta(days=1))
previous_day_str = previous_day.strftime("%Y-%m-%d")


last_date_unadjusted = today+relativedelta(months=1)
last_date = last_date_unadjusted.replace(day=monthrange(today.year, last_date_unadjusted.month)[1]) # today.replace(day=monthrange(today.year, current_month)[1])
last_3_days_date = last_date + timedelta(days=-3)

print(monthrange(today.year, last_date_unadjusted.month)[1])

today_str = today.strftime("%Y-%m-%d")
current_month_year = today.strftime("%Y-%m")
start_date_str = start_date.strftime("%Y-%m-%d")
last_date_str = last_date.strftime("%Y-%m-%d")
last_3_days_date_str = last_3_days_date.strftime("%Y-%m-%d")
number_of_days = last_date.day
if number_of_days < 30:
  number_of_days = 30
days_left = last_date.day - current_day


print(last_date)
print(previous_day)
print(end_date_for_actuals)
print(end_date_for_actuals_str)
print(start_date_for_projection)


# COMMAND ----------

number_of_days_in_month=[]
number_of_days_in_month.append([1,"31"])
number_of_days_in_month.append([2,"30"])
number_of_days_in_month.append([3,"31"])
number_of_days_in_month.append([4,"30"])
number_of_days_in_month.append([5,"31"])
number_of_days_in_month.append([6,"30"])
number_of_days_in_month.append([7,"31"])
number_of_days_in_month.append([8,"31"])
number_of_days_in_month.append([9,"30"])
number_of_days_in_month.append([10,"31"])
number_of_days_in_month.append([11,"30"])
number_of_days_in_month.append([12,"31"])

list_dates = []

delta = timedelta(days=1)


date_index = start_date_for_projection

while date_index <= last_date:
  list_dates.append([date_index])
  date_index += delta
  
dates_in_month_tmp = spark.createDataFrame(list_dates, schema=StructType([StructField("dates", DateType())])).withColumn("day_in_month", date_format(col("dates"), "dd").cast("int")).withColumn("month", date_format(col("dates"), "MM").cast("int"))

number_of_days_in_month_df = spark.createDataFrame(number_of_days_in_month, schema=StructType([StructField("month", IntegerType()),StructField("number_of_days_in_month", StringType())]))

dates_in_month = dates_in_month_tmp.alias('dates_in_month').join(number_of_days_in_month_df,on=['month']).select("dates","day_in_month","number_of_days_in_month")

# COMMAND ----------

# Reading data from curated layer
col_day_wise = "MONTH_WITH_{days}_DAYS".format(days=str(number_of_days))
date_dim = spark.sql("select * from {db}.{table}".format(db=processed_db_name, table='DIM_DATE'))
plant_depot_dim = spark.sql("select * from {db}.{table}".format(db=processed_db_name, table='DIM_PLANT_DEPOT')) 
product_dim = spark.sql("select * from {db}.{table}".format(db=processed_db_name, table='DIM_PRODUCT')) 
log_day_wise_ratio = spark.sql("select * from {db}.{table}".format(db=curated_db_name, table='LOGISTICS_DAYWISE_SALE_SPLIT')).select("DAY_OF_MONTH", col("PLANT_WITH_30_DAYS").alias("MONTH_WITH_30_DAYS"),col("PLANT_WITH_31_DAYS").alias("MONTH_WITH_31_DAYS"),"DEPOT_WITH_30_DAYS","DEPOT_WITH_31_DAYS","SKU","PLANT_DEPOT_ID") # 
depot_plant_mapping = spark.read.table("{db}.{table}".format(db=curated_db_name, table='LOGISTICS_BUDGETARY_UNIT')).select("DEPOT_ID", "ATTACHED_PLANT_ID")
daily_seasonal_plant_depot_map = spark.read.table("{db}.{table}".format(db=curated_db_name, table='DAILY_SEASONAL_PLANT_DEPOT_MAPPING')).select("DEPOT_ID", "PLANT_ID")
material_list_df=spark.sql("select * from {db}.{table}".format(db=curated_db_name, table='daily_seasonal_plan_material_list')).select("MATERIAL_ID")
material_list_df=material_list_df.withColumn("MATERIAL_ID",lpad(col("MATERIAL_ID"),18,'0'))

# COMMAND ----------

depot_sales_target = spark.read.table("{db}.{table}".format(db=processed_db_name, table='FACT_LOGISTICS_DEPOT_MONTHLY_TARGETS')).select("PLANT_KEY","MATERIAL_KEY","DATE_KEY","SALE_PLAN")


plant_sales_target = spark.read.table("{db}.{table}".format(db=processed_db_name, table='FACT_LOGISTICS_PLANT_MONTHLY_TARGETS')).select("PLANT_KEY","MATERIAL_KEY","DATE_KEY","SALE_PLAN")

fact_inventory = spark.read.table("{db}.{table}".format(db=processed_db_name, table='FACT_LOGISTICS_INVENTORY'))

fact_sales_invoice_df = spark.read.table("{db}.{table}".format(db=processed_db_name, table='FACT_SALES_INVOICE'))

fact_sto = spark.read.table("{db}.{table}".format(db=processed_db_name, table='FACT_LOGISTICS_STO'))

fact_sto_plan_plant = spark.read.table("{db}.{table}".format(db=processed_db_name, table='FACT_LOGISTICS_STC_TARGETS'))

dim_storage_location = spark.read.table("{db}.{table}".format(db=processed_db_name, table='DIM_STORAGE_LOCATION'))

production_actuals = spark.read.table("{db}.{table}".format(db=processed_db_name, table='FACT_LOGISTICS_PRODUCTION'))

plant_production_target = spark.read.table("{db}.{table}".format(db=processed_db_name, table='FACT_LOGISTICS_PLANT_DAILY_TARGETS')).select("PLANT_KEY","MATERIAL_KEY","DATE_KEY","PRODUCTION_PLAN") 


# COMMAND ----------

plant_sku_list = []

plant_sku_df1 = material_list_df #.withColumn("current_month_year",lit(current_month_year))
plant_sku_df1.createOrReplaceTempView("material_date")
dates_in_month.createOrReplaceTempView("dates_in_month")
plant_sku_df = spark.sql("select plant_id,material_id,current_month_year from material_date materials join (select distinct(plant_id) from {db}.{table}) req_plants  join (select distinct(date_format(dates,'yyyy-MM')) as current_month_year from dates_in_month) current_month_years ".format(db=curated_db_name, table='DAILY_SEASONAL_PLANT_DEPOT_MAPPING'))

plant_sku_df = plant_sku_df.alias("plant_sku").join(plant_depot_dim,on=[plant_sku_df["plant_id"]==plant_depot_dim["plant_id"]]).select("plant_sku.plant_id","plant_key","material_id","current_month_year")


plant_sku_df = plant_sku_df.alias("plant_sku").join(product_dim,on=[plant_sku_df["material_id"]==product_dim["material_number"]]).select("plant_id","plant_key","current_month_year","plant_sku.material_id","material_key",col("category").alias("sku"))

# COMMAND ----------

################### Openning stock #################




################### CALCULATION OF PLANT #################

stock = fact_inventory.groupBy("plant_key", "matl_key","snapshot_date_key").agg(sum("good_qty").alias("good_qty_sum"), sum("stock_curing").alias('stock_curing_sum')).withColumn("opening_stock", col("good_qty_sum") + col("stock_curing_sum"))


stock_with_date = stock.join(date_dim,on=[stock['snapshot_date_key']== date_dim['date_created_key']]).select("plant_key", "matl_key","date_dt", "good_qty_sum", "opening_stock").filter(col("date_dt") == previous_day_str)

stock_with_plant = stock_with_date.join(plant_depot_dim,on=[stock_with_date['plant_key']== plant_depot_dim['plant_key']]).select("plant_id","is_plant", "matl_key","date_dt","good_qty_sum","opening_stock","report_plant_id")

plant_stock_with_product_unagg = stock_with_plant.join(product_dim,on=[stock_with_plant['matl_key']== product_dim['material_key']]).select("plant_id","is_plant", "material_number","date_dt","good_qty_sum","opening_stock","report_plant_id")


plant_stock_with_product = plant_stock_with_product_unagg.groupBy("report_plant_id","material_number","date_dt").agg(sum("good_qty_sum").alias("good_qty_sum"),sum("opening_stock").alias("opening_stock"),max("is_plant").alias("is_plant")).select(col("report_plant_id").alias("plant_id"),"material_number","date_dt","good_qty_sum","opening_stock","is_plant")

#############################calculation for depot inventory ####################

depot_stock = fact_inventory.join(dim_storage_location,on=[fact_inventory['storage_location_key']== dim_storage_location['storage_location_key']]).select("plant_key","matl_key","snapshot_date_key","storage_location",col("good_qty").alias("good_qty_sum"),col("stock_curing").alias("stock_curing_sum")).withColumn("opening_stock", col("good_qty_sum") + col("stock_curing_sum"))


depot_stock_with_date = depot_stock.join(date_dim,on=[depot_stock['snapshot_date_key']== date_dim['date_created_key']]).select("plant_key", "matl_key","date_dt", "good_qty_sum", "opening_stock","storage_location").filter(col("date_dt") == previous_day_str)

depot_stock_with_date_plant_details = depot_stock_with_date.join(plant_depot_dim,on=[depot_stock_with_date['plant_key']== plant_depot_dim['plant_key']]).filter("is_plant='0'").select("plant_id","is_plant", "matl_key","date_dt","good_qty_sum","opening_stock","report_plant_id","storage_location")

depot_stock_with_product_unagg = depot_stock_with_date_plant_details.join(product_dim,on=[depot_stock_with_date_plant_details['matl_key']== product_dim['material_key']]).select("plant_id","is_plant", "material_number","date_dt","good_qty_sum","opening_stock","report_plant_id","storage_location")



inventory = depot_stock_with_product_unagg.alias("stock")
plant_depot_map = daily_seasonal_plant_depot_map.alias("plant_depot_map")


plant_sku_with_depot_inventory_tmp_df = plant_depot_map.join(inventory,on=[inventory["plant_id"]==plant_depot_map["depot_id"],inventory["storage_location"]==plant_depot_map["plant_id"]],how="left").select(col("stock.plant_id").alias("depot_id"),col("plant_depot_map.plant_id").alias("plant_id"),"is_plant", "material_number","date_dt","good_qty_sum")

depot_stock_with_product = plant_sku_with_depot_inventory_tmp_df.groupBy("plant_id", "material_number","date_dt").agg(sum("good_qty_sum").alias("depot_opening_stock"))


stock_with_product = plant_stock_with_product.alias('plant_stock').join(depot_stock_with_product,on=['plant_id','material_number','date_dt'],how="left").select(col("plant_stock.plant_id").alias("plant_id"),"is_plant", col("plant_stock.material_number").alias("material_number"),col("plant_stock.date_dt").alias("date_dt"),"good_qty_sum","opening_stock","depot_opening_stock")



plant_sku_with_inventory_df = plant_sku_df.alias("plant_sku_map").join(stock_with_product,on=[plant_sku_df["plant_id"]==stock_with_product["plant_id"],plant_sku_df["material_id"]==stock_with_product["material_number"]],how="left").select(col("plant_sku_map.plant_id").alias("plant_id"),"is_plant", col("plant_sku_map.material_id").alias("material_number"),"date_dt","good_qty_sum","opening_stock","depot_opening_stock",col("current_month_year").alias("billing_month"),"sku")


# COMMAND ----------


################### ACTUAL SALES #################


fact_sales_invoice_df_with_date = fact_sales_invoice_df.join(date_dim,on=[fact_sales_invoice_df['billing_date_key']== date_dim['date_created_key']]).filter(col("date_dt").between(start_date_str, end_date_for_actuals_str)).select("plant_key", "material_key",date_format(col("date_dt"),"yyyy-MM").alias("billing_month"),"gross_weight","billing_type").withColumn('gross_weight',when(col("billing_type") == 'ZCRE', -col('gross_weight')).otherwise(col('gross_weight'))).where("billing_type in ('ZFOC','ZINV','ZENV','ZCRE')")        



fact_sales_invoice_df_date_sales = fact_sales_invoice_df_with_date.groupBy("plant_key", "material_key","billing_month").agg(sum("gross_weight").alias("total_sales"))


fact_sales_invoice_df_date_plant_sales = fact_sales_invoice_df_date_sales.join(plant_depot_dim,on=[fact_sales_invoice_df_date_sales['plant_key']== plant_depot_dim['plant_key']]).select("plant_id","is_plant","material_key","billing_month","total_sales","report_plant_id")

fact_plant_sales_invoice_unagg = fact_sales_invoice_df_date_plant_matl_sales = fact_sales_invoice_df_date_plant_sales.join(product_dim,on=[fact_sales_invoice_df_date_plant_sales['material_key']== product_dim['material_key']]).select("plant_id", "is_plant", "material_number","billing_month","total_sales","report_plant_id")


fact_sales_invoice_df_final = fact_plant_sales_invoice_unagg.groupBy("report_plant_id","material_number","billing_month").agg(sum("total_sales").alias("total_sales"),max("is_plant").alias("is_plant")).select(col("report_plant_id").alias("plant_id"),"material_number","billing_month","total_sales","is_plant")


plant_sale = fact_sales_invoice_df_final.alias("plant_sale")
plant_depot_map = daily_seasonal_plant_depot_map.alias("plant_depot_map")


depot_sale = plant_depot_map.join(plant_sale,on=[plant_sale["plant_id"]==plant_depot_map["depot_id"]],how="left").select(col("plant_sale.plant_id").alias("depot_id"),col("plant_depot_map.plant_id").alias("plant_id"),"is_plant", "material_number","billing_month","total_sales")

depot_sale_with_product = depot_sale.groupBy("plant_id", "material_number","billing_month").agg(sum("total_sales").alias("depot_sale"))

sale_with_product = plant_sale.join(depot_sale_with_product,on=['plant_id','material_number','billing_month'],how="left").select(col("plant_sale.plant_id").alias("plant_id"),"is_plant", col("plant_sale.material_number").alias("material_number"),col("plant_sale.billing_month").alias("billing_month"),"depot_sale","total_sales")




plant_sku_with_inventory_sales_df = plant_sku_with_inventory_df.alias('plant_sku_with_inventory').join(sale_with_product,on=['plant_id','material_number',"billing_month"],how="left").select(col("plant_sku_with_inventory.plant_id").alias("plant_id"),col("plant_sku_with_inventory.is_plant").alias("is_plant"), col("plant_sku_with_inventory.material_number").alias("material_number"),"date_dt","good_qty_sum","opening_stock","depot_opening_stock","total_sales","depot_sale",col("plant_sku_with_inventory.billing_month").alias("billing_month"),"sku")

plant_sku_inventory_sales_df = plant_sku_with_inventory_sales_df.filter("is_plant='1'")



# COMMAND ----------

###################  SALES TARGETS FOR PLANT #################

plant_depot_sales_target=depot_sales_target.union(plant_sales_target)

plant_depot_sales_target_with_date_original = plant_depot_sales_target.join(date_dim,on=[plant_depot_sales_target['date_key']== date_dim['date_created_key']]).select("plant_key", "material_key",date_format(col("date_dt"),"yyyy-MM").alias("billing_month"),"date_dt","SALE_PLAN")




plant_depot_sales_target_with_date = plant_depot_sales_target_with_date_original.groupBy("plant_key", "material_key","billing_month").agg(sum("SALE_PLAN").alias("total_sales_plan"))

plant_depot_sales_target_with_date_plant = plant_depot_sales_target_with_date.join(plant_depot_dim,on=[plant_depot_sales_target_with_date['plant_key']== plant_depot_dim['plant_key']]).select("plant_id","is_plant","material_key","billing_month","total_sales_plan")

plant_depot_sales_target_final = plant_depot_sales_target_with_date_plant_matl = plant_depot_sales_target_with_date_plant.join(product_dim,on=[plant_depot_sales_target_with_date_plant['material_key']== product_dim['material_key']]).select("plant_id", "material_number","billing_month","total_sales_plan")


plant_targets = plant_depot_sales_target_final.alias("plant_targets")
plant_depot_map = daily_seasonal_plant_depot_map.alias("plant_depot_map")


depot_targets = plant_depot_map.join(plant_targets,on=[plant_targets["plant_id"]==plant_depot_map["depot_id"]],how="left").select(col("plant_targets.plant_id").alias("depot_id"),col("plant_depot_map.plant_id").alias("plant_id"),"material_number","billing_month","total_sales_plan")

depot_targets_with_product = depot_targets.groupBy("plant_id", "material_number","billing_month").agg(sum("total_sales_plan").alias("depot_sale_plan"))


saleplan_with_product = plant_targets.join(depot_targets_with_product,on=['plant_id','material_number','billing_month'],how="left").select(col("plant_targets.plant_id").alias("plant_id"), col("plant_targets.material_number").alias("material_number"),col("plant_targets.billing_month").alias("billing_month"),"depot_sale_plan","total_sales_plan")



plant_sku_inventory_sales_salesplan_df = plant_sku_inventory_sales_df.alias('plant_sku_inventory_sales').join(saleplan_with_product,on=['plant_id','material_number',"billing_month"],how="left").select(col("plant_sku_inventory_sales.plant_id").alias("plant_id"),col("plant_sku_inventory_sales.is_plant").alias("is_plant"), col("plant_sku_inventory_sales.material_number").alias("material_number"),col("plant_sku_inventory_sales.billing_month").alias("billing_month"),"date_dt","good_qty_sum","opening_stock","total_sales","total_sales_plan","depot_opening_stock","depot_sale","depot_sale_plan","sku")




# COMMAND ----------

#dbutils.fs.ls("/mnt/datalake/r01/pro/FACT/FACT_STO")
###################  STO ACTUAL FOR PLANT #################


fact_sto_with_date = fact_sto.join(date_dim,on=[fact_sto['SUPPLYING_DATE_KEY']== date_dim['date_created_key']]).select("supplying_plant_key", "material_key",date_format(col("date_dt"),"yyyy-MM").alias("billing_month"),"rec_quantity_mt").filter(col("date_dt").between(start_date_str, end_date_for_actuals_str))


fact_sto_with_date = fact_sto_with_date.groupBy("supplying_plant_key", "material_key","billing_month").agg(sum("rec_quantity_mt").alias("stc_actual"))

fact_sto_with_date_plant = fact_sto_with_date.join(plant_depot_dim,on=[fact_sto_with_date['supplying_plant_key']== plant_depot_dim['plant_key']]).select("plant_id","is_plant","material_key","billing_month","stc_actual")

fact_sto_with_date_plant_final = fact_sto_with_date_plant_plant_matl = fact_sto_with_date_plant.join(product_dim,on=[fact_sto_with_date_plant['material_key']== product_dim['material_key']]).select("plant_id", "is_plant", "material_number","billing_month","stc_actual")


plant_sku_inventory_sales_salesplan_sto_df = plant_sku_inventory_sales_salesplan_df.alias('plant_sku_inventory_sales_salesplan').join(fact_sto_with_date_plant_final,on=['plant_id','material_number',"billing_month"],how="left").select(col("plant_sku_inventory_sales_salesplan.plant_id").alias("plant_id"),col("plant_sku_inventory_sales_salesplan.is_plant").alias("is_plant"),col("plant_sku_inventory_sales_salesplan.material_number").alias("material_number"),col("plant_sku_inventory_sales_salesplan.billing_month").alias("billing_month"),"date_dt","good_qty_sum","opening_stock","total_sales","total_sales_plan","stc_actual","depot_opening_stock","depot_sale","depot_sale_plan","sku")



# COMMAND ----------

###################  STO PLAN FOR PLANT #################


fact_sto_plan_with_date = fact_sto_plan_plant.join(date_dim,on=[fact_sto_plan_plant['date_key']== date_dim['date_created_key']]).select("plant_key", "material_key",date_format(col("date_dt"),"yyyy-MM").alias("billing_month"),"stc_plan")
                                    

fact_sto_plan_with_date = fact_sto_plan_with_date.groupBy("plant_key", "material_key","billing_month").agg(sum("stc_plan").alias("stc_plan"))

fact_sto_plan_with_date_plant = fact_sto_plan_with_date.join(plant_depot_dim,on=[fact_sto_plan_with_date['plant_key']== plant_depot_dim['plant_key']]).select("plant_id","is_plant","material_key","billing_month","stc_plan")

fact_sto_plan_with_date_plant_final = fact_sto_plan_with_date_plant_plant_matl = fact_sto_plan_with_date_plant.join(product_dim,on=[fact_sto_plan_with_date_plant['material_key']== product_dim['material_key']]).select("plant_id", "is_plant", "material_number","billing_month","stc_plan")

plant_sku_inventory_sales_salesplan_sto_stoplan_df = plant_sku_inventory_sales_salesplan_sto_df.alias('plant_sku_inventory_sales_salesplan_sto').join(fact_sto_plan_with_date_plant_final,on=['plant_id','material_number',"billing_month"],how="left").select(col("plant_sku_inventory_sales_salesplan_sto.plant_id").alias("plant_id"),col("plant_sku_inventory_sales_salesplan_sto.is_plant").alias("is_plant"),col("plant_sku_inventory_sales_salesplan_sto.material_number").alias("material_number"),col("plant_sku_inventory_sales_salesplan_sto.billing_month").alias("billing_month"),"date_dt","good_qty_sum","opening_stock","total_sales","total_sales_plan","stc_actual","stc_plan","depot_opening_stock","depot_sale","depot_sale_plan","sku")

# COMMAND ----------

###################  BALANCE DISPATCH FOR PLANT #################
plant_sku_inventory_sales_salesplan_sto_stoplan_balancedispatch_df = plant_sku_inventory_sales_salesplan_sto_stoplan_df.fillna({"good_qty_sum":0,"opening_stock":0,"stc_plan":0,"stc_actual":0,"total_sales_plan":0,"total_sales":0,"depot_opening_stock":0,"depot_sale_plan":0,"depot_sale":0}).withColumn("balance_dispatch",col("total_sales_plan")+col("stc_plan")-col("total_sales")-col("stc_actual")).withColumn("depot_balance_dispatch",col("depot_sale_plan")-col("depot_sale")).withColumn("balance_stc",col("stc_plan")-col("stc_actual"))

# COMMAND ----------

###################  DAILY SALES AS PER PLAN #################

# Window logic to calculate the running total of the production ratio
w = Window.orderBy("DAY_OF_MONTH", col_day_wise).rowsBetween(
    Window.unboundedPreceding,  # Take all rows from the beginning of frame
    Window.currentRow           # To current row
)

log_day_wise_total_ratio = log_day_wise_ratio
ratio_of_sale = dates_in_month.join(log_day_wise_total_ratio,on=[dates_in_month["day_in_month"] == log_day_wise_total_ratio["DAY_OF_MONTH"]]).select('PLANT_DEPOT_ID',col('SKU').alias('SKU_FROM_DATASET'),'dates',date_format(col("dates"), "yyyy-MM").alias("month_name"),'number_of_days_in_month','MONTH_WITH_30_DAYS','MONTH_WITH_31_DAYS','DEPOT_WITH_30_DAYS','DEPOT_WITH_31_DAYS').withColumn('plant_sale_ratio',when(col("number_of_days_in_month") == 31, col('MONTH_WITH_31_DAYS')).otherwise(col('MONTH_WITH_30_DAYS'))).withColumn('depot_sale_ratio',when(col("number_of_days_in_month") == 31, col('DEPOT_WITH_31_DAYS')).otherwise(col('DEPOT_WITH_30_DAYS')))

daily_sale_as_per_plan = ratio_of_sale.join(plant_sku_inventory_sales_salesplan_sto_stoplan_balancedispatch_df, on=[ratio_of_sale["month_name"] == plant_sku_inventory_sales_salesplan_sto_stoplan_balancedispatch_df["billing_month"],ratio_of_sale["PLANT_DEPOT_ID"] == plant_sku_inventory_sales_salesplan_sto_stoplan_balancedispatch_df["PLANT_ID"],ratio_of_sale["SKU_FROM_DATASET"] == plant_sku_inventory_sales_salesplan_sto_stoplan_balancedispatch_df["SKU"]], how='inner').select("dates","billing_month", "plant_id", "material_number", "stc_plan", "stc_actual", "total_sales_plan", "total_sales", "balance_dispatch", "plant_sale_ratio", "good_qty_sum", "opening_stock","number_of_days_in_month","depot_opening_stock","depot_sale","depot_sale_plan","depot_sale_ratio","sku")

daily_sale_as_per_plan_final = daily_sale_as_per_plan.withColumn('plant_daily_sale_plan', (col("total_sales_plan") - col("total_sales")) * (col("plant_sale_ratio") / 100)).withColumn('depot_daily_sale_plan', (col("depot_sale_plan") - col("depot_sale")) * (col("depot_sale_ratio") / 100))

# COMMAND ----------

###################  DAILY STO AS PER PLAN #################
daily_sto_as_per_plan = daily_sale_as_per_plan_final.withColumn("sto_ratio", (lit(100)/(col("number_of_days_in_month").cast("int") - 3)))

daily_sto_as_per_plan_final = daily_sto_as_per_plan.withColumn('daily_sto_plan', when(date_format(col("dates"), "dd") < col("number_of_days_in_month").cast("int") - 3, when(col("stc_plan") - col("stc_actual") != 0 , (col("stc_plan") - col("stc_actual")) * (col('sto_ratio')/100)).otherwise(0)).otherwise(0))

# COMMAND ----------

###################  PRODUCTION PLAN FOR PLANT #################

plant_production_target_with_date = plant_production_target.join(date_dim,on=[plant_production_target['date_key']== date_dim['date_created_key']]).select("plant_key", "material_key",col("date_dt").alias("production_date"), date_add(col("date_dt"), 15).alias("curing_completion_date"), col("production_plan").alias("production_plan"))
                                        
# DSP = Aggregate on actual dates and to be used in closing inventory
# PDS = Aggregate on usable dates and to be used in plant dispatchable stock

plant_production_target_with_date_plant = plant_production_target_with_date.join(plant_depot_dim,on=[plant_production_target_with_date['plant_key']== plant_depot_dim['plant_key']]).select("plant_id","is_plant","material_key","production_date","curing_completion_date","production_plan")

plant_production_target_with_date_plant_final = plant_production_target_with_date_plant_plant_matl = plant_production_target_with_date_plant.join(product_dim,on=[plant_production_target_with_date_plant['material_key']== product_dim['material_key']]).select("plant_id", "is_plant", "material_number","production_date","curing_completion_date","production_plan")

# COMMAND ----------

##################  CALCULATION OF CLOSING INVENTORY #################
# Consider production plan(add + 14 days. get all the data from today) and production actuals(add - 14 days. get all the data till today). Union both the dataframe
closing_inventory = daily_sto_as_per_plan_final.alias('sto').join(plant_production_target_with_date_plant_final.alias('plant_prod'), on=[daily_sto_as_per_plan_final['plant_id']== plant_production_target_with_date_plant_final['plant_id'],daily_sto_as_per_plan_final['material_number']== plant_production_target_with_date_plant_final['material_number'],daily_sto_as_per_plan_final['dates']== plant_production_target_with_date_plant_final['production_date']], how='left').select("sto.dates", "sto.billing_month", "sto.plant_id", "sto.material_number", "sto.stc_plan", "sto.stc_actual", "sto.total_sales_plan", "sto.total_sales", "sto.balance_dispatch", "sto.plant_daily_sale_plan", "sto.daily_sto_plan", "sto.good_qty_sum", "sto.opening_stock", "plant_prod.production_plan","sto.depot_daily_sale_plan","sto.depot_sale_plan","sto.depot_opening_stock","sto.plant_sale_ratio","sto.depot_sale_ratio").fillna({'production_plan': 0, 'opening_stock': 0, 'depot_opening_stock': 0,'good_qty_sum': 0,'depot_sale_plan': 0})
## Check the join condition once. Verify it. ##

closing_inventory_net_stock = closing_inventory.withColumn('net_stock', col('production_plan') - col('plant_daily_sale_plan') - col('daily_sto_plan')).withColumn('depot_net_stock', col('daily_sto_plan') - col('depot_daily_sale_plan'))

w = Window.partitionBy("plant_id", "material_number").orderBy("plant_id", "material_number", "dates").rowsBetween(
    Window.unboundedPreceding,  # Take all rows from the beginning of frame
    Window.currentRow           # To current row
)
closing_inventory_net_stock_cumm = closing_inventory_net_stock.withColumn("net_stock_cumm", sum("net_stock").over(w)).withColumn("depot_net_stock_cumm", sum("depot_net_stock").over(w)).orderBy("plant_id", "material_number", "dates")
closing_inventory_final = closing_inventory_net_stock_cumm.withColumn("closing_inventory", col("opening_stock") + col("net_stock_cumm")).withColumn("depot_closing_inventory", col("depot_opening_stock") + col("depot_net_stock_cumm"))

# COMMAND ----------

###################  PRODUCTION ACTUALS FOR PLANT #################
plant_production_target_after_curing_with_date_final = plant_production_target_with_date_plant_final.groupBy("plant_id","is_plant","material_number","production_date", "curing_completion_date").agg(sum("production_plan").alias("quantity_after_curing")).select("plant_id","is_plant", "material_number","production_date", "curing_completion_date","quantity_after_curing")

production_actuals_with_date = production_actuals.join(date_dim,on=[production_actuals['date_key']== date_dim['date_created_key']]).filter(date_add(col("date_dt"), 15) >= end_date_for_actuals_str).select("plant_key","material_key",col("date_dt").alias("production_date"), "weights_mt").withColumn("curing_completion_date", date_add(col("production_date"), 15))

production_actuals_with_plant = production_actuals_with_date.join(plant_depot_dim,on=[production_actuals_with_date['plant_key']== plant_depot_dim['plant_key']]).groupBy("plant_id","is_plant","material_key","production_date","curing_completion_date").agg(sum("weights_mt").alias("quantity_after_curing")).select("plant_id","is_plant","material_key","production_date", "curing_completion_date","quantity_after_curing")

production_actuals_with_product = production_actuals_with_plant.join(product_dim,on=[production_actuals_with_plant['material_key']== product_dim['material_key']]).select("plant_id","is_plant","material_number","production_date", "curing_completion_date", "quantity_after_curing")

dispatchable_production = production_actuals_with_product.union(plant_production_target_after_curing_with_date_final)

# COMMAND ----------

###################  PLANT DISPATCHABLE STOCK #################
plant_dispatchable_stock = closing_inventory_final.alias('clos').join(dispatchable_production.alias('prod_a'), on=[closing_inventory_final['plant_id']== dispatchable_production['plant_id'],closing_inventory_final['material_number']== dispatchable_production['material_number'],closing_inventory_final['dates']== dispatchable_production['curing_completion_date']], how='left').select("clos.dates","clos.billing_month","clos.plant_id","clos.material_number","clos.stc_plan","clos.stc_actual","total_sales_plan","total_sales","balance_dispatch","plant_daily_sale_plan","daily_sto_plan","good_qty_sum", "opening_stock","prod_a.is_plant","production_plan","net_stock","net_stock_cumm","closing_inventory", "depot_daily_sale_plan","depot_opening_stock","depot_net_stock","depot_net_stock_cumm","depot_closing_inventory","quantity_after_curing","plant_sale_ratio","depot_sale_ratio").fillna({'quantity_after_curing': 0})
## Check the join condiion once. Verify it. ##

w = Window.partitionBy("plant_id", "material_number").orderBy("plant_id", "material_number", "dates").rowsBetween(
    Window.unboundedPreceding,  # Take all rows from the beginning of frame
    Window.currentRow           # To current row
)

plant_dispatchable_net_stock = plant_dispatchable_stock.withColumn("plant_daily_net_dispatchable_stock",col("quantity_after_curing") - col('plant_daily_sale_plan') - col('daily_sto_plan')).withColumn("plant_net_dispatchable_stock_cumm", sum("plant_daily_net_dispatchable_stock").over(w)).orderBy("plant_id", "material_number", "dates")

plant_dispatchable_net_stock = plant_dispatchable_net_stock.withColumn("plant_closing_dispatchable_stock", col("good_qty_sum") + col("plant_net_dispatchable_stock_cumm"))
plant_dispatchable_net_stock = plant_dispatchable_net_stock.withColumn("plant_opening_inventory", col("closing_inventory") - col("net_stock"))
plant_dispatchable_net_stock = plant_dispatchable_net_stock.withColumn("plant_opening_good_inventory", col("plant_closing_dispatchable_stock") -col("plant_daily_net_dispatchable_stock"))

plant_dispatchable_stock_final = plant_dispatchable_net_stock.withColumn("depot_opening_inventory", col("depot_closing_inventory") - col("depot_net_stock"))

# COMMAND ----------

plant_dispatchable_stock_final.write.mode('overwrite').parquet(processed_location+"DAILY_SEASONAL_PLAN")

# COMMAND ----------

db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)


# COMMAND ----------

#SURROGATE IMPLEMENTATION

def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):
  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
  plant_dispatchable_stock_final.createOrReplaceTempView("{}".format(fact_name))
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
 BILLING_MONTH
,STC_PLAN
,STC_ACTUAL
,TOTAL_SALES_PLAN
,TOTAL_SALES
,BALANCE_DISPATCH
,PLANT_DAILY_SALE_PLAN
,DAILY_STO_PLAN
,GOOD_QTY_SUM
,OPENING_STOCK
,daily_seasonal_plan.IS_PLANT
,PRODUCTION_PLAN
,NET_STOCK_CUMM
,CLOSING_INVENTORY
,DEPOT_DAILY_SALE_PLAN
,DEPOT_OPENING_STOCK
,DEPOT_NET_STOCK_CUMM
,DEPOT_CLOSING_INVENTORY
,QUANTITY_AFTER_CURING
,PLANT_DAILY_NET_DISPATCHABLE_STOCK
,PLANT_NET_DISPATCHABLE_STOCK_CUMM
,PLANT_CLOSING_DISPATCHABLE_STOCK
,PLANT_OPENING_INVENTORY
,PLANT_OPENING_GOOD_INVENTORY
,DEPOT_OPENING_INVENTORY
,PLANT_SALE_RATIO
,DEPOT_SALE_RATIO
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

d = surrogate_mapping_hil(csv_data,"DAILY_SEASONAL_PLAN","DAILY_SEASONAL_PLAN",processed_db_name)

# COMMAND ----------

d.write.mode('overwrite').parquet(processed_location+"FACT_LOGISTICS_DAILY_SEASONAL_PLAN")

# COMMAND ----------

date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
