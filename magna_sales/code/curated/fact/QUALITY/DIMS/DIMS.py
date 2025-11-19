# Databricks notebook source
# import statements
from datetime import datetime
from pyspark.sql.functions import col, lit, expr, when, unix_timestamp, current_date,first,lpad,max as _max,first,lower
from pyspark.sql import functions as sf
from pyspark.sql.types import DateType
from ProcessMetadataUtility import ProcessMetadataUtility
pmu = ProcessMetadataUtility()

# COMMAND ----------

# enabling cross join
spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

# code to read the parameters from ADF
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
table_name = "FACT_CMS"
last_processed_time = datetime.now()
last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# code to run the code manually
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
# table_name = "FACT_CMS"
# last_processed_time = datetime.now()
# last_processed_time_str = last_processed_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# reading the surrogate metadata from sql
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
csv_data = pmu.get_data(cursor=db_cursor, col_lookup=meta_table, value=table_name, column="FACT_TABLE",schema_name = schema_name)
pmu.close(db_connector, db_cursor)

# COMMAND ----------

#register_complaints
register_complaints = spark.sql("""select COMPLAINT_TRACKING_NO,COMPLAINT_CODE,DOC_STATUS,CUSTOMER_CODE,LOCATION_CODE,CITY_CODE,STATE_CODE,SALES_REPRESENTATIVE_CODE,PRODUCT_TYPE_CODE,PRODUCT_CATEGORY_CODE,COMPLAINT_REGISTRATION_DATE,DATE_OF_VISIT,REMARKS,CANCEL_BY,CANCEL_DATE,CANCEL_REMARKS as COMPLIANT_CANCEL_REMARKS,END_CUSTOMER_DETAILS as CRC_END_CUSTOMER_DETAILS,COMPLAINT_CATEGORY_CODE,CREATED_BY as REGISTERED_BY,CUSTOMER_NAME,
CUSTOMER_TYPE_CODE,SUBSTOCKIEST_CODE,SUBSTOCKIEST_NAME,SUBSTOCKIEST_ADDRESS,SUBSTOCKIEST_NUMBER from {}.{} where DOC_STATUS <> 'DRAFT' AND COMPLAINT_TRACKING_NO <> ''""".format(curated_db_name, "CMS_REGISTER_COMPLAINTS"))

compensation = spark.sql("select * from {}.{}".format(curated_db_name, "cms_compensation"))

#investigation
investigation = spark.sql("""select COMPLAINT_TRACKING_NO,END_CUSTOMER_DETAILS as CI_END_CUSTOMER_DETAILS,ID as INVESTIGATION_NO,COMPLAINED_ATTENDED_DATE as ATTENDED_DATE,INVESTIGATION_DONE_BY_CODE AS INVESTIGATION_DONE_BY,TOTAL_SUPPLY_TON as CI_TOTAL_SUPPLY_TON,NET_LOSS_TONS as CI_NET_LOSS_TONS,TOTAL_BREAKAGE_TONS as CI_TOTAL_BREAKAGE_TONS,DELAY_DAYS,DELAY_REASON,DOC_STATUS as INVESTIGATION_STATUS,VISITED_DATE as CI_VISIT_DATE,CANCEL_REMARKS as INV_CANCEL_REMARKS,PARTY_TYPE,COMPLAINT_TYPE from {}.{}""".format(curated_db_name, "cms_investigation"))

complaint_status = spark.sql("select COMPLAINT_TRCKING_NO,INVESTIGATED_DATE,complaint_status,COMPLAINT_DATE from {}.{}".format(curated_db_name, "cms_complaint_status"))

# COMMAND ----------

# removing duplicates from investigation table
count_inv = spark.sql("""select count(COMPLAINT_TRACKING_NO) as Count,COMPLAINT_TRACKING_NO from {}.{} group by COMPLAINT_TRACKING_NO""".format(curated_db_name, "cms_investigation"))
duplicate_compno = count_inv.filter(count_inv.Count>1)
comp_no_list = duplicate_compno.select("COMPLAINT_TRACKING_NO").rdd.flatMap(lambda x: x).collect()
list_in_parens = lambda l: "(%s)" % str(l).strip('[]')
comp_no_string = list_in_parens(comp_no_list)

# COMMAND ----------

# reading data from curated layer by filtering on complaint tracking no
duplicate_inv = spark.sql("""select COMPLAINT_TRACKING_NO,END_CUSTOMER_DETAILS as CI_END_CUSTOMER_DETAILS,ID as INVESTIGATION_NO,COMPLAINED_ATTENDED_DATE as ATTENDED_DATE,INVESTIGATION_DONE_BY_CODE AS INVESTIGATION_DONE_BY,TOTAL_SUPPLY_TON as CI_TOTAL_SUPPLY_TON,NET_LOSS_TONS as CI_NET_LOSS_TONS,TOTAL_BREAKAGE_TONS as CI_TOTAL_BREAKAGE_TONS,DELAY_DAYS,DELAY_REASON,DOC_STATUS as INVESTIGATION_STATUS,VISITED_DATE as CI_VISIT_DATE,CANCEL_REMARKS as INV_CANCEL_REMARKS,PARTY_TYPE,COMPLAINT_TYPE from {}.{} where COMPLAINT_TRACKING_NO in {}""".format(curated_db_name, "cms_investigation",comp_no_string))

# COMMAND ----------

# reading data from curated layer
inv_msf = spark.sql("select * from {}.{}".format(curated_db_name, "cms_investigation_msf"))

#inv_msf_bu3
inv_msf_bu3 = spark.sql("select * from {}.{}".format(curated_db_name, "cms_investigation_msf_BU3"))

cms_comp_msf = spark.sql("select * from {}.{}".format(curated_db_name, "cms_compensation_msf"))

#comp_msf_bu3
comp_msf_bu3 = spark.sql("select * from {}.{}".format(curated_db_name, "cms_compensation_msf_BU3"))

# COMMAND ----------

# reading data from curated layer
#inv_break_sbu3_lines
inv_break_sbu3_lines = spark.sql("select LINE_ID,ID,SUPPLIED_QTY,DEFECT_QTY,DEFECT_TYPE_CODE,ACT_DEFECT_QTY,INVOICE_NO from {}.{}".format(curated_db_name, "cms_investigation_breakage_SBU3_lines"))

cmp_sup_det_bu3 = spark.sql("select * from {}.{}".format(curated_db_name,"cms_compensation_supply_details_BU3"))

#inv_break_sheet_lines
inv_break_sheet_lines = spark.sql("select LINE_ID,ID,SUPPLIED_QTY_M,REJECTED_QTY_M,DEFECT_TYPE_CODE,INVOICE_NO from {}.{}".format(curated_db_name, "cms_investigation_breakage_sheeting_lines"))

#inv_break_other_lines
inv_break_other_lines = spark.sql("select LINE_ID,ID,SUPPLIED_QTY,BREAKAGE_QTY,DEFECT_TYPE_CODE,INVOICE_NO from {}.{}".format(curated_db_name, "cms_investigation_breakage_others_lines"))

cmp_break_other_lines = spark.sql("select * from {}.{}".format(curated_db_name,"cms_compensation_breakage_others_lines"))

# COMMAND ----------

# reading data from curated layer
complaint_category_mas = spark.sql("select * from {}.{}".format(curated_db_name, "cms_complaint_category_master"))

#division_master
division_master = spark.sql("select division_code,product_type_code,division_name from {}.{}".format(curated_db_name, "sap_division_master"))

state = spark.sql("select state_code,state_desc from {}.{}".format(curated_db_name, "cms_state"))

#compensation_mode_master
compensation_mode_master = spark.sql("select COMPENSATION_MODE_NAME,COMPENSATION_MODE_CODE from {}.{}".format(curated_db_name, "cms_compensation_mode_master"))

#defect_type_master
defect_type_master = spark.sql("select * from {}.{}".format(curated_db_name, "cms_defect_type_master"))

# dims_employee = spark.sql("select EmployeeCode,EmployeeName from {}.{}".format(curated_db_name, "dims_employee_details"))

employee_role_configuration = spark.sql("select distinct EMPLOYEE_CODE,USER_ROLE_CODE from {}.{} where EMPLOYEE_CODE <> ''".format(curated_db_name, "cms_employee_role_configuration"))

# plant_master = spark.sql("select PLANT_CODE,PLANT_NAME from {}.{}".format(curated_db_name, "cms_plant_master"))

employee_role = employee_role_configuration.withColumn("ROLE", when(col("EMPLOYEE_CODE")=='50000967','QAM_SBU2').otherwise(col('USER_ROLE_CODE'))).drop(col('USER_ROLE_CODE')).distinct()

cms_type_of_complaints = spark.sql("select * from {}.{}".format(curated_db_name,"cms_type_of_complaints"))

# COMMAND ----------

# reading data from curated layer
zone_df = spark.read.format('delta').load("dbfs:/mnt/bi_datalake/prod/cur/DIMS_ZONE_MASTER")
zone_df = zone_df.withColumn('SBU',when(col("SALES_ORG") == 1000,"SBU1").when(col("SALES_ORG") == 2000,"SBU2").when(col("SALES_ORG") == 3000,"SBU3")).withColumn("STATE_CODE",lpad(col("STATE_CODE"),2,'0'))

# mapping_df = spark.read.format('parquet').load("dbfs:/mnt/bi_datalake/prod/lnd/dims/cms_zone_state_mapping")

cms_reg_complaint_msf = spark.sql('SELECT PLANT_CODE,ID,LINE_ID from {}.{}'.format(curated_db_name, "cms_register_complaints_msf"))

dim_personnel = spark.sql('SELECT PER_NUMBER,PER_NAME from {}.{}'.format(processed_db_name, "dim_personnel"))

party_type = spark.sql("select * from {}.{}".format(curated_db_name,"cms_party_type"))


dim_cust = spark.sql("select * from {}.{}".format(processed_db_name,"dim_customer"))

# COMMAND ----------

# removing duplicates after joining with msf table
bu_join = duplicate_inv.alias('bu').join(inv_msf.alias('inv'),on = [
  col('bu.INVESTIGATION_NO') == col("inv.ID")], how = 'inner').select(["bu." + cols for cols in duplicate_inv.columns])

bu3_join = duplicate_inv.alias('bu').join(inv_msf_bu3.alias('inv'),on = [
  col('bu.INVESTIGATION_NO') == col("inv.ID")], how = 'inner').select(["bu." + cols for cols in duplicate_inv.columns])

con_df = bu_join.union(bu3_join)
inv_sub = investigation.subtract(duplicate_inv)
main_inv = inv_sub.union(con_df)

# COMMAND ----------

# reading data from curated layer
cms_employee_master = spark.sql("Select Employee_Code,Employee_Name from {}.{}".format(curated_db_name,"cms_employeemaster"))
dims_employee_details = spark.sql("Select EmployeeCode,Sales_Organisation,first(EmployeeName) as EmployeeName from {}.{} group by EmployeeCode,Sales_Organisation".format(curated_db_name,"DIMS_EmployeeDetails"))

# COMMAND ----------

# joining register complaints data with investigation data
df1 = register_complaints.alias('reg').join(main_inv.where("INVESTIGATION_STATUS <> 'DRAFT'").alias('inv'),on = [
  col('reg.COMPLAINT_TRACKING_NO') == col("inv.COMPLAINT_TRACKING_NO")],
                                            how='left').drop(col("inv.COMPLAINT_TRACKING_NO"))

# COMMAND ----------

# joining with compensation data
df2=df1.alias('df').join(compensation.where("doc_status <> 'DRAFT'").alias('cmp'),on = [
  col('df.COMPLAINT_TRACKING_NO') == col('cmp.COMPLAINT_ID') 
],how='left').select(["df." + cols for cols in df1.columns] + [col("cmp.DOC_STATUS").alias("COMPENSATION_STATUS")] + [col("cmp.ID").alias("COMPENSATION_NO")] + [col("cmp.COMPENSATION_MODE_CODE")] + [col("cmp.end_customer_details").alias("CMP_END_CUSTOMER_DETAILS")] + [col("APPROVED_DATE").alias("COMPENSATION_DATE")] + [col("NET_LOSS_QTY_TONS").alias("COMPENSATION_VALUE")] + [col("CANCEL_REMARKS").alias("COMPENSATION_CANCEL_REMARKS"),col("COMPENSATION_IN_TONS"),col("COMPENSATION_IN_CUBIC_METER")])

# COMMAND ----------

# fetching complaint and investigated date
df3=df2.alias('df').join(complaint_status.alias('ccs'),on = [
  col('df.COMPLAINT_TRACKING_NO') == col('ccs.COMPLAINT_TRCKING_NO') 
],how='left').select(["df." + cols for cols in df2.columns] + [col("ccs.complaint_status").alias("FINAL_STATUS")] + [col("ccs.INVESTIGATED_DATE")] + [col("ccs.COMPLAINT_DATE")])

# COMMAND ----------

# extracting cancel remarks colummn values
remarks_df = df3.withColumn("CANCEL_REMARKS",when((col("FINAL_STATUS") == "Complaint Rejected"),col("COMPLIANT_CANCEL_REMARKS")).when((col("FINAL_STATUS") == "Investigation Rejected"),col("INV_CANCEL_REMARKS")).when((col("FINAL_STATUS") == "Compensation Rejected"),col("COMPENSATION_CANCEL_REMARKS")))
remarks_df = remarks_df.drop("COMPLIANT_CANCEL_REMARKS").drop("INVESTIGATION_CANCEL_REMARKS").drop("COMPENSATION_CANCEL_REMARKS")

# COMMAND ----------

# defining final status value
df4 = remarks_df.withColumn("END_CUSTOMER_DETAILS",when(((col("FINAL_STATUS") == "Complaint Rejected") | (col("FINAL_STATUS") == "Complaint Assigned") | (col("FINAL_STATUS") == "Complaint Registered") | (col("FINAL_STATUS") == "Complaint Approved")), col ("CRC_END_CUSTOMER_DETAILS")).when(((col("FINAL_STATUS") == "Investigation Rejected") | (col("FINAL_STATUS") == "Investigated and under review") | (col("FINAL_STATUS") == "Investigation Approved. Compensation under review")) , col ("CI_END_CUSTOMER_DETAILS")).when(((col("FINAL_STATUS") == "Compensation Rejected") | (col("FINAL_STATUS") == "Compensation Under Review") | (col("FINAL_STATUS") == "Compensation Approved and sent")) , col ("CMP_END_CUSTOMER_DETAILS")).otherwise(col("CRC_END_CUSTOMER_DETAILS")))
df4 = df4.drop("CRC_END_CUSTOMER_DETAILS").drop("CI_END_CUSTOMER_DETAILS").drop("CMP_END_CUSTOMER_DETAILS")

# COMMAND ----------

# logic to fetch plant id
plant_comp_df =df4.alias('df').join(cms_comp_msf.alias('msf'),on = [
  col('df.COMPENSATION_NO') == col('msf.ID') 
],how='left').select(["df." + cols for cols in df4.columns] + [col("msf.PLANT_CODE").alias("COMP_PLANT_CODE1")])


plant_inv_df =plant_comp_df.where("COMP_PLANT_CODE1 is null").alias('df').join(inv_msf.alias('msf'),on = [
  col('df.INVESTIGATION_NO') == col('msf.ID') 
],how='left').select(["df." + cols for cols in plant_comp_df.columns] + [col("msf.PLANT_CODE").alias("INV_PLANT_CODE1")])


plant_df1 = df4.alias("l_df").join(plant_comp_df.alias("r_df"),on = [col("l_df.COMPLAINT_TRACKING_NO") == col("r_df.COMPLAINT_TRACKING_NO")],how='left').select(["l_df." + cols for cols in df4.columns] + [col("COMP_PLANT_CODE1")])


plant_df2 = plant_df1.alias("l_df").join(plant_inv_df.alias("r_df"),on = [col("l_df.COMPLAINT_TRACKING_NO") == col("r_df.COMPLAINT_TRACKING_NO")],how='left').select(["l_df." + cols for cols in plant_df1.columns] + [col("INV_PLANT_CODE1")])


plant_df3 = plant_df2.withColumn("PLANT_CODE1",when(col("COMP_PLANT_CODE1").isNotNull(),col("COMP_PLANT_CODE1")).otherwise(col('INV_PLANT_CODE1')))

plant_df =plant_df3.alias('df').join(cms_reg_complaint_msf.alias('regmsf'),on = [
  col('df.COMPLAINT_TRACKING_NO') == col('regmsf.ID') 
],how='left').select(["df." + cols for cols in plant_df3.columns] + [col("regmsf.PLANT_CODE")])

fin_plant_df = plant_df.withColumn("PLANT_ID",when((col("PRODUCT_TYPE_CODE") == "SBU1"),col("PLANT_CODE1")).otherwise(col("PLANT_CODE")))
fin_plant_df = fin_plant_df.drop("PLANT_CODE1").drop("PLANT_CODE").distinct()

# COMMAND ----------

# aggregating qty values on complaint id and plant level for sbu1
comp_sbu1 = cms_comp_msf.groupBy(["ID","PLANT_CODE"]).agg(sf.sum("SUPPLY_QTY").alias("COMP_SUPPLIED_QTY"),sf.sum("BREAKAGE_QTY").alias("COMP_BREAKAGE_QTY"),sf.sum("NET_LOSS").alias("COMP_NET_LOSS"))

inv_sbu1 = inv_msf.groupBy("ID","PLANT_CODE").agg(sf.sum("SUPPLY_QTY").alias("INV_SUPPLIED_QTY"),sf.sum("BREAKAGE_QTY").alias("INV_BREAKAGE_QTY"),sf.sum("NET_LOSS").alias("INV_NET_LOSS"))

comp_qty_sbu1 = fin_plant_df.alias("l_df").join(comp_sbu1.alias("r_df"),on=[col("l_df.COMPENSATION_NO") == col("r_df.ID"),
                                                                                col("l_df.plant_id") == col("r_df.PLANT_CODE")],how='left').select(["l_df." + cols for cols in fin_plant_df.columns] + [col("COMP_SUPPLIED_QTY"),col("COMP_BREAKAGE_QTY"),col("COMP_NET_LOSS")])

inv_qty_sbu1 = comp_qty_sbu1.alias("l_df").join(inv_sbu1.alias("r_df"),on=[col("l_df.INVESTIGATION_NO") == col("r_df.ID"),
                                                                          col("l_df.PLANT_ID") == col("r_df.PLANT_CODE")],how='left').select(["l_df." + cols for cols in comp_qty_sbu1.columns] + [col("INV_SUPPLIED_QTY"),col("INV_BREAKAGE_QTY"),col("INV_NET_LOSS")])

sbu1 = inv_qty_sbu1.withColumn("SBU1_SUPPLIED_QTY",when(col("COMP_SUPPLIED_QTY").isNotNull(),col("COMP_SUPPLIED_QTY")).otherwise(col("INV_SUPPLIED_QTY"))).withColumn("SBU1_BREAKAGE_QTY",when(col("COMP_BREAKAGE_QTY").isNotNull(),col("COMP_BREAKAGE_QTY")).otherwise(col("INV_BREAKAGE_QTY"))).withColumn("SBU1_NET_LOSS",when(col("COMP_NET_LOSS").isNotNull(),col("COMP_NET_LOSS")).otherwise(col("INV_NET_LOSS"))).drop(*["COMP_SUPPLIED_QTY","INV_SUPPLIED_QTY","COMP_BREAKAGE_QTY","INV_BREAKAGE_QTY","COMP_NET_LOSS","INV_NET_LOSS"])

# COMMAND ----------

# aggregating qty values on complaint id and plant level for sbu2 and sbu3
comp_sbu23 = cms_comp_msf.groupBy(["ID"]).agg(sf.sum("SUPPLY_QTY").alias("COMP_SUPPLIED_QTY"),sf.sum("BREAKAGE_QTY").alias("COMP_BREAKAGE_QTY"),sf.sum("NET_LOSS").alias("COMP_NET_LOSS"))

inv_sbu23 = inv_msf.groupBy("ID").agg(sf.sum("SUPPLY_QTY").alias("INV_SUPPLIED_QTY"),sf.sum("BREAKAGE_QTY").alias("INV_BREAKAGE_QTY"),sf.sum("NET_LOSS").alias("INV_NET_LOSS"))

comp_qty_sbu2 = sbu1.alias("l_df").join(comp_sbu23.alias("r_df"),on=[col("l_df.COMPENSATION_NO") == col("r_df.ID")],how='left').select(["l_df." + cols for cols in sbu1.columns] + [col("COMP_SUPPLIED_QTY"),col("COMP_BREAKAGE_QTY"),col("COMP_NET_LOSS")])

inv_qty_sbu2 = comp_qty_sbu2.alias("l_df").join(inv_sbu23.alias("r_df"),on=[col("l_df.INVESTIGATION_NO") == col("r_df.ID")],how='left').select(["l_df." + cols for cols in comp_qty_sbu2.columns] + [col("INV_SUPPLIED_QTY"),col("INV_BREAKAGE_QTY"),col("INV_NET_LOSS")])

sbu2 = inv_qty_sbu2.withColumn("SBU2_SUPPLIED_QTY",when(col("COMP_SUPPLIED_QTY").isNotNull(),col("COMP_SUPPLIED_QTY")).otherwise(col("INV_SUPPLIED_QTY"))).withColumn("SBU2_BREAKAGE_QTY",when(col("COMP_BREAKAGE_QTY").isNotNull(),col("COMP_BREAKAGE_QTY")).otherwise(col("INV_BREAKAGE_QTY"))).withColumn("SBU2_NET_LOSS",when(col("COMP_NET_LOSS").isNotNull(),col("COMP_NET_LOSS")).otherwise(col("INV_NET_LOSS"))).drop(*["COMP_SUPPLIED_QTY","INV_SUPPLIED_QTY","COMP_BREAKAGE_QTY","INV_BREAKAGE_QTY","COMP_NET_LOSS","INV_NET_LOSS"])

# COMMAND ----------

# joining with compensation msf table and fetching quantity values for pipes and fittings
comp_wp_sbu3 = sbu2.alias('df').join(cms_comp_msf.alias('ccm'), on = [
  col('df.COMPENSATION_NO') == col('ccm.ID') ],how='left').where(col('PRODUCT_CATEGORY_CODE')=='36').select([col("ccm.ID").alias("ID")] + [col("ccm.SUPPLY_QTY").alias("SBU3_WP_SUPPLIED_QTY")] + [col("ccm.BREAKAGE_QTY").alias("SBU3_WP_BREAKAGE_QTY")] +[col("ccm.NET_LOSS").alias("SBU3_WP_NET_LOSS")]).groupBy("ID").agg(sf.sum("SBU3_WP_SUPPLIED_QTY").alias("SUPPLIED_QTY"),sf.sum("SBU3_WP_BREAKAGE_QTY").alias("BREAKAGE_QTY"),sf.sum("SBU3_WP_NET_LOSS").alias("NET_LOSS"))
if comp_wp_sbu3.rdd.isEmpty():
  inv_wp_sbu3 = sbu2.alias('df').join(inv_msf.alias('invm'), on = [
  col('df.INVESTIGATION_NO') == col('invm.ID') 
],how='left').where(col('PRODUCT_CATEGORY_CODE')=='36').select([col("invm.ID").alias("ID")] + [col("invm.SUPPLY_QTY").alias("SBU3_WP_SUPPLIED_QTY")] + [col("invm.BREAKAGE_QTY").alias("SBU3_WP_BREAKAGE_QTY")] +
[col("invm.NET_LOSS").alias("SBU3_WP_NET_LOSS")]).groupBy("ID").agg(sf.sum("SBU3_WP_SUPPLIED_QTY").alias("SUPPLIED_QTY"),sf.sum("SBU3_WP_BREAKAGE_QTY").alias("BREAKAGE_QTY"),sf.sum("SBU3_WP_NET_LOSS").alias("NET_LOSS"))
  wp_sbu3 = sbu2.alias('df').join(inv_wp_sbu3.alias('inv3'), on = [col('df.INVESTIGATION_NO') == col('inv3.ID')],how='left').select(["df." + cols for cols in sbu2.columns] + [col("inv3.SUPPLIED_QTY").alias('SBU3_WP_SUPPLIED_QTY')] + [col("inv3.BREAKAGE_QTY").alias('SBU3_WP_BREAKAGE_QTY')] + [col("inv3.NET_LOSS").alias('SBU3_WP_NET_LOSS')])
else:
  wp_sbu3 = sbu2.alias('df').join(comp_wp_sbu3.alias('comp3'), on = [col('df.COMPENSATION_NO') == col('comp3.ID')],how='left').select(["df." + cols for cols in sbu2.columns] + [col("comp3.SUPPLIED_QTY").alias('SBU3_WP_SUPPLIED_QTY')] + [col("comp3.BREAKAGE_QTY").alias('SBU3_WP_BREAKAGE_QTY')] + [col("comp3.NET_LOSS").alias('SBU3_WP_NET_LOSS')])

# COMMAND ----------

# joining with compensation msf table and fetching quantity values for putty
comp_sbu3 = wp_sbu3.alias('df').join(comp_msf_bu3.alias('ccm'), on = [
  col('df.COMPENSATION_NO') == col('ccm.ID') 
],how='left').where(col('PRODUCT_CATEGORY_CODE')!='36').select( [col("ccm.ID").alias("ID")] +[col("ccm.SupplyQty").alias("SBU3_SUPPLIED_QTY")] + [col("ccm.DefQty").alias("SBU3_BREAKAGE_QTY")] +
[col("ccm.ActDftQty").alias("SBU3_NET_LOSS")]).groupBy("ID").agg(sf.sum("SBU3_SUPPLIED_QTY").alias("SUPPLIED_QTY"),sf.sum("SBU3_BREAKAGE_QTY").alias("BREAKAGE_QTY"),sf.sum("SBU3_NET_LOSS").alias("NET_LOSS"))
if comp_sbu3.rdd.isEmpty():
  inv_sbu3 = wp_sbu3.alias('df').join(inv_msf_bu3.alias('invm'), on = [
  col('df.INVESTIGATION_NO') == col('invm.ID') 
],how='left').where(col('PRODUCT_CATEGORY_CODE')!='36').select([col("invm.ID").alias("ID")] + [col("invm.SupplyQty").alias("SBU3_SUPPLIED_QTY")] + [col("invm.DefQty").alias("SBU3_BREAKAGE_QTY")] +
[col("invm.ActDftQty").alias("SBU3_NET_LOSS")]).groupBy("ID").agg(sf.sum("SBU3_SUPPLIED_QTY").alias("SUPPLIED_QTY"),sf.sum("SBU3_BREAKAGE_QTY").alias("BREAKAGE_QTY"),sf.sum("SBU3_NET_LOSS").alias("NET_LOSS"))
  sbu3 = wp_sbu3.alias('df').join(inv_sbu3.alias('inv3'), on = [col('df.INVESTIGATION_NO') == col('inv3.ID')],how='left').select(["df." + cols for cols in wp_sbu3.columns] + [col("inv3.SUPPLIED_QTY").alias('SBU3_SUPPLIED_QTY')] + [col("inv3.BREAKAGE_QTY").alias('SBU3_BREAKAGE_QTY')] + [col("inv3.NET_LOSS").alias('SBU3_NET_LOSS')])
else:
  sbu3 = wp_sbu3.alias('df').join(comp_sbu3.alias('comp3'), on = [col('df.COMPENSATION_NO') == col('comp3.ID')],how='left').select(["df." + cols for cols in wp_sbu3.columns] + [col("comp3.SUPPLIED_QTY").alias('SBU3_SUPPLIED_QTY')] + [col("comp3.BREAKAGE_QTY").alias('SBU3_BREAKAGE_QTY')] + [col("comp3.NET_LOSS").alias('SBU3_NET_LOSS')])

# COMMAND ----------

# defining total supply ton and filling nulls with 0's
df5 = sbu3.withColumn("TOTAL_SUPPLY_TON",when((col("PRODUCT_TYPE_CODE") == "SBU1"),col("SBU1_SUPPLIED_QTY")).when((col("PRODUCT_TYPE_CODE") == "SBU2"),col("SBU2_SUPPLIED_QTY")).when((col("PRODUCT_TYPE_CODE") == "SBU3") & (col("PRODUCT_CATEGORY_CODE") == "36") ,col("SBU3_WP_SUPPLIED_QTY")).when((col("PRODUCT_TYPE_CODE") == "SBU3") & (col("PRODUCT_CATEGORY_CODE") != "36") ,col("SBU3_SUPPLIED_QTY")))
df5 = df5.fillna({ 'TOTAL_SUPPLY_TON':0 })

# COMMAND ----------

# defining total breakage ton and filling nulls with 0's
df6 = df5.withColumn("TOTAL_BREAKAGE_TON",when((col("PRODUCT_TYPE_CODE") == "SBU1"),col("SBU1_BREAKAGE_QTY")).when((col("PRODUCT_TYPE_CODE") == "SBU2"),col("SBU2_BREAKAGE_QTY")).when(((col("PRODUCT_TYPE_CODE") == "SBU3") & (col("PRODUCT_CATEGORY_CODE") == "36")),col("SBU3_WP_BREAKAGE_QTY")).when(((col("PRODUCT_TYPE_CODE") == "SBU3") & (col("PRODUCT_CATEGORY_CODE") != "36")),col("SBU3_BREAKAGE_QTY")))
df6 = df6.fillna( { 'TOTAL_BREAKAGE_TON':0 } )

# COMMAND ----------

# defining netloss ton and filling nulls with 0's
df7 = df6.withColumn("NET_LOSS_TON",when((col("PRODUCT_TYPE_CODE") == "SBU1"),col("SBU1_NET_LOSS")).when((col("PRODUCT_TYPE_CODE") == 
"SBU2"),col("SBU2_NET_LOSS")).when(((col("PRODUCT_TYPE_CODE") == "SBU3") & (col("PRODUCT_CATEGORY_CODE") == "36")),col("SBU3_WP_NET_LOSS")).when(((col("PRODUCT_TYPE_CODE") == "SBU3") & (col("PRODUCT_CATEGORY_CODE") != "36")),col("SBU3_NET_LOSS")))
df7 = df7.fillna( { 'NET_LOSS_TON':0 } )

# COMMAND ----------

# droppint SBU level qty columns
df7 = df7.drop("CI_TOTAL_SUPPLY_TON").drop("CI_NET_LOSS_TONS").drop("CI_TOTAL_BREAKAGE_TONS")
df7 = df7.drop("SBU1_SUPPLIED_QTY").drop("SBU1_BREAKAGE_QTY").drop("SBU1_NET_LOSS")
df7 = df7.drop("SBU2_SUPPLIED_QTY").drop("SBU2_BREAKAGE_QTY").drop("SBU2_NET_LOSS")
df7 = df7.drop("SBU3_WP_SUPPLIED_QTY").drop("SBU3_WP_BREAKAGE_QTY").drop("SBU3_WP_NET_LOSS")
df7 = df7.drop("SBU3_SUPPLIED_QTY").drop("SBU3_BREAKAGE_QTY").drop("SBU3_NET_LOSS")

# COMMAND ----------

# joining with inv break sheet lines data to fetch defect type code and invoice number for SBU1
bu1_df =df7.alias('df').join(inv_break_sheet_lines.groupBy("ID").agg(first("DEFECT_TYPE_CODE").alias("DEFECT_TYPE_CODE"),first("INVOICE_NO").alias("INVOICE_NO")).alias('sbu1'),on = [
  col('df.INVESTIGATION_NO') == col('sbu1.ID') 
],how='left').select(["df." + cols for cols in df7.columns] + [col("sbu1.DEFECT_TYPE_CODE").alias("SBU1_DEFECT_TYPE_CODE")] + [col("sbu1.INVOICE_NO").alias("SBU1_INVOICE_NO")] )

# COMMAND ----------

# joining with cmp break other lines data to fetch defect type code and invoice number for SBU2
bu2_df =bu1_df.alias('df').join(cmp_break_other_lines.groupBy("ID").agg(first("DEFECT_TYPE_CODE").alias("DEFECT_TYPE_CODE"),first("INVOICE_NO").alias("INVOICE_NO")).alias('sbu2'),on = [
  col('df.COMPENSATION_NO') == col('sbu2.ID') 
],how='left').select(["df." + cols for cols in bu1_df.columns] + [col("sbu2.DEFECT_TYPE_CODE").alias("SBU2_DEFECT_TYPE_CODE")] + [col("sbu2.INVOICE_NO").alias("SBU2_INVOICE_NO")]  )

# COMMAND ----------

# joining with cmp sup det bu3 data to fetch defect type code and invoice number for SBU3
bu3_df =bu2_df.alias('df').join(cmp_sup_det_bu3.groupBy("ID").agg(first("DEFECTTYPE").alias("DEFECT_TYPE_CODE"),first("INVOICENO").alias("INVOICE_NO")).alias('sbu3'),on = [
  col('df.COMPENSATION_NO') == col('sbu3.ID') 
],how='left').select(["df." + cols for cols in bu2_df.columns] + [col("sbu3.DEFECT_TYPE_CODE").alias("SBU3_DEFECT_TYPE_CODE")] + [col("sbu3.INVOICE_NO").alias("SBU3_INVOICE_NO")] )

# COMMAND ----------

# generating invoice no field value
invoice_df = bu3_df.withColumn("INVOICE_NO",when((col("PRODUCT_TYPE_CODE") == "SBU1"),col("SBU1_INVOICE_NO")).when((col("PRODUCT_TYPE_CODE") == 
"SBU2"),col("SBU2_INVOICE_NO")).when((col("PRODUCT_TYPE_CODE") == "SBU3"),col("SBU3_INVOICE_NO")))
invoice_df = invoice_df.drop("SBU1_INVOICE_NO").drop("SBU2_INVOICE_NO").drop("SBU3_INVOICE_NO")

# COMMAND ----------

# generating defect type code value
df9 = invoice_df.withColumn("DEFECT_TYPE_CODE",when((col("PRODUCT_TYPE_CODE") == "SBU1"),col("SBU1_DEFECT_TYPE_CODE")).when((col("PRODUCT_TYPE_CODE") == 
"SBU2"),col("SBU2_DEFECT_TYPE_CODE")).when((col("PRODUCT_TYPE_CODE") == "SBU3"),col("SBU3_DEFECT_TYPE_CODE")))

# COMMAND ----------

# dropping SBU level defect type code fields
df9 = df9.drop("SBU1_DEFECT_TYPE_CODE").drop("SBU2_DEFECT_TYPE_CODE").drop("SBU3_DEFECT_TYPE_CODE")

# COMMAND ----------

# fetching defect type name, product category code
dm_sbu3_c = df9.alias('df').join(defect_type_master.alias('dm'),on = [
  col('df.DEFECT_TYPE_CODE') == col('dm.DEFECT_TYPE_CODE'),col('df.PRODUCT_TYPE_CODE') == col('dm.PRODUCT_TYPE_CODE'),col('df.PRODUCT_CATEGORY_CODE') == col('dm.PRODUCT_CATEGORY_CODE') 
],how='left').select(["df." + cols for cols in df9.columns] + [col("dm.DEFECT_TYPE_NAME").alias("SBU3_DEFECT_TYPE_NAME")] + [col("dm.PRODUCT_CATEGORY_CODE").alias("SBU3_PRODUCT_CATEGORY_CODE")])

dm_sbu3_d = df9.alias('df').join(defect_type_master.alias('dm'),on = [
  col('df.DEFECT_TYPE_CODE') == col('dm.DEFECT_TYPE_CODE'),col('df.PRODUCT_TYPE_CODE') == col('dm.PRODUCT_TYPE_CODE'),col('df.PRODUCT_CATEGORY_CODE') == col('dm.PRODUCT_CATEGORY_CODE')
],how='left').select(["df." + cols for cols in df9.columns] + [col("dm.DEFECT_TYPE_NAME").alias("SBU3_DEFECT_TYPE_NAME")] + [col("dm.PRODUCT_CATEGORY_CODE").alias("SBU3_PRODUCT_CATEGORY_CODE")] ).where(sf.lower(defect_type_master.PRODUCT_CATEGORY_CODE).contains('_'))

dm_sbu3 = dm_sbu3_c.subtract(dm_sbu3_d)

# COMMAND ----------

# fetching defect type name, product category code
dm_sbu_c = dm_sbu3.alias('df').join(defect_type_master.alias('dm'),on = [
  col('df.DEFECT_TYPE_CODE') == col('dm.DEFECT_TYPE_CODE'),col('df.PRODUCT_TYPE_CODE') == col('dm.PRODUCT_TYPE_CODE'),col('df.PRODUCT_CATEGORY_CODE') == col('dm.PRODUCT_CATEGORY_CODE')
],how='left').select(["df." + cols for cols in dm_sbu3.columns] + [col("dm.DEFECT_TYPE_NAME").alias("SBU_DEFECT_TYPE_NAME")] + [col("dm.PRODUCT_CATEGORY_CODE").alias("SBU_PRODUCT_CATEGORY_CODE")] )

dm_sbu_d = dm_sbu3.alias('df').join(defect_type_master.alias('dm'),on = [
  col('df.DEFECT_TYPE_CODE') == col('dm.DEFECT_TYPE_CODE'),col('df.PRODUCT_TYPE_CODE') == col('dm.PRODUCT_TYPE_CODE'),col('df.PRODUCT_CATEGORY_CODE') == col('dm.PRODUCT_CATEGORY_CODE')
],how='left').select(["df." + cols for cols in dm_sbu3.columns] + [col("dm.DEFECT_TYPE_NAME").alias("SBU_DEFECT_TYPE_NAME")] + [col("dm.PRODUCT_CATEGORY_CODE").alias("SBU_PRODUCT_CATEGORY_CODE")] ).where(sf.lower(defect_type_master.PRODUCT_CATEGORY_CODE).contains('_'))

dm_sbu = dm_sbu_c.subtract(dm_sbu_d).drop(col("DEFECT_TYPE_CODE"))

# COMMAND ----------

# generating final defect type name and dropping sbu level defect type fields
df10 = dm_sbu.withColumn("DEFECT_TYPE_NAME",when((col("PRODUCT_TYPE_CODE") == "SBU1") | (col("PRODUCT_TYPE_CODE") == "SBU2"),col("SBU_DEFECT_TYPE_NAME")).otherwise(col("SBU3_DEFECT_TYPE_NAME")))
df10 = df10.drop("SBU3_DEFECT_TYPE_NAME").drop("SBU_DEFECT_TYPE_NAME").drop("SBU3_PRODUCT_CATEGORY_CODE").drop("SBU_PRODUCT_CATEGORY_CODE")

# COMMAND ----------

# fetching no of days to attend
days_df = df10.withColumn("NO_OF_DAYS", when(col('ATTENDED_DATE').isNull(),lit('')).otherwise(sf.datediff(sf.col('ATTENDED_DATE'),sf.col('COMPLAINT_REGISTRATION_DATE'))))
days_df = days_df.withColumn('NO_OF_DAYS',sf.abs(days_df.NO_OF_DAYS))

# COMMAND ----------

# finding no of days to compensation
comp_days = days_df.withColumn("NO_OF_DAYS_TO_COMPENSATION",when(col('COMPENSATION_DATE').isNull(),lit('')).otherwise(sf.datediff(sf.col('COMPENSATION_DATE'),sf.col('ATTENDED_DATE'))))
comp_days = comp_days.withColumn('NO_OF_DAYS_TO_COMPENSATION',sf.abs(comp_days.NO_OF_DAYS_TO_COMPENSATION))

# COMMAND ----------

# finding rejected date
rejected_df = comp_days.withColumn("REJECTED_DATE",when((col("FINAL_STATUS") == "Complaint Rejected"),col("COMPLAINT_DATE")).when((col("FINAL_STATUS") == "Investigation Rejected"),col("COMPLAINT_DATE")).when((col("FINAL_STATUS") == "Compensation Rejected"),col("COMPLAINT_DATE")))

# COMMAND ----------

# finding no of days for rejection
rej_days = rejected_df.withColumn("NO_OF_DAYS_FOR_REJECTION",when(col('REJECTED_DATE').isNull(),lit('')).otherwise(sf.datediff(sf.col('REJECTED_DATE'),sf.col('COMPLAINT_REGISTRATION_DATE'))))
rej_days = rej_days.withColumn('NO_OF_DAYS_FOR_REJECTION',sf.abs(rej_days.NO_OF_DAYS_FOR_REJECTION))

# COMMAND ----------

# finding compensation mode name
df11 = rej_days.alias('df').join(compensation_mode_master.alias('cm'),on = [
  col('df.COMPENSATION_MODE_CODE') == col('cm.COMPENSATION_MODE_CODE')]
,how = 'left').select(["df." + cols for cols in rej_days.columns] + [col("cm.COMPENSATION_MODE_NAME")] ).drop(col("df.COMPENSATION_MODE_CODE"))

# COMMAND ----------

# finding state description
df12 = df11.alias('df').join(state.alias('state'),on = [
  col('df.STATE_CODE') == col('state.STATE_CODE')
],how='left').select(["df." + cols for cols in df11.columns] + [col("state.STATE_DESC")])

# COMMAND ----------

# finding division name
df13 = df12.alias('df').join(division_master.alias('div'),on = [
  col('df.PRODUCT_CATEGORY_CODE') == col('div.DIVISION_CODE'),col('df.PRODUCT_TYPE_CODE') == col('div.PRODUCT_TYPE_CODE')
],how='left').select(["df." + cols for cols in df12.columns] + [col("div.DIVISION_NAME")] )

# COMMAND ----------

# finding compensation mode name
df14 = df13.alias('df').join(complaint_category_mas.alias('ccm'),on = [
  col('df.COMPLAINT_CATEGORY_CODE') == col('ccm.COMPLAINT_CATEGORY_CODE')
],how='left').select(["df." + cols for cols in df13.columns] + [col("ccm.COMPLAINT_CATEGORY_NAME")] ).drop(col("df.COMPLAINT_CATEGORY_CODE"))

# COMMAND ----------

# adding leading 0's to customer code
df14=df14.withColumn("CUSTOMER_CODE",lpad(col("CUSTOMER_CODE"),10,'0'))

# COMMAND ----------

# deriviing customer category for each SBU
sbu1_cat = df14.withColumn("SBU1_CUSTOMER_CATEGORY",when((col('END_CUSTOMER_DETAILS') == col('CUSTOMER_NAME')) |
 ((col('END_CUSTOMER_DETAILS').isNull()) & (col("SUBSTOCKIEST_NAME").isNull())) | (lower(col("END_CUSTOMER_DETAILS")) == "stockiest"),"DEALER").otherwise("SUB_DEALER"))
sbu2_cat = sbu1_cat.withColumn("SBU2_CUSTOMER_CATEGORY",lit('Customer'))
sbu3_cat = sbu2_cat.withColumn("SBU3_CUSTOMER_CATEGORY",lit('Distributor'))

# COMMAND ----------

# defining final customer category
df15 = sbu3_cat.withColumn("CUSTOMER_CATEGORY",when((col("PRODUCT_TYPE_CODE") == "SBU1"),col("SBU1_CUSTOMER_CATEGORY")).when((col("PRODUCT_TYPE_CODE") == 
"SBU2"),col("SBU2_CUSTOMER_CATEGORY")).when((col("PRODUCT_TYPE_CODE") == "SBU3"),col("SBU3_CUSTOMER_CATEGORY"))).drop('SBU1_CUSTOMER_CATEGORY').drop('SBU2_CUSTOMER_CATEGORY').drop('SBU3_CUSTOMER_CATEGORY')

# COMMAND ----------

#defining zone name
df16 = df15.alias('df').join(zone_df.alias("zone"), on = [col("df.STATE_CODE") == col("zone.STATE_CODE"),
                                                          col("df.PRODUCT_CATEGORY_CODE") == col("zone.DIVISION_CODE"),
                                                         col("df.PRODUCT_TYPE_CODE") == col("SBU")], how = 'left').select(["df." + cols for cols in df15.columns] +  
[col("zone.ZONE").alias("ZONE_NAME")])
df17 = df16.withColumn("DIVISION_CODE",col('PRODUCT_CATEGORY_CODE'))

# COMMAND ----------

# defining registered empoyee role
df18 = df17.alias('df').join(employee_role.alias('er'),on = [
 (col('df.REGISTERED_BY') == col('er.EMPLOYEE_CODE'))
],how='left').select(["df." + cols for cols in df17.columns] + [col("er.ROLE").alias('REGISTERED_EMPLOYEE_ROLE')])

# COMMAND ----------

# defining sales person role
df19 = df18.alias('df').join(employee_role.alias('er'),on = [  
  (col('df.SALES_REPRESENTATIVE_CODE') == col('er.EMPLOYEE_CODE'))
],how='left').select(["df." + cols for cols in df18.columns] + [col("er.ROLE").alias('SALES_PERSON_ROLE')])

# COMMAND ----------

# defining final date to use for Surrogate key
df20 = df19.withColumn("FINAL_DATE",when((col("PRODUCT_TYPE_CODE") == "SBU1") & ((col("FINAL_STATUS") == "Complaint Rejected") | (col("FINAL_STATUS") == "Compensation Rejected") | (col("FINAL_STATUS") == "Investigation Rejected")),col('REJECTED_DATE'))
                       .when((col("PRODUCT_TYPE_CODE") == "SBU1") & ((col("FINAL_STATUS") == "Under Investigation") | (col("FINAL_STATUS") == "Complaint Assigned") | (col("FINAL_STATUS") == "Complaint Registered") | (col("FINAL_STATUS") == "Complaint Approved")),col('COMPLAINT_REGISTRATION_DATE'))
                       .when((col("PRODUCT_TYPE_CODE") == "SBU1") & ((col("FINAL_STATUS") == "Compensation Under Review") | (col("FINAL_STATUS") == "Investigation Approved. Compensation under review") | (col("FINAL_STATUS") == "Investigated and under review")),col('ATTENDED_DATE'))
                       .when((col("PRODUCT_TYPE_CODE") == "SBU1") & (col('FINAL_STATUS')=='Compensation Approved and sent'),col('COMPENSATION_DATE')).otherwise(col('COMPLAINT_REGISTRATION_DATE')))

# COMMAND ----------

# type casting final date field
df20 = df20.withColumn('FINAL_DATE', df20['FINAL_DATE'].cast(DateType()))

# COMMAND ----------

# defining investigation done by employee role
df22 = df20.alias('df').join(employee_role.alias('er'),on = [  
  (col('df.INVESTIGATION_DONE_BY') == col('er.EMPLOYEE_CODE'))
],how='left').select(["df." + cols for cols in df20.columns] + [col("er.ROLE").alias('INVESTIGATION_DONE_BY_ROLE')])

# COMMAND ----------

# defining registered by name for sbu1
sbu1_registered = df22.alias('df').join(cms_employee_master.alias('cem'),on = [  
  (col('df.REGISTERED_BY') == col('cem.EMPLOYEE_CODE'))
],how='left').select(["df." + cols for cols in df22.columns] + [col("cem.EMPLOYEE_NAME").alias('SBU1_REGISTERED_BY_NAME')])

# COMMAND ----------

# defining SBU 
dims_employee_details = dims_employee_details.withColumn("SBU",when(col('Sales_Organisation')=="1000","SBU1").when(col('Sales_Organisation')=="2000","SBU2").when(col('Sales_Organisation')=="3000","SBU3"))

# COMMAND ----------

# defining registered by name for sbu2 and 3
sbu23_registered = sbu1_registered.alias('df').join(dims_employee_details.alias('ded'),on = [  
  (col('df.REGISTERED_BY') == col('ded.EmployeeCode')),(col("df.PRODUCT_TYPE_CODE")==col("ded.SBU"))
],how='left').select(["df." + cols for cols in sbu1_registered.columns] + [col("ded.EmployeeName").alias('SBU23_REGISTERED_BY_NAME')])

# COMMAND ----------

# defining final registered by name
df23 = sbu23_registered.withColumn("REGISTERED_BY_NAME",when(col("SBU1_REGISTERED_BY_NAME").isNotNull(),col("SBU1_REGISTERED_BY_NAME")).otherwise(col("SBU23_REGISTERED_BY_NAME")))
df23 = df23.drop('SBU1_REGISTERED_BY_NAME').drop('SBU23_REGISTERED_BY_NAME')

# COMMAND ----------

# defining sales person name for sbu1
sbu1_sales = df23.alias('df').join(cms_employee_master.alias('cem'),on = [  
  (col('df.SALES_REPRESENTATIVE_CODE') == col('cem.EMPLOYEE_CODE'))
],how='left').select(["df." + cols for cols in df23.columns] + [col("cem.EMPLOYEE_NAME").alias('SBU1_SALES_PERSON_NAME')])

# COMMAND ----------

# defining sales person name for sbu2 and 3
sbu23_sales = sbu1_sales.alias('df').join(dims_employee_details.alias('ded'),on = [  
  (col('df.SALES_REPRESENTATIVE_CODE') == col('ded.EmployeeCode')),(col("df.PRODUCT_TYPE_CODE")==col("ded.SBU"))
],how='left').select(["df." + cols for cols in sbu1_sales.columns] + [col("ded.EmployeeName").alias('SBU23_SALES_PERSON_NAME')])

# COMMAND ----------

# df24 = sbu23_sales.withColumn("SALES_PERSON_NAME",when(col('PRODUCT_TYPE_CODE') == 'SBU1',col('SBU1_SALES_PERSON_NAME')).otherwise(col('SBU23_SALES_PERSON_NAME')))
df24 = sbu23_sales.withColumn("SALES_PERSON_NAME",when(col("SBU1_SALES_PERSON_NAME").isNotNull(),col("SBU1_SALES_PERSON_NAME")).otherwise(col("SBU23_SALES_PERSON_NAME")))
df24 = df24.drop('SBU1_SALES_PERSON_NAME').drop('SBU23_SALES_PERSON_NAME')

# COMMAND ----------

# defining investigation done by name for sbu1
sbu1_inv_done = df24.alias('df').join(cms_employee_master.alias('cem'),on = [  
  (col('df.INVESTIGATION_DONE_BY') == col('cem.EMPLOYEE_CODE'))
],how='left').select(["df." + cols for cols in df24.columns] + [col("cem.EMPLOYEE_NAME").alias('SBU1_INVESTIGATION_DONE_BY_NAME')])

# COMMAND ----------

# defining investigation done by name for sbu2 and 3
sbu23_inv_done = sbu1_inv_done.alias('df').join(dims_employee_details.alias('ded'),on = [  
  (col('df.INVESTIGATION_DONE_BY') == col('ded.EmployeeCode'))
],how='left').select(["df." + cols for cols in sbu1_inv_done.columns] + [col("ded.EmployeeName").alias('SBU23_INVESTIGATION_DONE_BY_NAME')])

# COMMAND ----------

# defining investigation done by name field
df25 = sbu23_inv_done.withColumn("INVESTIGATION_DONE_BY_NAME",when(col('PRODUCT_TYPE_CODE') == 'SBU1',col('SBU1_INVESTIGATION_DONE_BY_NAME')).otherwise(col('SBU23_INVESTIGATION_DONE_BY_NAME')))

df25 = sbu23_inv_done.withColumn("INVESTIGATION_DONE_BY_NAME",when(col("SBU1_INVESTIGATION_DONE_BY_NAME").isNotNull(),col("SBU1_INVESTIGATION_DONE_BY_NAME")).otherwise(col("SBU23_INVESTIGATION_DONE_BY_NAME")))
df25 = df25.drop('SBU1_INVESTIGATION_DONE_BY_NAME').drop('SBU23_INVESTIGATION_DONE_BY_NAME')

# COMMAND ----------

# defining rls string for SBU field
df26 = df25.withColumn("SBU_RLS",when(col('PRODUCT_TYPE_CODE') == 'SBU1',lit('SBU 1')).when(col('PRODUCT_TYPE_CODE') == 'SBU2',lit('SBU 2')).when(col('PRODUCT_TYPE_CODE') == 'SBU3',lit('SBU 3')))

# COMMAND ----------

# defining rls string for division level
RLS_df = df26.withColumn("RLS",sf.concat(col('SBU_RLS'),lit("_"),col('DIVISION_NAME'))).drop('DIVISION_CODE')

# COMMAND ----------

# defining no of days to close complaint
df27 = RLS_df.withColumn("NO_OF_DAYS_TO_CLOSE_COMPLAINT",when(col('COMPENSATION_DATE').isNull(),lit('')).otherwise(sf.datediff(sf.col('COMPENSATION_DATE'),sf.col('COMPLAINT_REGISTRATION_DATE'))))
df27 = df27.withColumn('NO_OF_DAYS_TO_CLOSE_COMPLAINT',sf.abs(df27.NO_OF_DAYS_TO_CLOSE_COMPLAINT))

# COMMAND ----------

# defining party type description
df28 = df27.alias("l_df").join(party_type.alias("r_df"),on=[col("l_df.PARTY_TYPE") == col("r_df.ID")],how='left').select(["l_df." + cols for cols in df27.columns] + [col("r_df.PARTY_TYPE").alias("PARTY_TYPE_DESC")]).drop("PARTY_TYPE")

# COMMAND ----------

# defining complaint type description
df29 = df28.alias("l_df").join(cms_type_of_complaints.alias("r_df"),on=[col("l_df.COMPLAINT_TYPE") == col("r_df.ID")],how='left').select(["l_df." + cols for cols in df28.columns] + [col("r_df.COMPLAINT_TYPE").alias("COMPLAINT_TYPE_DESC")]).drop(*["PARTY_TYPE","COMPLAINT_TYPE"])

# COMMAND ----------

# defining netloss value
df30 = df29.withColumn("NETLOSS_SBU2",when(((col("PRODUCT_TYPE_CODE") == 'SBU2') & (col("DIVISION_NAME") == 'Blocks')),col("COMPENSATION_IN_CUBIC_METER")).when((col("PRODUCT_TYPE_CODE") == 'SBU2'),col("COMPENSATION_IN_TONS")).otherwise(lit(0)))

df31 = df30.withColumn("NET_LOSS_TON",when((col("PRODUCT_TYPE_CODE") == 'SBU2'),col("NETLOSS_SBU2")).otherwise(col("NET_LOSS_TON")))

# COMMAND ----------

# writing final data to processed layer before generating surrogate key
df31.write.parquet(processed_location+'DIMS', mode='overwrite')

# COMMAND ----------

# surrogate mapping
def surrogate_mapping_hil(dim_fact_mapping_df,fact_table_name,fact_name, processed_db_name):  
  dim_fact_mapping = dim_fact_mapping_df.select("fact_table","fact_column", "dim_column","fact_surrogate","dim_surrogate", "dim_table").collect()
  select_condition=""
  join_condition=""
  count=0
  
  df30.createOrReplaceTempView("{}".format(fact_name))
  
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
  COMPLAINT_TRACKING_NO,
  END_CUSTOMER_DETAILS as SECONDARY_CUSTOMER,
  CUSTOMER_CATEGORY,  
COMPLAINT_CODE,
CUSTOMER_CODE,
CUSTOMER_NAME,
INVOICE_NO,
DOC_STATUS,
LOCATION_CODE,
CITY_CODE,
STATE_DESC,
SALES_REPRESENTATIVE_CODE as SALES_PERSON_CODE,
SALES_PERSON_ROLE,
PRODUCT_TYPE_CODE as BUSINESS_UNIT,
DIVISION_NAME as PRODUCT_TYPE,
COMPLAINT_REGISTRATION_DATE as REGISTERED_DATE,
INVESTIGATION_NO,
ATTENDED_DATE,
DATE_OF_VISIT,
INVESTIGATION_DONE_BY,
DELAY_DAYS,
DELAY_REASON,
TOTAL_SUPPLY_TON as SUPPLIED_QTY,
TOTAL_BREAKAGE_TON as BREAKAGE_DEFECT_QTY,
NET_LOSS_TON as NET_LOSS_QTY,
INVESTIGATION_STATUS,
FINAL_STATUS as COMPLAINT_STATUS,
COMPENSATION_STATUS,
COMPENSATION_NO,
COMPENSATION_DATE as COMPENSATION_APPROVED_DATE,
COMPENSATION_VALUE,
REMARKS,
CANCEL_BY,
CANCEL_DATE,
CANCEL_REMARKS as REJECTED_REMARKS,
COMPENSATION_MODE_NAME as COMPENSATION_NAME,
REGISTERED_BY,
REGISTERED_EMPLOYEE_ROLE,
CI_VISIT_DATE,
INVESTIGATED_DATE,
REJECTED_DATE,
NO_OF_DAYS,
NO_OF_DAYS_TO_COMPENSATION,
NO_OF_DAYS_FOR_REJECTION,
NO_OF_DAYS_TO_CLOSE_COMPLAINT,
COMPLAINT_CATEGORY_NAME as COMPLAINT_CATEGORY,
DEFECT_TYPE_NAME as DEFECT_TYPE,
ZONE_NAME,  
RLS,
INVESTIGATION_DONE_BY_ROLE,
REGISTERED_BY_NAME,
SALES_PERSON_NAME,
INVESTIGATION_DONE_BY_NAME,
SBU_RLS,
PARTY_TYPE_DESC,
COMPLAINT_TYPE_DESC,
SUBSTOCKIEST_CODE as PARTY_CODE,
SUBSTOCKIEST_NAME as PARTY_NAME,
SUBSTOCKIEST_ADDRESS as PARTY_ADDRESS,
SUBSTOCKIEST_NUMBER as PARTY_NUMBER,
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
final_df = surrogate_mapping_hil(csv_data,table_name,"DIMS",processed_db_name)

# COMMAND ----------

# writing data to processed layer after generating surrogate key
final_df.write.mode('overwrite').parquet(processed_location+"FACT_CMS")

# COMMAND ----------

# inserting log record to last execution details table
date_now = datetime.utcnow()
vals = "('" + table_name + "_" + date_now.strftime("%Y%m%d%H%M%S") + "', '" + table_name + "', 'FULL', '"+ last_processed_time_str + "','" + date_now.strftime("%Y-%m-%d %H:%M:%S") + "')"
db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
pmu.insert(db_connector, db_cursor, schema_name, "LAST_EXECUTION_DETAILS", "(UNIQUE_ID, FACT_NAME, LOAD_TYPE, LAST_EXECUTION_TIME, JOB_COMPLETED_TIME)", vals)
pmu.close(db_connector, db_cursor)
