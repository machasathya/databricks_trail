# Databricks notebook source
#Import Statements
from azure.storage.blob import BlobClient, ContainerClient
import os
import xlrd
import pandas as pd
import time
import csv

# COMMAND ----------

SHAREPOINT_LANDING_LOCATION = dbutils.widgets.get("SHAREPOINT_LANDING_LOCATION")
ADLS_LANDING_LOCATION = dbutils.widgets.get("ADLS_LANDING_LOCATION")
SBU_MAPPING_LOCATION = dbutils.widgets.get("SBU_MAPPING_LOCATION")
scope = dbutils.widgets.get("scope")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
storage_account_key = dbutils.secrets.get(scope = scope.strip(), key = "AZR-LS-ADLS-HIL-300")
module = dbutils.widgets.get("module")
file_name = dbutils.widgets.get("file_name")
storage_account_key = dbutils.secrets.get(scope = scope.strip(), key = "AZR-LS-ADLS-HIL-300")

# COMMAND ----------

# SHAREPOINT_LANDING_LOCATION = "/dbfs/mnt/bi_datalake/prod/sharepoint/Finance/MIS_INPUT/"

# ADLS_LANDING_LOCATION = "/dbfs/mnt/bi_datalake/prod/lnd/sharepoint/MIS_SUMMARY/"

# SBU_MAPPING_LOCATION = "/dbfs/mnt/bi_datalake/prod/Utils/SBU_MAPPING/SBU_DIVISIONS"

# storage_account_name = "azradlhil300"
# container_name = "hil-datalake"
# scope='AZR-DBR-KV-SCOPE-300'
# storage_account_key = dbutils.secrets.get(scope = scope.strip(), key = "AZR-LS-ADLS-HIL-300")
# module = "Finance"
# file_name = "June_FY21"

# COMMAND ----------

# MAGIC %run "./APP_CONSTANTS_MIS"

# COMMAND ----------

# MAGIC %run "./INPUT_MIS_CONVERSION_LOGIC"

# COMMAND ----------

# Extracting the year and month from the file name
input_mis_file_name = os.path.basename(file_name)
input_mis_file_name = input_mis_file_name.replace(".", "_")
input_mis_file_list = input_mis_file_name.split("_")
month_name = input_mis_file_list[0][0:3]
year = "20" + input_mis_file_list[1][2:4]
    
#Opening the file from the sharepoint landing location
file_name = file_name +".xlsx"
cell_workbook = xlrd.open_workbook(os.path.join(SHAREPOINT_LANDING_LOCATION,file_name))

   
#Output file path in the after the file is converted
output_path = "OUTPUT_MIS_FILE"
if not os.path.exists(output_path):
    os.mkdir(output_path)
local_file_name = "INPUT_MIS" + "_" + month_name + "_" + year + ".xlsx"
month_year = MONTH_MAPPING[month_name]+"-"+year
SHEET_NAMES = [
    [month_name, month_year]]

final_list = []

#Sbu mappings from the Utils file
# sbu_mapping = pd.read_csv(SBU_MAPPING_LOCATION,sep="\t",header=None)
# for index,row in sbu_mapping.iterrows():
#     x = row.values
# #     print(x)
#     main_list = x[0].strip("\n").split(",")
#     final_list.append(main_list)
# SBU_MAPPING = final_list
SBU_MAPPING = spark.sql("select * from {}.{}".format("cur_prod","MIS_SBU_PLANT_PRODUCT_MAPPING")).drop("LAST_UPDATED_DT_TS").collect()

#Calling the notebook which has the actual conversion logic
defining_col_width_headers(SHEET_NAMES,os.path.join(SHAREPOINT_LANDING_LOCATION,file_name),os.path.join(output_path,local_file_name),SBU_MAPPING)

#Creating the output file name with the timestamp and storing it in landing location
workbook_name = os.path.join(output_path,local_file_name)
# data_xls = pd.read_excel(workbook_name, 'Sheet1', dtype=str, index_col=None)
# timestamp = str(time.time()).replace(".", "")
# final_csv_file_name = file_name[:-5] + "_" + timestamp + ".csv"
# print(final_csv_file_name)
# csv_file = os.path.join(ADLS_LANDING_LOCATION,final_csv_file_name)
# data_xls.to_csv(csv_file, encoding='utf-8', index=False)
# log.warning("Completed the required conversion of excel format")
# print("Completed the required conversion of excel format")

try:
  excel_data = xlrd.open_workbook(workbook_name)
  file_content = excel_data.sheet_by_index(0)
  timestamp = str(time.time()).replace(".", "")
  final_csv_file_name = file_name[:-5] + "_" + timestamp + ".csv"
  csv_file = open("{}\{}".format(ADLS_LANDING_LOCATION, final_csv_file_name), 'w', encoding='utf-8')
  wr = csv.writer(csv_file)
  for rownum in range(file_content.nrows):
    wr.writerow(file_content.row_values(rownum))
  csv_file.close()  
except Exception as e:
  print(e)
log.warning("Completed the required conversion of excel format")
print("Completed the required conversion of excel format")
