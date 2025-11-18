# Databricks notebook source
#Import Statements
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobClient, ContainerClient
import os
import xlrd
import pandas as pd
import logging
import time
import csv
import datetime

log = logging.getLogger('excel_formatting')

# COMMAND ----------

SHAREPOINT_LANDING_LOCATION = dbutils.widgets.get("SHAREPOINT_LANDING_LOCATION")
ADLS_LANDING_LOCATION = dbutils.widgets.get("ADLS_LANDING_LOCATION")
DATE_MAPPING_LOCATION = dbutils.widgets.get("DATE_MAPPING_LOCATION")
scope = dbutils.widgets.get("scope")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
storage_account_key = dbutils.secrets.get(scope = scope.strip(), key = "AZR-LS-ADLS-HIL-300")
module = dbutils.widgets.get("module")
file_name = dbutils.widgets.get("file_name")
storage_account_key = dbutils.secrets.get(scope = scope.strip(), key = "AZR-LS-ADLS-HIL-300")

# COMMAND ----------

# SHAREPOINT_LANDING_LOCATION = "/dbfs/mnt/bi_datalake/prod/sharepoint/Finance/WC_ROCE"

# ADLS_LANDING_LOCATION = "/dbfs/mnt/bi_datalake/prod/lnd/sharepoint/WC_ROCE"

# DATE_MAPPING_LOCATION = "/dbfs/mnt/bi_datalake/prod/Utils/DATE_MAPPING/date_mapping"

# scope='AZR-DBR-KV-SCOPE-300'

# storage_account_name = "azradlhil300"
# container_name = "hil-datalake"
# storage_account_key = dbutils.secrets.get(scope = scope.strip(), key = "AZR-LS-ADLS-HIL-300")
# module = "Finance"
# file_name = "WC_ROCE_2020"

# COMMAND ----------

# SHAREPOINT_LANDING_LOCATION = "/dbfs/mnt/bi_datalake/dev/sharepoint/Finance/WC_ROCE"

# ADLS_LANDING_LOCATION = "/dbfs/mnt/bi_datalake/dev/sharepoint/Finance/WC_ROCE/"

# DATE_MAPPING_LOCATION = "/dbfs/mnt/bi_datalake/prod/Utils/DATE_MAPPING/date_mapping"

# scope='AZR-DBR-KV-SCOPE-300'

# storage_account_name = "azradlhil300"
# container_name = "hil-datalake"
# storage_account_key = dbutils.secrets.get(scope = scope.strip(), key = "AZR-LS-ADLS-HIL-300")
# module = "Finance"
# file_name = "WC_ROCE_2021"

# COMMAND ----------

# MAGIC %run "./APP_CONSTANTS_WC_ROCE"

# COMMAND ----------

# MAGIC %run "./WC_ROCE_CONVERSION_LOGIC"

# COMMAND ----------

try:
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", storage_account_name), credential=storage_account_key)
    file_system_client = service_client.get_file_system_client(file_system=container_name)
except Exception as e:
  
    print(e)

# COMMAND ----------

# extracting the year from the WC_ROCE file based on the file name given
input_mis_file_name = os.path.basename(file_name)
input_mis_file_name = input_mis_file_name.replace(".", "_")
input_mis_file_list = input_mis_file_name.split("_")
year = int(input_mis_file_list[2])

#creating the ouput folder where the converted wc roce file has to be stored
log.warning("Started Conversion of Excel file")
output_path = "OUTPUT_WC_ROCE"
if not os.path.exists(output_path):
    os.mkdir(output_path)
local_file_name = file_name + ".xlsx"
last_year = int(year)-1

# opening the file from the sharepoint landing location
file_name = file_name +".xlsx"
cell_workbook = xlrd.open_workbook(os.path.join(SHAREPOINT_LANDING_LOCATION,file_name))

sheets = cell_workbook.sheets()
print(sheets)
test = []

final_list = []

DATE_MAPPING= []

#Extracting the values from the DATE MAPPING loction
# date_mapping = pd.read_csv(DATE_MAPPING_LOCATION,sep="\t",header=None)
# for index,row in date_mapping.iterrows():
#     x = row.values
# #     print(x)
#     main_list = x[0].strip("\n").split(",")
#     final_list.append(main_list)
# DATE_MAPPING = final_list
# print(final_list)

date_range = pd.date_range(str(year)+'-04-01',str(year+1)+'-03-31', 
              freq='MS').strftime("%d-%m-%Y").tolist()

DATE_MAPPING = []
for each_date in date_range:
  sp = []
  temp_date = datetime.datetime.strptime(each_date,"%d-%m-%Y")
  month_str = temp_date.strftime("%b")
  sp.append(month_str)
  sp.append(each_date)
  DATE_MAPPING.append(sp) 
    
# calling the function in the WC_ROCE_CONVERSION_LOGIC notebook where the actual transformations of the excel file take place
defining_col_width_headers(DATE_MAPPING,os.path.join(SHAREPOINT_LANDING_LOCATION,file_name),os.path.join(output_path,local_file_name))

# After the transformations  are done the file will be stored in the output_path 
workbook_name = os.path.join(output_path,local_file_name)

#the output file generated is in xlsx file format, converting it into csv format
# data_xls = pd.read_excel(workbook_name, 'Sheet1', dtype=str, index_col=None)
# timestamp = str(time.time()).replace(".", "")
# final_csv_file_name = file_name[:-5] + "_" + timestamp + ".csv"

# # the output file is finally stored in the dbfs location which gets mounted to adls 
# csv_file = os.path.join(ADLS_LANDING_LOCATION,final_csv_file_name)
# print(csv_file)
# data_xls.to_csv(csv_file, encoding='utf-8', index=False)


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

# COMMAND ----------


