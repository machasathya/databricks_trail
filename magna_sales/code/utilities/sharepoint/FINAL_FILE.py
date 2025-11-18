# Databricks notebook source
# Improt statements
import requests
import xlrd
import json
import time
from pathlib import Path
import os
import pandas as pd
import csv
from azure.storage.filedatalake import DataLakeServiceClient
import logging
import pandas as pd
import datetime
import xmltodict,xlsxwriter
from datetime import timedelta  
import pyspark.sql.functions as F

# COMMAND ----------

now = datetime.datetime.now() + timedelta(seconds = 19800)
day_n = now.day
ist_zone = datetime.datetime.now() + timedelta(hours=5.5)
load_date = (ist_zone - timedelta(days=1)).strftime("%d-%m-%Y")
load_date

# COMMAND ----------

# Commands used to fetch the data from ADF
module = dbutils.widgets.get("Module")
file_name = dbutils.widgets.get("File_Name")
access_token = dbutils.widgets.get("Access_Token")
sharepoint_location = dbutils.widgets.get("Sharepoint_Location")
sharepoint_landing_location = dbutils.widgets.get("Sharepoint_Landing_Location")
tab_names = dbutils.widgets.get("Tab_Names").split(',')
table_names = dbutils.widgets.get("Table_Names").split(',')
adls_landing_location = dbutils.widgets.get("ADLS_Landing_Location")
storage_account_name = "azradlhil300"
container_name = "hil-datalake"
scope = dbutils.widgets.get("Scope")
storage_account_key = dbutils.secrets.get(scope = scope.strip(), key = "AZR-LS-ADLS-HIL-300")
last_processed_time = dbutils.widgets.get("last_processed_time")
last_processed_time = datetime.datetime.strptime(last_processed_time, "%Y-%m-%dT%H:%M:%SZ")

# COMMAND ----------

# Variables used to run the notebook manually
# module = "Finance"
# file_name = "WC_ROCE"
# sharepoint_location = "/hil/corporate/Finance/Shared%20Documents/CORPORATE_FINANCE/WC_ROCE"
# sharepoint_landing_location = "/dbfs/mnt/bi_datalake/prod/sharepoint"
# adls_landing_location = "/dbfs/mnt/bi_datalake/prod/lnd/sharepoint"
# tab_names = ''
# tab_names = tab_names.split(',')
# table_names = ''
# table_names = table_names.split(',')


# module = "Procurement"
# file_name = "SAVINGS_BUYER"
# sharepoint_location = "/hil/corporate/Procurement/Shared%20Documents/Savings_Buyer/"
# sharepoint_landing_location = "/dbfs/mnt/bi_datalake/prod/sharepoint"
# adls_landing_location = "/dbfs/mnt/bi_datalake/prod/lnd/sharepoint"
# tab_names = ''
# tab_names = tab_names.split(',')
# table_names = ''
# table_names = table_names.split(',')


# storage_account_name = "azradlhil300"
# container_name = "hil-datalake"
last_processed_time = "2020-01-01T10:35:00Z"
last_processed_time = datetime.datetime.strptime(last_processed_time, "%Y-%m-%dT%H:%M:%SZ")
# scope='AZR-DBR-KV-SCOPE-300'
# storage_account_key = dbutils.secrets.get(scope = scope.strip(), key = "AZR-LS-ADLS-HIL-300")
# access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Im5PbzNaRHJPRFhFSzFqS1doWHNsSFJfS1hFZyIsImtpZCI6Im5PbzNaRHJPRFhFSzFqS1doWHNsSFJfS1hFZyJ9.eyJhdWQiOiIwMDAwMDAwMy0wMDAwLTBmZjEtY2UwMC0wMDAwMDAwMDAwMDAvaGlsaW4uc2hhcmVwb2ludC5jb21AMmVlZjAyMDQtNjI5MS00YTYwLTk0Y2UtMWQxMTUxMjk5YWY2IiwiaXNzIjoiMDAwMDAwMDEtMDAwMC0wMDAwLWMwMDAtMDAwMDAwMDAwMDAwQDJlZWYwMjA0LTYyOTEtNGE2MC05NGNlLTFkMTE1MTI5OWFmNiIsImlhdCI6MTYyMjE3ODg2NywibmJmIjoxNjIyMTc4ODY3LCJleHAiOjE2MjIyNjU1NjcsImlkZW50aXR5cHJvdmlkZXIiOiIwMDAwMDAwMS0wMDAwLTAwMDAtYzAwMC0wMDAwMDAwMDAwMDBAMmVlZjAyMDQtNjI5MS00YTYwLTk0Y2UtMWQxMTUxMjk5YWY2IiwibmFtZWlkIjoiM2E4NjkxOTgtNWQ1Ni00NjJlLTkzODUtNDFiOWExZTZmYjZjQDJlZWYwMjA0LTYyOTEtNGE2MC05NGNlLTFkMTE1MTI5OWFmNiIsIm9pZCI6IjZmNGNiY2NmLWEyMjEtNDc0ZS05NjQyLWY0NWYzOWM5NzI2YSIsInN1YiI6IjZmNGNiY2NmLWEyMjEtNDc0ZS05NjQyLWY0NWYzOWM5NzI2YSIsInRydXN0ZWRmb3JkZWxlZ2F0aW9uIjoiZmFsc2UifQ.KrObiFJrB15Q4TVeHXhxfNI3e2ZQY_qFFXExmGxqGqCVaFe5lsbUBME0fLNUFkxmdGT_obLMYA5P1BeWNKSFSwEpkzBLwtvbv5s-8f69cmAeAEK58XNnJ_Jcxa65FX8v9bvjF0R4OGal2J2Lp_G8wkl8we82f1DubPCXS2A9SuxgaTM6EFJmBrYZviE188yyVkT9BP9OpFRvM5u_vCqXgYa3jqg7YN8B7saB8GwIg7NuWpZJTFRc-IOy08BvUd_eij7Tatq91NPiFfVl3wZsML_qTzt3d07-QRrCWgZvekD407zHCvQNabw9FKx9ZTk0oabXN8hvvzjNYLUYbZsy-A"

# COMMAND ----------

# Function used to establish connection to ADLS
def stor_acc_conn(storage_account_name,storage_account_key,container_name):
  try:
      service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
          "https", storage_account_name), credential=storage_account_key)
      file_system_client = service_client.get_file_system_client(file_system=container_name)
      return file_system_client
  except Exception as e:
      print(e)

# COMMAND ----------

# Funcion used to hit the API and get response
def api_get_response(url,Access_headers):
  response = requests.get(url,headers=Access_headers)
  return response.text

# COMMAND ----------

# Function used to parse XML to Json
def xml_to_json_parser(xml_response):
  xml_parse = xmltodict.parse(xml_response)
  str_parse = json.dumps(xml_parse)
  json_parse = json.loads(str_parse)
  return json_parse

# COMMAND ----------

# Function used to get the files that are modified between my last job run end time and current job run start time
def sharepoint_file_filter(file_name,sharepoint_location,access_token,last_processed_time):
  files_list = []
  Access_headers = {'Authorization': 'Bearer {}'.format(access_token)}
  
  if file_name in ['ALL_INDIA_CEMENT_PLANTS','MATERIAL_DEPARTMENT_MAPPING','PRODUCTION_PLAN','RESIN_PRICE','RM_BUDGETS','RM_NEW_VENDOR_DEVELOPMENT','GL_MASTER','SAP_CLOSING','STATE_LIST','DAILY_SALES_TRACKER','EVA','OUTSTANDING_AND_COLLECTIONS','FREIGHT_BUDGET_SBU1','NSR_SBU1','PLANT_SALES_TARGETS','FREIGHT_BUDGETS_SBU3','NSR_SBU3','PUTTY_MIS','P&F_MIS','RM_GROUPING_ASPIRE','MIS_SBU_PLANT_PRODUCT_MAPPING','PROCUREMENT_RLS','FINANCE_RLS']:
    url = "https://hilin.sharepoint.com/hil/corporate/{}/_api/Web/getfilebyserverrelativeurl('{}/{}.xlsx')/properties".format(module,sharepoint_location,file_name)
    response = api_get_response(url,Access_headers)
    json_out = xml_to_json_parser(response)
    last_mod_time_str = json_out["entry"]["content"]["m:properties"]["d:vti_x005f_timelastmodified"]["#text"]
    last_mod_time = datetime.datetime.strptime(last_mod_time_str,"%Y-%m-%dT%H:%M:%S")
    if last_mod_time > last_processed_time:
      files_list.append(sharepoint_location+"/"+file_name+".xlsx")
      
      
  elif file_name in ['WC_ROCE','SAVINGS_BUYER','ASPIRE','COST_TRACKER']:
    url = "https://hilin.sharepoint.com/hil/corporate/{}/_api/Web/getfolderbyserverrelativeurl('{}')/files".format(module,sharepoint_location,file_name)
    response = api_get_response(url,Access_headers)
    json_out = xml_to_json_parser(response)
    for each_value in json_out["feed"]["entry"]:
      url1 = each_value["id"]+"/properties"
      response1 = api_get_response(url1,Access_headers)
      json_out1 = xml_to_json_parser(response1)
      last_mod_time_str = json_out1["entry"]["content"]["m:properties"]["d:vti_x005f_timelastmodified"]["#text"]
      last_mod_time = datetime.datetime.strptime(last_mod_time_str,"%Y-%m-%dT%H:%M:%S")
      if last_mod_time > last_processed_time:
        start = url1.find("decodedurl=") + len("decodedurl=")
        end = url1.find(")/properties")
        req_string = url1[start:end]
        files_list.append(req_string[1:-1])
  
  elif file_name in ['MIS_INPUT']:
    url = "https://hilin.sharepoint.com/hil/corporate/{}/_api/Web/getfolderbyserverrelativeurl('{}')/folders".format(module,sharepoint_location,file_name)
    response = api_get_response(url,Access_headers)
    json_out = xml_to_json_parser(response)
    for each_value in json_out["feed"]["entry"]:
      url1 = each_value["id"] + "/folders"
      response1 = api_get_response(url1,Access_headers)
      json_out1 = xml_to_json_parser(response1)
      for each_value1 in json_out1["feed"]["entry"]:
          url2 = each_value1["id"]+"/files"
          response2 = api_get_response(url2,Access_headers)
          json_out2 = xml_to_json_parser(response2)
          if "entry" not in json_out2["feed"]:
              continue
          url3 = json_out2["feed"]["entry"]["id"]
          response3 = api_get_response(url3 + "/properties",Access_headers)
          json_out3 = xml_to_json_parser(response3)
          last_mod_time_str = json_out3["entry"]["content"]["m:properties"]["d:vti_x005f_timelastmodified"]["#text"]
          last_mod_time = datetime.datetime.strptime(last_mod_time_str,"%Y-%m-%dT%H:%M:%S")
          if last_mod_time > last_processed_time:
            start = url3.find("decodedurl=") + len("decodedurl=")
            end = url3.find(")/properties")
            req_string = url3[start:end]
            files_list.append(req_string[1:-1])
  return files_list   

# COMMAND ----------

# Function used to fetch the data from sharepoint .xlsx files and write the same to ADLS as an .xlsx file
def adls_xlsx_writer(module,file_to_load,sharepoint_landing_location):
  submit_headers = {'Content-Type': 'application/x-www-form-urlencoded',
                        'Authorization': 'Bearer ' + access_token}
  try:
    submit_Token_url = "https://hilin.sharepoint.com/hil/corporate/{}/_api/Web/getfilebyserverrelativeurl('{}')/$Value".format(
        module,file_to_load)
    Submit_response = requests.get(submit_Token_url, headers=submit_headers)
#     dbutils.fs.rm("dbfs:"+"{0}\{1}\{2}".format(sharepoint_landing_location,module,file_name)[5:].replace("\\","/"),recurse=True)
    print("{0}\{1}\{2}\{3}".format(sharepoint_landing_location,module,file_name,file_to_load[file_to_load.rfind("/")+1:]))
    binary_file = open("{0}\{1}\{2}\{3}".format(sharepoint_landing_location,module,file_name,file_to_load[file_to_load.rfind("/")+1:]), "wb")
    binary_file.write(Submit_response.content)
    binary_file.close()
  except Exception as e:
    print(e)

# COMMAND ----------

# Function used to parse the excel filess to .csv files and store them in landing location
def csv_parser(file_name,module,file_to_load,sharepoint_landing_location,tab_names,table_names,adls_landing_location):
  if file_name in ['ALL_INDIA_CEMENT_PLANTS','MATERIAL_DEPARTMENT_MAPPING','PRODUCTION_PLAN','RESIN_PRICE','RM_BUDGETS','RM_NEW_VENDOR_DEVELOPMENT','GL_MASTER','SAP_CLOSING','STATE_LIST','DAILY_SALES_TRACKER','EVA','OUTSTANDING_AND_COLLECTIONS','FREIGHT_BUDGET_SBU1','NSR_SBU1','PLANT_SALES_TARGETS','FREIGHT_BUDGETS_SBU3','NSR_SBU3','PUTTY_MIS','P&F_MIS','ASPIRE','RM_GROUPING_ASPIRE','MIS_SBU_PLANT_PRODUCT_MAPPING','PROCUREMENT_RLS','FINANCE_RLS']:
    try:
      excel_data = xlrd.open_workbook("{0}\{1}\{2}\{3}".format(sharepoint_landing_location,module,file_name,file_to_load[file_to_load.rfind("/")+1:]))
      counter = 0
      for each_tab in tab_names:
          file_content = excel_data.sheet_by_name(each_tab)
          original_file_name = table_names[counter]
          timestamp = str(time.time()).replace(".", "")
          final_csv_file_name = original_file_name + "_" + timestamp + ".csv"
          csv_file = open("{}\{}\{}".format(adls_landing_location , file_name+"_"+each_tab, final_csv_file_name), 'w', newline='', encoding='utf-8')
          wr = csv.writer(csv_file)
          for rownum in range(file_content.nrows):
              wr.writerow(file_content.row_values(rownum))
          csv_file.close()
          counter += 1
    except Exception as e:
      print(e)
      
  elif file_name in ['SAVINGS_BUYER']:
    try:
      excel_data = xlrd.open_workbook("{0}\{1}\{2}\{3}".format(sharepoint_landing_location,module,file_name,file_to_load[file_to_load.rfind("/")+1:]))
      file_content = excel_data.sheet_by_index(0)
      timestamp = str(time.time()).replace(".", "")
      final_csv_file_name = file_to_load[file_to_load.rfind("/")+1:-5] + "_" + timestamp + ".csv"
#       dbutils.fs.rm("dbfs:"+"{}\{}".format(adls_landing_location,file_name)[5:].replace("\\","/"),recurse=True)
      csv_file = open("{}\{}\{}".format(adls_landing_location,file_name, final_csv_file_name), 'w', newline='', encoding='utf-8')
      wr = csv.writer(csv_file)
      for rownum in range(file_content.nrows):
        wr.writerow(file_content.row_values(rownum))
      csv_file.close()  
    except Exception as e:
      print(e)
  elif file_name in ['WC_ROCE']:
    try:
#       print(file_to_load[file_to_load.rindex("/")+1:-5])
      dbutils.notebook.run("/code/utilities/sharepoint/WC_ROCE/WC_ROCE_CONNECT_TO_ADLS",120,{"SHAREPOINT_LANDING_LOCATION":"/dbfs"+"{0}\{1}\{2}".format(sharepoint_landing_location,module,file_name)[5:].replace("\\","/"),
                                                                          "ADLS_LANDING_LOCATION":adls_landing_location+"/WC_ROCE",
                                                                          "DATE_MAPPING_LOCATION":"/dbfs/mnt/bi_datalake/prod/Utils/DATE_MAPPING/date_mapping",
                                                                          "scope":scope,
                                                                         "storage_account_name":storage_account_name,
                                                                         "container_name":container_name,
                                                                         "module":module,
                                                                         "file_name":file_to_load[file_to_load.rindex("/")+1:-5]})
    except Exception as e:
      print(e)
  elif file_name in ['COST_TRACKER']:
    excel_data = pd.read_excel ("{0}\{1}\{2}\{3}".format(sharepoint_landing_location,module,file_name,file_to_load[file_to_load.rfind("/")+1:]),header=None,skip_rows=[1])
    plant_id = file_to_load[file_to_load.rfind("/")+1:-5]
    plant_id = plant_id[plant_id.rfind("_")+1:]
    excel_data = excel_data.astype("str")
    excel_data = excel_data.dropna()
    excel_data.columns = list(excel_data.loc[1])
    excel_data["PLANT_ID"] = plant_id
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    data = excel_data.values.tolist()
    col_data = []
    for iter_count,elem in enumerate(data[1]):
      if iter_count>=4 and iter_count<=34:
        col_data.append("PLANT_"+elem)
      elif iter_count>=35 and iter_count<=65:
        col_data.append("DEPOT_"+elem)
      else:
        col_data.append(elem)
    data[1]=col_data
    data = data[1:]
    data[0][len(data[0])-1]= "PLANT_ID"
    for pos,row in enumerate(data):
      data[pos] = ['' if (i.strip() in (u'\xa0','nan','') or '-' in i) else i for i in data[pos]]
    data[0] = [i.replace(".0","") if ('.0' in i) else i for i in data[0]]
    data[0].append('DATE')
    for pos,row in enumerate(data[1:]):
      data[pos+1].append(load_date)
    timestamp = str(time.time()).replace(".", "")
    final_csv_file_name = file_to_load[file_to_load.rfind("/")+1:-5] + "_" + timestamp + ".csv"
    csv_file = open("{}\{}\{}".format(adls_landing_location,file_name,final_csv_file_name), 'w', newline='', encoding='utf-8')
    wr = csv.writer(csv_file)
    with csv_file:    
        write = csv.writer(csv_file)
        write.writerows(data)
  
  elif file_name in ['MIS_INPUT']:
    try:
      print(file_to_load[file_to_load.rindex("/")+1:-5])
      dbutils.notebook.run("/code/utilities/sharepoint/INPUT_MIS/INPUT_MIS_CONNECT_TO_ADLS",120,{"SHAREPOINT_LANDING_LOCATION":"/dbfs"+"{0}\{1}\{2}".format(sharepoint_landing_location,module,file_name)[5:].replace("\\","/"),
                                                                          "ADLS_LANDING_LOCATION":adls_landing_location+"/MIS_SUMMARY",
                                                                          "SBU_MAPPING_LOCATION":"/dbfs/mnt/bi_datalake/prod/Utils/SBU_MAPPING/SBU_DIVISIONS",
                                                                          "scope":scope,
                                                                         "storage_account_name":storage_account_name,
                                                                         "container_name":container_name,
                                                                         "module":module,
                                                                         "file_name":file_to_load[file_to_load.rindex("/")+1:-5]})
    except Exception as e:
      print(e)

# COMMAND ----------

file_system_client = stor_acc_conn(storage_account_name,storage_account_key,container_name)
files_to_load = sharepoint_file_filter(file_name,sharepoint_location,access_token,last_processed_time)
if len(files_to_load)>0:
  for each_file in files_to_load:
    adls_xlsx_writer(module,each_file,sharepoint_landing_location)
    csv_parser(file_name,module,each_file,sharepoint_landing_location,tab_names,table_names,adls_landing_location)
