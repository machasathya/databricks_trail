# Databricks notebook source
# Import statements
import json 
from datetime import datetime,timedelta
import re

# COMMAND ----------

# Commands used to get the input data from ADF
file_list = json.loads(dbutils.widgets.get("file_list"))
file_date_extract_regex = dbutils.widgets.get("file_date_extract_regex")
file_date_format = dbutils.widgets.get("file_date_format")
end_date = dbutils.widgets.get("end_date")
start_date = dbutils.widgets.get("start_date")

# COMMAND ----------

# Script used to convert the GMT time to IST time
start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
end_date = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
start_date  = start_date + timedelta(seconds = 19800)
end_date =  end_date + timedelta(seconds = 19800)

# COMMAND ----------

# Script used to return the files which got updated/inserted between the last job end time and current job start time
table_files = list()
for file in file_list:
  if file["type"] == "File":
    file_time = re.search(file_date_extract_regex,file["name"])
    print(file_time)
    if file_time:
      file_time = file_time.group(1)
      ok = True
      if ok:
        try:
          file_extract_time = datetime.strptime(file_time, file_date_format)
          print(file_extract_time)
          if file_extract_time > start_date and file_extract_time <= end_date:
            table_files.append(file["name"])
        except ValueError as e:
          print(e)
          continue      
print(table_files)

# COMMAND ----------

# Exit command
dbutils.notebook.exit(table_files)
