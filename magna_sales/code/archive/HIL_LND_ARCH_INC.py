# Databricks notebook source
#import statements
from datetime import datetime

# COMMAND ----------

# reading landing layer path of the table from ADF
landing_path = dbutils.widgets.get("landing_location")

# COMMAND ----------

# taking the date on which this notebook is running
current_date = datetime.now().strftime('%Y%m%d_%H%M%S')

# COMMAND ----------

#replacing landing layer path with archive path and adding date to the path
arc_path = landing_path.replace('/lnd/','/arc/')
arc_path =  "{}{}/".format(arc_path,current_date)

# COMMAND ----------

# moving data from landing to archive location
try:
  dbutils.fs.mv(landing_path, arc_path, recurse=True)
except Exception as e:
  print(e)
