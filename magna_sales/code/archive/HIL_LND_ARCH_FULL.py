# Databricks notebook source
#import statements
from datetime import date

# COMMAND ----------

#fetching landing layer path of the table from ADF
landing_path = dbutils.widgets.get("landing_location")

# COMMAND ----------

#replacing landing layer path with archive path
arc_path = landing_path.replace('/lnd/','/arc/')

# COMMAND ----------

#moving data from landing path to archive path
try:
  dbutils.fs.mv(landing_path, arc_path, recurse=True)
except Exception as e:
  print(e)
