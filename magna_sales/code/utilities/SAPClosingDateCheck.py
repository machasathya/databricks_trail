# Databricks notebook source
# Import statements
from datetime import date, timedelta,datetime

# COMMAND ----------

# finding out sap closing month and year
now = datetime.today()

last_month_date = now.replace(day=1) - timedelta(days=1)

sap_closing_month = (last_month_date.strftime("%B")).upper()

sap_closing_year = last_month_date.strftime("%Y")

print(sap_closing_month,sap_closing_year)

# COMMAND ----------

# Fetching sap closing date and returning it to ADF
sap_closing_date = spark.sql("select SAP_CLOSING_DATE from {}.SAP_CLOSING where MONTH = '{}' and CALENDAR_YEAR = '{}'".format("cur_prod",sap_closing_month,sap_closing_year)).collect()
if sap_closing_date[0][0] is None:
  dbutils.notebook.exit("")
else:
  closing_date = sap_closing_date[0][0].strftime("%Y-%m-%d")
  dbutils.notebook.exit(closing_date)
