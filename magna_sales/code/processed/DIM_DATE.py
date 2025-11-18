# Databricks notebook source
# Import statements
import time
import datetime
import math
from math import remainder
import fiscalyear
from fiscalyear import *
from datetime import timedelta, datetime, date, time
from pyspark.sql.functions import col
from pyspark.sql import Row

# COMMAND ----------

start_dt = date(2015,4,1) # start date in dim_date table
end_dt = date(2025,12,31) # end date in dim_date table
l=[]

delta = end_dt - start_dt
i = 0
tmp_week = 1
j = 1
# loop used to generate all required values for each date
while i<=delta.days + 1:
  d={}
  dd=(start_dt + timedelta(days=i))
  D=date(dd.year,dd.month,dd.day)
  d["DATE_DT"]=D
  d["DATE_ID"]=int(''.join([str(dd.year),str(dd.month).zfill(2),str(dd.day).zfill(2)]))
  d["DAY_OF_YR_NUM"]=int(D.strftime("%j"))
  d["DAY_ID"]=int(str(D.year)+str(d["DAY_OF_YR_NUM"]).zfill(3))
  d["DAY_NAME"]=D.strftime("%A")
  d["DAY_OF_MONTH_NUM"]=int(D.strftime("%d"))
  d["WEEK_OF_YR_NUM"]=int(D.strftime("%U"))
  d["WEEK_ID"]=int(str(D.year)+str(d["WEEK_OF_YR_NUM"]).zfill(2))
  d["MONTH_OF_YR_NUM"]=int(D.strftime("%m"))
  d["MONTH_OF_YR_NAME"]=D.strftime("%B")
  d["MONTH_ID"]=int(str(D.year)+str(d["MONTH_OF_YR_NUM"]).zfill(2))
  d["QTR_ID"]=math.ceil(D.month/3)
  d["QTR_OF_YR_NUM"]=int(str(D.year)+str(d["QTR_ID"]))
  d["YR_NAME"]=D.year
  
  t={4:1,5:2,6:3,7:4,8:5,9:6,10:7,11:8,12:9,1:10,2:11,3:12}
  if 1 <= int(d["MONTH_OF_YR_NUM"]) <= 3:
      setup_fiscal_calendar(start_year='previous', start_month=4, start_day=1)
      FY = FiscalDate(D.year, D.month, D.day)
      d["FY_NAME"] = int(str(FY.prev_fiscal_year).strip('FY'))
  else:
      setup_fiscal_calendar(start_year='same', start_month=4, start_day=1)
      FY = FiscalDate(D.year, D.month, D.day)
      d["FY_NAME"] = FY.fiscal_year
  fy_start_date = FiscalYear(d["FY_NAME"]).start
  fy_end_date = FiscalYear(d["FY_NAME"]).end
  start_date = date(fy_start_date.year, fy_start_date.month, fy_start_date.day)
  start_week = int(start_date.strftime("%U")) - 1
  d["FY_MONTH_OF_YR_NUM"]=t.get(D.month)
  if 4 <= int(d["MONTH_OF_YR_NUM"]) <= 12:
    d["FY_WEEK_OF_YR_NUM"]=int(d["WEEK_OF_YR_NUM"]) - start_week
  else:
    d["FY_WEEK_OF_YR_NUM"]=int(d["WEEK_OF_YR_NUM"]) + 40
  d["FY_WEEK_ID"]=int(str(d["FY_NAME"])+str(d["FY_WEEK_OF_YR_NUM"]).zfill(2))
  d["FY_MONTH_ID"]=int(str(d["FY_NAME"])+str(d["FY_MONTH_OF_YR_NUM"]).zfill(2))
  d["FY_QTR_ID"]=FY.quarter
  d["FY_QTR_NAME"]='Q'+str(d["FY_QTR_ID"])
  d["FY_QTR_OF_YR_NUM"]=int(str(d["FY_NAME"])+str(d["FY_QTR_ID"]))
  if 1 <= d["FY_MONTH_OF_YR_NUM"] <= 6:
    d["FY_HY_NAME"]='H1'
  else:
    d["FY_HY_NAME"]='H2'
  d["FY_HF_YR_OF_YR"]=d["FY_HY_NAME"]+' '+str(d["FY_NAME"]+1)
  d["FY_QTR_OF_YR"]=d["FY_QTR_NAME"]+' '+str(d["FY_NAME"]+1)
  d["FY_MONTH_OF_YR"]=d["MONTH_OF_YR_NAME"]+' '+str(d["YR_NAME"])
  d["FY_DAY_OF_MONTH"]=str(d["DAY_OF_MONTH_NUM"])+' '+d["MONTH_OF_YR_NAME"]
  if 1<=d["DAY_OF_MONTH_NUM"]<=7:
    d["WK_NAME"]='W1'
  elif 8 <= d["DAY_OF_MONTH_NUM"] <= 14:
    d["WK_NAME"]='W2'
  elif 15 <= d["DAY_OF_MONTH_NUM"] <= 21:
    d["WK_NAME"]='W3'
  else:
    d["WK_NAME"]='W4'
  d["FY_WK_OF_MONTH_NAME"]=d["WK_NAME"]+' '+d["MONTH_OF_YR_NAME"]
  if tmp_week != int(d["WK_NAME"][1:]):
    j += 1
    tmp_week = int(d["WK_NAME"][1:])
  d["WK_SORT"] = j
  l.append(d)
  i=i+1

# COMMAND ----------

# creating spark dataframe from the final list
df=spark.createDataFrame(Row(**x) for x in l)
df = df.sort(col("DATE_ID").asc())

# COMMAND ----------

# Writing the final dataframe data to cur location
df.coalesce(1).write.format('delta').mode('overwrite').save("dbfs:/mnt/bi_datalake/prod/cur/DIM_DATE/")
