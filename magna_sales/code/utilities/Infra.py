# Databricks notebook source
# Command used to put the init script file in DBFS
dbutils.fs.put("/databricks/scripts/pyodbc-install.sh","""
#!/bin/bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install msodbcsql17
apt-get -y install unixodbc-dev
sudo apt-get install python3-pip -y
pip3 install --upgrade pyodbc""", True)

# COMMAND ----------

# Script used to mount ADLS to DBFS
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "3a869198-5d56-462e-9385-41b9a1e6fb6c",
           "fs.azure.account.oauth2.client.secret": "@YzS0mxvvKV-PXV@L97L6J-=:1ma?hWz",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/2eef0204-6291-4a60-94ce-1d1151299af6/oauth2/token"
          }

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://hil-datalake@azradlhil300.dfs.core.windows.net/",
  mount_point = "/mnt/bi_datalake",
  extra_configs = configs)

# COMMAND ----------

# Command used to unmount ADLS from DBFS
dbutils.fs.unmount("/mnt/bi_datalake")

# COMMAND ----------

# Script used to generate ddls
dbs = spark.catalog.listDatabases()
for db in dbs:
  f = open("/dbfs/mnt/table_ddl_{}.ddl".format(db.name), "w")
  tables = spark.catalog.listTables(db.name)
  for t in tables:
    DDL = spark.sql("SHOW CREATE TABLE {}.{}".format(db.name, t.name))
    f.write(DDL.first()[0])
    f.write("\n")
f.close()
