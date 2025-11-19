# Databricks notebook source
# Import statements
import requests,json

# COMMAND ----------

# Commands used to fetch the input values from ADF
scope = dbutils.widgets.get("scope")
tenant_id = dbutils.secrets.get(scope = scope.strip(), key = "tenantID")
client_id = dbutils.secrets.get(scope = scope.strip(), key = "appID") 
client_secret = dbutils.secrets.get(scope = scope.strip(), key = "appKey")

# COMMAND ----------

# Script used to generate the sharepoint API token
try:
    Access_Token_url = "https://accounts.accesscontrol.windows.net/{}/tokens/OAuth/2".format(tenant_id)
    Access_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    Access_response = requests.post(Access_Token_url,
                                    data="grant_type=client_credentials&client_id={}@{}&client_secret={}&resource=00000003-0000-0ff1-ce00-000000000000/hilin.sharepoint.com@2eef0204-6291-4a60-94ce-1d1151299af6".format(client_id,tenant_id,client_secret),
                                    headers=Access_headers)
    Access_value = json.loads(Access_response.text)
except Exception as e:
    print(e)

# COMMAND ----------

# Exit command
dbutils.notebook.exit(Access_value["access_token"])
