# Databricks notebook source
#import statements
import pyodbc  
import json
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, IntegerType, FloatType, LongType, \
    DecimalType, DateType, TimestampType

# COMMAND ----------

# Input Json required for running manually
# data = {
#         "process_name": "SOURCE2LANDING",
#         "db_url": "tcp:azr-hil-sql-srvr-300.database.windows.net",
#         "db": "bi_analytics",
#         "user_name": "hil-admin@azr-hil-sql-srvr-300",
#         "table_name": "subprocess_run_meta",
#         "schema_name":"metadata",
#         "scope": "AZR-DBR-KV-SCOPE-300",
#         "insert": "True",
#         "op": "spe",
#         "load_type": "FULL",
#   "batch_id":"1",
#   "status":"SUCCESS",
#   "sub_process_name":"s2l_ADRC_FULL",
#   "landing_file_names":"[\"ADRC1.txt\",\"ADRC2.txt\"]"
#     }

# COMMAND ----------

# to fetch input data from ADF
try:
  op = dbutils.widgets.get("op")
  table_name = dbutils.widgets.get("table_name")
  process_name = dbutils.widgets.get("process_name")
  db_url = dbutils.widgets.get("db_url")
  db = dbutils.widgets.get("db")
  user_name = dbutils.widgets.get("user_name")
  scope = dbutils.widgets.get('scope')
  schema_name = dbutils.widgets.get('schema_name')
except Exception as e:  
  op = data['op']
  table_name = data['table_name']
  process_name = data['process_name']
  db_url = data['db_url']
  db = data['db']
  user_name = data['user_name']
  scope = data['scope']
  schema_name = data['schema_name']
  
password = dbutils.secrets.get(scope = scope.strip(), key = user_name.split("@")[0])

# COMMAND ----------

# ProcessMetadataUtility Class Decleration
class ProcessMetadataUtility():

    #Constructor Initialization
    def __init__(self):

        self.mapping = {
            "": StringType(),
            "varchar": StringType(),
            "text": StringType(),
            "nvarchar": StringType(),
            "binary": BinaryType(),
            "blob": StringType(),
            "int": IntegerType(),
            "float": FloatType(),
            "bigint": LongType(),
            "decimal": DecimalType(),
            "numeric": DecimalType(),
            "date": DateType(),
            "time": DateType(),
            "datetime": DateType(),
            "timestamp": TimestampType()
        }

    # Function used to connect to SQL DB
    def connect(self, host, user_name, password, database):
        try:
          connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+host+';DATABASE='+database+';UID='+user_name+';PWD='+password+''
          print("connection_string: " + connection_string)
          mydb = pyodbc.connect(connection_string)
          cursor = mydb.cursor()
#           print("DB connected!")
          return mydb, cursor
        except Exception as e:
            raise e
    
    # Function used to insert data to SQL DB
    def insert(self, db_connector, cursor,schema, table, columns, values):
        try:
            query = "INSERT INTO {}.{} {} VALUES {}".format(schema , table, columns, values)
            print("Insert query: " + query)
            cursor.execute(query)
            db_connector.commit()
            return True
        except Exception as e:
            raise e
    
    # Function used to Update data to SQL DB
    def update(self, db_connector, cursor,schema, table, condition, value):
        try:
            query = "UPDATE {}.{} SET {} WHERE {}".format(schema,table, value, condition)
            print("update query: " + query)
            cursor.execute(query)
            db_connector.commit()
            return True
        except Exception as e:
            raise e
            
    # Function used to get the last batch_id from process_run_meta table 
    def get_last_batch_id(self, cursor,schema, table):
        try:
            query = "SELECT BATCH_ID FROM {}.{} ORDER BY BATCH_ID DESC".format(schema,table)
            print("Batch_id QUERY: " + query)
            cursor.execute(query)
            res = cursor.fetchone()
            if res is not None and len(res) > 0:
                res = res[0]
            else:
                res = ""
            return res
        except Exception as e:
            raise e
            
    # Function used to get the last_processed_time from process_run_meta table
    def get_last_processed_time(self, cursor, schema,table, process_name):
        try:
            query = "SELECT LAST_PROCESSED_TIME FROM {schema}.{table} WHERE STATUS = 'SUCCESS' AND PROCESS_NAME like '{process_name}%' ORDER BY BATCH_ID DESC".format(
                schema = schema,table=table, process_name=process_name)
            print("Processed_time QUERY: " + query)
            cursor.execute(query)
            res = cursor.fetchone()
            if res is not None and len(res) > 0:
                res = res[0]
            else:
                res = ""
            return res
        except Exception as e:
            raise e
            
    # Function used to insert process log data to process_run_meta table
    def process_insert(self, db_connector, cursor, process_name, batch_id, status, start_time, table, load_type,schema):
        columns = '(PROCESS_NAME, BATCH_ID, STATUS, START_TIME, LAST_PROCESSED_TIME, LOAD_TYPE)'
        values = (process_name, batch_id, status, start_time, start_time, load_type)
        return self.insert(db_connector, cursor,schema, table, columns, values)

    # Function used to update process log data to process_run_meta table
    def process_update(self, db_connector, cursor, process_name, batch_id, status, completion_time, table,schema):
        value = "COMPLETION_TIME='{}', STATUS='{}'".format(completion_time, status)
        condition = "BATCH_ID={} AND PROCESS_NAME='{}'".format(batch_id, process_name)
        return self.update(db_connector, cursor,schema, table, condition, value)
    
    # Function used to insert subprocess log to subprocess_run_meta table
    def subprocess_insert(self, db_connector, cursor, process_name, batch_id, subprocess_name, status, start_time,
                          table,schema):
        columns = '(PROCESS_NAME, SUBPROCESS_NAME, BATCH_ID, STATUS, START_TIME)'
        values = (process_name, subprocess_name, batch_id, status, start_time)
        return self.insert(db_connector, cursor,schema, table, columns, values)

    # Function used to update subprocess log to subprocess_run_meta table  
    def subprocess_update(self, db_connector, cursor, process_name, batch_id, subprocess_name, status, completion_time,
                          table, file_list,schema):
        value = "COMPLETION_TIME='{}', STATUS='{}', FILE_LIST='{}'".format(completion_time, status, ','.join(
            file_name for file_name in file_list))
        condition = "BATCH_ID={} AND PROCESS_NAME='{}' AND SUBPROCESS_NAME='{}'".format(batch_id, process_name,
                                                                                        subprocess_name)
        return self.update(db_connector, cursor,schema, table, condition, value)
    
    # Function used to fetch the data from any of the sql table present in DB
    def get_data(self, cursor, value, col_lookup, column,schema_name):
        try:
            spark = SparkSession.builder.getOrCreate()
            sc = spark.sparkContext
            sql_context = SQLContext(sc)
            query = "SELECT * FROM {schema_name}.{tab} where {col}='{val}'".format(col=column, val=value, tab=col_lookup,schema_name = schema_name)
            print("Table column query: " + query)
            cursor.execute(query)
            rows = cursor.fetchall()
            query = "exec sp_columns {tab}, @table_owner ={schema_name}".format(tab=col_lookup,schema_name = schema_name)
            cursor.execute(query)
            columns_list = cursor.fetchall()
            schema_arr = []
            for column_tuple in columns_list:
                key = ""
                try:
                    key = column_tuple[5].lower()
                except Exception as e:
                    pass
                if key not in self.mapping.keys():
                    key = ""
                schema_arr.append(
                    StructField(column_tuple[3], self.mapping[key]))
            schema = StructType(schema_arr)
            df = sql_context.createDataFrame((tuple(r) for r in rows), schema)
            return df
        except Exception as e:
            raise e
            
    # Function used to close the SQL DB Connection
    def close(self, db_connector, cursor):
        cursor.close()
        db_connector.close()
        return True

# COMMAND ----------

# Class object initialization
pmu = ProcessMetadataUtility()

# COMMAND ----------

# Function used to create the values and insert them into the process_run_meta table
def process_start():
  # Fetching data from widgets
  try:
    insert = dbutils.widgets.get('insert')
    load_type = dbutils.widgets.get('load_type')
  except:
    insert = data['insert']
    load_type = data['load_type']
  
  # Establishing connection to SQL DB to fetch the last batch id and last load time
  db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
  previous_batch_id = pmu.get_last_batch_id(cursor=db_cursor,schema = schema_name, table=table_name)
  previous_last_load_time = pmu.get_last_processed_time(cursor=db_cursor,schema = schema_name, table=table_name, process_name=process_name)
  pmu.close(db_connector, db_cursor)
  print(previous_batch_id)
  
  # creating batch_id value for he current execution based on previous batch id
  if previous_batch_id is None or previous_batch_id == "":
    current_batch_id = 1
  else:
    current_batch_id = int(previous_batch_id) + 1
  
  # handling precious process incomplete state
  if previous_last_load_time == '' or previous_last_load_time is None or previous_batch_id is None or previous_batch_id == '':
    previous_last_load_time = datetime.strptime("2019-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ") 
  else:
    try:
      previous_last_load_time = datetime.strptime(previous_last_load_time, "%Y-%m-%dT%H:%M:%SZ")
    except:
      previous_last_load_time = datetime.strptime(previous_last_load_time, "%Y-%m-%d %H:%M:%S")
      
  date_now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
  
  # inserting records into process_run_meta table
  if insert == 'True':
    db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)  
    pmu.process_insert(db_connector=db_connector, cursor=db_cursor, process_name=process_name, batch_id=current_batch_id, status="RUNNING",
                                     start_time=date_now, table=table_name, load_type=load_type,schema = schema_name)
    pmu.close(db_connector, db_cursor)
    
  return {"batch_id":current_batch_id, "processs_name":process_name, "previous_batch_end": previous_last_load_time.strftime("%Y-%m-%dT%H:%M:%SZ"), "batch_start":date_now}

# COMMAND ----------

# Function used to create the values and update them into the process_run_meta table
def process_end():
  # Fetching data from ADF
  try:
    batch_id = int(dbutils.widgets.get("batch_id"))
    status = dbutils.widgets.get("status")
  except:
    status = data['status']
    batch_id = int(data['batch_id'])
  
  # Updating the status and completion time of the process into the process_run_meta table
  date_now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
  db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
  pmu.process_update(db_connector=db_connector, cursor=db_cursor, process_name=process_name, batch_id=batch_id, status=status,
                                   completion_time=date_now, table=table_name,schema = schema_name)
  pmu.close(db_connector, db_cursor)
  return "SUCCESS"

# COMMAND ----------

# Function used to create the values and insert them into the subprocess_run_meta table
def sub_process_start():
  # Fetching data from ADF
  try:
    batch_id = int(dbutils.widgets.get("batch_id"))
    sub_process_name = dbutils.widgets.get("sub_process_name")
  except:
    batch_id = int(data['batch_id'])
    sub_process_name = data['sub_process_name']
  
  # inserting the data into subprocess_run_meta table 
  db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
  date_now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
  pmu.subprocess_insert(db_connector=db_connector, cursor=db_cursor, process_name=process_name, batch_id=batch_id, status="RUNNING",
                                      start_time=date_now, subprocess_name=sub_process_name, table=table_name,schema = schema_name)
  pmu.close(db_connector, db_cursor)
  return {"subprocess_name":sub_process_name, "processs_name":process_name}

# COMMAND ----------

# Function used to create the values and insert them into the subprocess_run_meta table
def sub_process_end():
  # Fetching data from ADF
  try:
    status = dbutils.widgets.get("status")
    batch_id = int(dbutils.widgets.get("batch_id"))
    sub_process_name = dbutils.widgets.get("sub_process_name")
    try:
      landing_file_names = json.loads(dbutils.widgets.get('landing_file_names'))
    except:
      landing_file_names = []
  except:
    status = data['status']
    batch_id = int(data['batch_id'])
    sub_process_name = data['sub_process_name']
    landing_file_names = json.loads(data['landing_file_names'])
  
  # Updating the status and completion time of the subprocess into the subprocess_run_meta table
  db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
  date_now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
  pmu.subprocess_update(db_connector=db_connector, cursor=db_cursor, process_name=process_name, batch_id=batch_id, status=status,
                                      completion_time=date_now, subprocess_name=sub_process_name, table=table_name, file_list=landing_file_names,schema = schema_name
                       )
  pmu.close(db_connector, db_cursor)
  return "SUCCESS"

# COMMAND ----------

# Function used to get the last processed time from process_run_meta table
def last_process_time():
  db_connector, db_cursor = pmu.connect(host=db_url, user_name=user_name, password=password, database=db)
  previous_last_load_time = pmu.get_last_processed_time(cursor=db_cursor,schema = schema_name, table=table_name, process_name=process_name)
  previous_last_load_time = datetime.strptime(previous_last_load_time, "%Y-%m-%dT%H:%M:%SZ")
  pmu.close(db_connector, db_cursor)
  return {"previous_batch_end": previous_last_load_time.strftime("%Y-%m-%dT%H:%M:%SZ")}

# COMMAND ----------

if op == 'ps':
  output = process_start()
elif op == 'pe':
  output = process_end()
elif op == 'sps':
  output = sub_process_start()
elif op == 'spe':
  output = sub_process_end()
elif op == 'lpt':
  output = last_process_time()
else:
  raise Exception()

# COMMAND ----------

# Notebook exit command
dbutils.notebook.exit(output)
