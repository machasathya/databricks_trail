# Databricks notebook source
#Folder names in the local after its downloaded from the blob
DATE_MAPPING_FOLDER = "DATE_MAPPING"
DATE_MAPPING_FILE_NAME = "date_mapping"

row_numbers = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 19, 20, 21, 22, 23, 25, 28, 29, 30, 31, 32, 35, 37, 40, 41,
               42]

# COMMAND ----------

HEADERS = ["HIL", "SBU", "PRODUCT_LINE", "DATE", "ACTUAL_OR_BUDGET"]

ACTUAL_BUDGET = ["BUDGET",
                 "ACTUAL"]

# COMMAND ----------

attributes = ["CASH_AND_BANK_BALANCES",
              "Raw_materials_components_Imported",
              "RAW_MATERIALS_COMPONENTS_INDIGENOUS",
              "STORES_SPARES",
              "STOCK_IN_PROCESS",
              "FINISHED_GOODS",
              "STOCK_IN_TRADE",
              "INVENTORIES",
              "RECEIVABLES",
              "ADVANCES",
              "DEPOSITS",
              "OTHER_CURRENT_ASSETS",
              "TOTAL_CURRENT_ASSETS",
              "ACCOUNTS_PAYABLE_IMPORT",
              "ACCOUNTS_PAYABLE_DOMESTIC",
              "DEPOSITS_AND_ADVANCES",
              "OTHER_CURRENT_LIABILITIES",
              "TOTAL_CURRENT_LIABILITIES",
              "NET_WORKING_CAPITAL",
              "GROSS_BLOCK",
              "Addition_deletion_during_the_year",
              "Total_Gross_Block",
              "Less_Accumlated_Depreciation",
              "NET_FIXED_ASSETS",
              "NET_CAPITAL_EMPLOYEED",
              "Revenue_net_of_discount",
              "EBIT",
              "ROCE",
              "Working_Capital_to_Revenue"
              ]

# COMMAND ----------

SBU_DIVISIONS = [
    ["HIL", "SBU 1", "AC SHEET"],
    ["HIL", "SBU 1", "FORTUNE"],
    ["HIL", "SBU 1", "CC SHEET"],
    ["HIL", "SBU 2", "BLOCKS"],
    ["HIL", "SBU 2", "PANELS"],
    ["HIL", "SBU 2", "FOB"],
    #["HIL", "SBU 3", "OTHERS"],
    ["HIL", "SBU 3", "PUTTY"],
    ["HIL", "SBU 3", "PIPES AND FITTINGS"],
    ["HIL", "OTHERS", "PARADOR"],
    ["HIL", "OTHERS", "ED"],
    ["HIL", "OTHERS", "WIND POWER"],
    ["HIL", "OTHERS", "CORP.OFFICE"],
]

# COMMAND ----------

SHEET_NAMES = ['AC SHEET', 'FORTUNE', 'CC SHEET', 'BLOCKS', 'PANELS', 'FOB', 'PUTTY', 'PIPES AND FITTINGS',
               'PARADOR', 'ED', 'WIND POWER', 'CORP. OFFICE']
