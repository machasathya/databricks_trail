# Databricks notebook source
# DBTITLE 1,MIS
from azure.storage.blob import BlobClient, ContainerClient
import os

        

row_numbers = [4, 5, 7, 8, 9, 12, 13, 14, 16, 19, 20, 21, 22, 23, 24, 25, 28, 29, 30, 31, 35, 36, 37, 38, 39, 40, 42,
               44, 48, 49, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 62, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74,
               75,
               77, 79, 80, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95]

HEADERS = ["HIL", "SBU", "PRODUCT_LINE", "PLANT", "DATE", "ACTUAL_OR_BUDGET"]



ACTUAL_BUDGET = ["PY ACTUAL",
                 "BUDGET",
                 "ACTUAL"]

attributes = ["Production",
              "Sales",
              "Gross_Revenue",
              "DISCOUNT_COMMISSION",
              "Revenue",
              "Freight",
              "Packing",
              "TOTAL_DISTRIBUTION_EXPENSES",
              "NET_SALES_REALISATION",
              "MISC_RECEIPTS",
              "SERV_TO_OTHER_DIVNS",
              "FOB_Sale_to_AEROCON",
              "Total_Other_Income",
              "TOTAL_INCOME_SALES_INCOME",
              "INC_DEC_IN_INVENTORY",
              "Production_Value",
              "Imported_Raw_Material",
              "Indigenous_Raw_Material",
              "Traded_Goods",
              "Total_Raw_Material",
              "STORES_SPARES",
              "POWER_FUEL",
              "WAGES_BENEFITS",
              "GRM",
              "Contract_Wages",
              "TOTAL_OF_OTHER_VARIABLE_EXP_EXCLUDING_RM",
              "TOTAL_MATERIAL_COST_VERIABLE_COST",
              "Contribution",
              "Repairs_to_Buildings",
              "SALARIES_BENEFITS",
              "Insurance",
              "Vehicle_expenses",
              "Communication_Expenses",
              "G_Charges",
              "TRAV_CONV",
              "RATE_TAXES",
              "TOTAL_OTHER_MFG_EXPENSES",
              "R_D_EXPENSES",
              "C_O_EXPENSES_APPORTIONED",
              "TOTAL_MANUFACTURING_OVERHEADS",
              "Total_Manufacturing_Exp",
              "SGA",
              "RENT_RATES_TAXES",
              "EXCISE_DUTY_ON_CLOSING_STOCK",
              "Marketing_Staff_Salaries_and_benefits",
              "DIRECTORS_AUDITORS_FEES",
              "SAG_Vehicles_Expenses",
              "SAG_Communication_Expenses",
              "Advertisement",
              "General_Charges",
              "Travelling_Conveyance",
              "SAG_C_O_EXPENSES_APPORTIONED",
              "TOTAL_SELLING_ADMN_GEN_EXP",
              "HIL_C_O_EXPENSES_APPORTIONED",
              "Total_Expenses",
              "PBIDT",
              "GRP_C_O_EXPENSES_APPORTIONED",
              "EBITDA",
              "Interest_Long_Term",
              "Interest_Short_Term",
              "C_O_Interest_apportioned",
              "Total_Interest",
              "PBDT",
              "Depreciation",
              "C_O_Share_apportioned",
              "Total_Depreciation",
              "Operating_Profit",
              "OTHER_NON_OPERATING_INCOME",
              "NON_OPERATING_EXPENSES",
              "PBT"]

#Function to extract the date from date_mapping file

# SBU_DIVISIONS = [
#     ["HIL", "SBU 1", "AC SHEET", "HYDERABAD-FCP"],
#     ["HIL", "SBU 1", "AC SHEET", "FARIDABAD-FCP"],
#     ["HIL", "SBU 1", "AC SHEET", "JASIDIH-FCP"],
#     ["HIL", "SBU 1", "AC SHEET", "VIJAYAWADA-FCP"],
#     ["HIL", "SBU 1", "AC SHEET", "WADA-FCP"],
#     ["HIL", "SBU 1", "AC SHEET", "SATHARIYA-FCP"],
#     ["HIL", "SBU 1", "AC SHEET", "BALASORE"],
#     # ["HIL", "SBU 1", "AC SHEET", "PUNJAB/CHHINDWARA"],
#     ["HIL", "SBU 1", "AC SHEET", "CHHINDWARA"],
#     ["HIL", "SBU 1", "AC SHEET", "THRISSUR-FCP"],
#     ["HIL", "SBU 1", "CC SHEET", " WADA CC SHEET"],
#     ["HIL", "SBU 1", "CC SHEET", "BALASORE CC SHEET"],
#     ["HIL", "SBU 1", "FORTUNE", "VIJAYAWADA-FORTUNE"],
#     ["HIL", "SBU 1", "FORTUNE", "FARIDABAD-FORTUNE"],
#     ["HIL", "SBU 1", "TRADING", "TRADING"],
#     ["HIL", "SBU 2", "BLOCKS", "AAC BLOCKS-CHENNAI"],
#     ["HIL", "SBU 2", "BLOCKS", "AAC BLOCKS- GOLAN"],
#     ["HIL", "SBU 2", "BLOCKS", "AAC BLOCKS- JHAJAR"],
#     ["HIL", "SBU 2", "BLOCKS", "AAC BLOCKS- THIMMAPUR"],
#     ["HIL", "SBU 2", "BLOCKS", "DRY MIX - JHAJAR"],
#     ["HIL", "SBU 2", "PANELS", "THP-PANEL"],
#     ["HIL", "SBU 2", "PANELS", "FBD-PANEL"],
#     ["HIL", "SBU 2", "THERMAL/HYSIL", "THERMAL/HYSIL"],
#     ["HIL", "SBU 2", "FOB", "HYD FOB "],
#     ["HIL", "SBU 2", "FOB", "FARIDABAD FOB"],
#     ["HIL", "SBU 2", "TRADING 2", "TRADING - 2"],
#     # ["HIL", "SBU 3", "PIPES & FITTINGS", " FARIDABAD- PIPES AND FITTINGS"],
#     ["HIL", "SBU 3", "PIPES & FITTINGS", "FBD PIPES AND FITTINGS"],
#     ["HIL", "SBU 3", "PIPES & FITTINGS", "GOLAN PIPES AND FITTINGS"],
#     ["HIL", "SBU 3", "PIPES & FITTINGS", "THIM PIPES AND FITTINGS"],
#     ["HIL", "SBU 3", "PUTTY", "PUTTY - JHAJHAR"],
#     ["HIL", "SBU 3", "PUTTY", "GOLAN PUTTY "],
#     ["HIL", "SBU 3", "PUTTY", "TRADING PUTTY"],
#     ["HIL", "Others", "ED", "ED"],
#     ["HIL", "Others", "CORP.OFFICE", "CORP.OFFICE"],
#     # ["HIL", "Others", "PARADOR", "PARADOR"],
#     ["HIL", "Others", "PARADOR", "PARADOR-INDIA"],
#     ["HIL", "Others", "WIND POWER", "WIND POWER"],

# ]
# Add new SBU to this list along with its heirarchy


SHEET_NAMES = [
    # ["Apr", "01-04-2020"],
    # ["May", "01-05-2020"],
    # ["June", "01-06-2020"],
    # ["Jul", "01-07-2020"],
    # ["Aug", "01-08-2020"],
    # ["Sep", "01-09-2020"],
    # ["Oct", "01-10-2020"],
    # ["Nov", "01-12-2020"]
    # ["Dec", "01-12-2020"],
    # ["Jan", "01-01-2021"],
    ["Feb", "01-02-2021"],
    # ["Mar", "01-03-2020"]

]

MONTH_MAPPING = {
    "Apr": "01-04",
    "May": "01-05",
    "Jun": "01-06",
    "Jul": "01-07",
    "Aug": "01-08",
    "Sep": "01-09",
    "Oct": "01-10",
    "Nov": "01-11",
    "Dec": "01-12",
    "Jan": "01-01",
    "Feb": "01-02",
    "Mar": "01-03"
}
# Add new dates here

