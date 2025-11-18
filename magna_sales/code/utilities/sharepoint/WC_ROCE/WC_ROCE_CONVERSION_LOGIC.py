# Databricks notebook source
#This is the file where the actual tranformations for WC ROCE are implemented
#Import Statements
import xlrd
import pandas as pd
import logging

log = logging.getLogger('excel_formatting')

# COMMAND ----------

def defining_col_width_headers(DATE_MAPPING, INPUT_FILE_PATH, OUTPUT_FILE_PATH):
    """
    Inside function to define column headers
    :return:
    """
    try:
        log.warning("Inside function to define the column headers")
        # Set the columns widths.
        col_dict = {}
        headers_list = []
        for each in attributes:
            headers_list.append(each.upper())

        final_list = HEADERS + headers_list
        for each in final_list:
            col_dict[each] = []
        df = pd.DataFrame(col_dict)
        writer = pd.ExcelWriter(OUTPUT_FILE_PATH, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False)

        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # Add a header format.
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#D7E4BC',
            'border': 1})

        # Write the column headers with the defined format.
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        print_attributes(worksheet, SHEET_NAMES)
        print_heirarchy_for_sub_divisions(worksheet, writer, INPUT_FILE_PATH, DATE_MAPPING)
    except Exception as es:
        log.debug("Exception in function defining_col_width_headers")
        raise Exception(es)


# COMMAND ----------


def print_attributes(worksheet,SHEET_NAMES):
  """
  Inside function to print attributes
  :return:
  """
  try:
      log.warning("Inside function to print attributes ")
      row_count = 1
      cols_count = 4
      for month in SHEET_NAMES:
          for each in DATE_MAPPING:
              worksheet.write_column(row=row_count, col=cols_count, data=ACTUAL_BUDGET)
              row_count = row_count + 2

  except Exception as es:
      log.debug("Exception in function print_attributes")
      raise Exception(es)

# COMMAND ----------

def print_heirarchy_for_sub_divisions(worksheet, writer,INPUT_FILE_PATH, DATE_MAPPING):
    """
    Inside function to print heirarchy for sub divisions
    :return:
    """
    log.warning("Inside function to print heirarchy for sub divisions")
    rownumber = 1
    try:
        for item in SBU_DIVISIONS:
            for each in DATE_MAPPING:
                for i in range(1, 3):
                    worksheet.write(rownumber, 0, item[0])
                    worksheet.write(rownumber, 1, item[1])
                    worksheet.write(rownumber, 2, item[2])
                    worksheet.write_column(row=rownumber, col=3, data=[each[1]])
                    rownumber = rownumber + 1
                # rownumber = rownumber + 1
        CELL_NUMBERS = []
        plant_names_extracted_from_excel = []
        cell_workbook = xlrd.open_workbook(INPUT_FILE_PATH)
        print("FILE__PATH ------->")
        print(INPUT_FILE_PATH)
        sheets = cell_workbook.sheets()
        print(sheets)
        test = []
        for sheet in sheets:
            if sheet.name in SHEET_NAMES:
                print("SHEET NAME",sheet.name)
                for each in DATE_MAPPING:
                    for row in range(sheet.nrows):
                        for column in range(sheet.ncols):
                            if sheet.cell(row, column).value == each[0]:
                                plant_names_extracted_from_excel.append(each[0])
                                CELL_NUMBERS.append(column)
                                break
                break
        error_list = []
        for each in DATE_MAPPING:
            if each[0] not in plant_names_extracted_from_excel:
                error_list.append(each[0])
        if len(error_list) > 0:
            listToStr = ' '.join([str(elem) for elem in error_list])
            error_msg = "List of Months not present in the Input File but has been mentioned in the constants list : {}".format(
                listToStr)
            #raise Exception(error_msg)

        print_required_fields(INPUT_FILE_PATH,CELL_NUMBERS, worksheet, SHEET_NAMES)
        writer.save()
    except Exception as es:
        log.debug("Exception in function print_heirarchy_for_sub_divisions")
        raise Exception(es)

# COMMAND ----------

def print_required_fields(INPUT_FILE_PATH,COLUMN_SEPARATIONS, worksheet,SHEET_NAMES):
    """
    Inside function to print the the Variance,Current year Actual and Budget
    :return:
    """
    try:
        log.warning("Inside function print_required_fields")
        row = 1
        for sbu_division in SHEET_NAMES:
            xl = pd.ExcelFile(INPUT_FILE_PATH)
            df = xl.parse(sbu_division)
            df = df.fillna("")
            for item in COLUMN_SEPARATIONS:
                col = 5
                for each in row_numbers:
                    prev_year_actual = df.iloc[:, item:item + 1].values[each]
                    worksheet.write(row, col, prev_year_actual[0])

                    current_month_budget = df.iloc[:, item + 1:item + 2].values[each]
                    worksheet.write(row + 1, col, current_month_budget[0])

                    # current_month_actual = df.iloc[:, item + 2:item + 3].values[each]
                    # worksheet.write(row + 2, col, current_month_actual[0])
                    col = col + 1

                row = row + 2

    except Exception as es:
        log.debug("Exception in function print_Actual_Current_Actual_Budget")
        raise Exception(es)
