# Databricks notebook source
import xlrd
import pandas as pd
import logging

log = logging.getLogger('excel_formatting')

# COMMAND ----------

# MAGIC %run "./APP_CONSTANTS_MIS"

# COMMAND ----------


def defining_col_width_headers(SHEET_NAMES, INPUT_FILE_PATH, OUTPUT_FILE_PATH,SBU_MAPPING):
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
        print_attributes(worksheet, SHEET_NAMES,SBU_MAPPING)
        print_heirarchy_for_sub_divisions(worksheet, writer, INPUT_FILE_PATH, SHEET_NAMES,SBU_MAPPING)
    except Exception as es:
        log.debug("Exception in function defining_col_width_headers")
        raise Exception(es)

def print_attributes(worksheet, SHEET_NAMES,SBU_DIVISIONS):
    """
    Inside function to print attributes
    :return:
    """
    try:
        log.warning("Inside function to print attributes ")
        row_count = 1
        cols_count = 5
        for month in SHEET_NAMES:
            for each in SBU_DIVISIONS:
                worksheet.write_column(row=row_count, col=cols_count, data=ACTUAL_BUDGET)
                row_count = row_count + 3

    except Exception as es:
        log.debug("Exception in function print_attributes")
        raise Exception(es)

def print_heirarchy_for_sub_divisions(worksheet, writer, INPUT_FILE_PATH, SHEET_NAMES,SBU_DIVISIONS):
    """
    Inside function to print heirarchy for sub divisions
    :return:
    """
    log.warning("Inside function to print heirarchy for sub divisions")
    rownumber = 1
    try:
        for item in SHEET_NAMES:
            for each in SBU_DIVISIONS:
                for i in range(1, 4):
                    worksheet.write(rownumber, 0, each[0])
                    worksheet.write(rownumber, 1, each[1])
                    worksheet.write(rownumber, 2, each[2])
                    worksheet.write(rownumber, 3, each[3])
                    worksheet.write_column(row=rownumber, col=4, data=[item[1]])

                    rownumber = rownumber + 1
                # rownumber = rownumber + 1
        CELL_NUMBERS = []
        plant_names_extracted_from_excel = []
        cell_workbook = xlrd.open_workbook(INPUT_FILE_PATH)
        for sheet in cell_workbook.sheets():
            for each in SBU_DIVISIONS:
                for row in range(sheet.nrows):
                    for column in range(sheet.ncols):
                        data_value = sheet.cell(row, column).value
                        if(isinstance(data_value,str)):
                            data_value = data_value.strip()
                        if data_value == (each[-1]).strip():
                            plant_names_extracted_from_excel.append(each[-1])
                            CELL_NUMBERS.append(column)
                            break
            break
        error_list = []
        for each in SBU_DIVISIONS:
            if each[-1] not in plant_names_extracted_from_excel:
                error_list.append(each[-1])
        if len(error_list) > 0:
            listToStr = ' '.join([str(elem) for elem in error_list])
            error_msg = " List of plant names not present in the Input File but has been mentioned in the constants list: {}".format(
                listToStr)
            raise Exception(error_msg)

        print_required_fields(CELL_NUMBERS, worksheet, SHEET_NAMES,INPUT_FILE_PATH)
        writer.save()
    except Exception as es:
        log.debug("Exception in function print_heirarchy_for_sub_divisions")
        raise Exception(es)

def print_required_fields(COLUMN_SEPARATIONS, worksheet, SHEET_NAMES,INPUT_FILE_PATH):
    """
    Inside function to print the the Previous Year Actual,Current year Actual and Budget
    :return:
    """
    try:
        log.warning("Inside function print_required_fields")
        row = 1
        for month_name in SHEET_NAMES:
            print("INPUT EXCEL FILE")
            print(INPUT_FILE_PATH)
            xl = pd.ExcelFile(INPUT_FILE_PATH)
            df = xl.parse(month_name[0])
            df = df.fillna("")
            for item in COLUMN_SEPARATIONS:
                col = 6
                for each in row_numbers:
                    prev_year_actual = df.iloc[:, item:item + 1].values[each]
                    worksheet.write(row, col, prev_year_actual[0])

                    current_month_budget = df.iloc[:, item + 1:item + 2].values[each]
                    worksheet.write(row + 1, col, current_month_budget[0])

                    current_month_actual = df.iloc[:, item + 2:item + 3].values[each]
                    worksheet.write(row + 2, col, current_month_actual[0])
                    col = col + 1

                row = row + 3

    except Exception as es:
        log.debug("Exception in function print_Actual_Current_Actual_Budget")
        raise Exception(es)

