#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This modul export analyzes results to other files.
# Dependencies: b_settings, openpyxl, os, pandas


# import packages and modules
import b_settings.global_variables as st_gv
from   e_outputs import user_errors as ue
from   openpyxl import Workbook
from   openpyxl.utils.dataframe import dataframe_to_rows
from   openpyxl.styles import Font
import os
import pandas as pd


# transform data to a suitable form for export to an Excel file
def __transform_for_excel(input_data):
    '''
    Parameters:
    input_data - dictionary containing results from previous evaluations (modules); this data will undergo transformation
    '''

    # data preparation to transform data into long data format
    data_combined = []
    
    # get the analyzes (table, column) and expectations for every object
    for iteration, obj_name in enumerate(input_data['object_names']):
        # table analysis
        for key, value in input_data['table_analyzes'][iteration].items():
            data_combined.append({
                'Object Name': obj_name,
                'Analysis Type': 'Table Analysis',
                'Analysis Name': f'{key}',
                'Status': 'Current',
                'Analysis Result': value
            })
        # column analysis
        for key, value in input_data['column_analyzes'][iteration].items():
            data_combined.append({
                'Object Name': obj_name,
                'Analysis Type': 'Column Analysis',
                'Analysis Name': f'{key}',
                'Status': 'Current',
                'Analysis Result': value
            })
        # expectations for table analysis
        if 'table_analyzes' in input_data['expectations'][iteration]:
            for exp in input_data['expectations'][iteration]['table_analyzes']:
                for key, value in exp.items():
                    data_combined.append({
                        'Object Name': obj_name,
                        'Analysis Type': 'Table Analysis',
                        'Analysis Name': f'{key}',
                        'Status': 'Expectation',
                        'Analysis Result': value
                    })
        # expectations for column analysis
        if 'column_analyzes' in input_data['expectations'][iteration]:
            for col in input_data['expectations'][iteration]['column_analyzes']:
                data_combined.append({
                    'Object Name': obj_name,
                    'Analysis Type': 'Column Analysis',
                    'Analysis Name': f'{col[1]} of {col[0]}',
                    'Status': 'Expectation',
                    'Analysis Result': col[2]
                })
    
    # create a dataframe for long data format
    df_long = pd.DataFrame(data_combined)
    
    # convert lists to strings in the 'Analysis Result' column
    #   this approach utilizes lambda function
    df_long['Analysis Result'] = \
        df_long['Analysis Result'].apply(lambda x:
            ', '.join(map(str, x)) if isinstance(x, list) and len(x) > 1
            else x[0] if isinstance(x, list) and len(x) == 1
            else x
        )

    # sort data for a better readability
    df_long['Sort Key'] = df_long['Object Name'] + df_long['Analysis Type'] + df_long['Analysis Name'] + df_long['Status']
    df_long.sort_values(by='Sort Key', inplace=True)
    df_long.drop(columns=['Sort Key'], inplace=True)
    
    # add column 'Row ID'
    df_long.insert(0, 'Row ID', range(1, len(df_long) + 1))

    return df_long


# save evaluated and transformed results to an Excel file
def __save_to_excel(df_long, excel_file_name):
    '''
    Parameters:
    df_long - evaluated and transformed results suitable for saving to an Excel file
    excel_file_name - Excel file name where the evaluated and transformed results will be saved
    '''

    # create a new Excel workbook and add a worksheet
    wb = Workbook()
    ws = wb.active
    ws.title = 'Analysis Data'

    # append DataFrame to the worksheet
    for row in dataframe_to_rows(df_long, index=False, header=True):
        ws.append(row)
    
    # set column widths
    column_widths = {
        'A': 6.75,      # Column Width in Excel = 6
        'B': 40.75,     # Column Width in Excel = 40
        'C': 15.75,     # Column Width in Excel = 15
        'D': 35.75,     # Column Width in Excel = 35
        'E': 12.75,     # Column Width in Excel = 12
        'F': 65.75      # Column Width in Excel = 65
    }
    for col, width in column_widths.items():
        ws.column_dimensions[col].width = width

    # apply bold font to the header row
    for cell in ws[1]:
        cell.font = Font(bold=True)

    # save the workbook
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir))   # didn't work when debugging: parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    excel_file_path = os.path.join(parent_dir, 'e_outputs', excel_file_name)
    wb.save(excel_file_path)

    return None

# export results as tidy long data table into an excel
def export_to_excel(input_data = st_gv.dataset_dict, excel_file_name = r'tidy long data export.xlsx'):      # action: input_data = st_gv.dataset_dict - this does not have sense since I can use directly global variable but it might have a sence from readablity point of view - I will reconsider the final apporach
    '''
    Parameters:
    input_data - dictionary containing results from previous evaluations (modules); this data will undergo transformation
    excel_file_name - Excel file name where the evaluated and transformed results will be saved
    '''

    df_long = __transform_for_excel(input_data)
    try:
        __save_to_excel(df_long, excel_file_name)   # attempt to save the dataframe to an Excel file
    except PermissionError as e:
        ue.handle_permission_error(e)   # handle the PermissionError when the Excel file is already open or write permissions are insufficient
    except Exception as e:
        ue.handle_unexpected_error(e)   # handle any unexpected exceptions that occur during the process

    return None