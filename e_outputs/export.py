#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This modul export analyzes results to other files.
# Dependencies: b_settings, openpyxl, os, pandas


# import packages and modules
import b_settings.global_variables as st_gv
import datetime
import pandas as pd
## to install snowflake.sqlalchemy: pip3 install --upgrade snowflake-sqlalchemy


# transform data to a suitable form for export to an Excel file
def transforms_for_export(input_data):

    # data preparation to transform data into long data format
    data_combined = []
    expectation_combined = []
    change_type_combined = []

    # get the analyzes (table, column) and expectations for every object
    for iteration, obj_name in enumerate(input_data['object_names']):
        # table analysis
        for key, value in input_data['table_analyzes'][iteration].items():
            data_combined.append({
                'Object Name': obj_name,
                'Analysis Type': 'Table Analysis',
                'Analysis Name': f'{key}',
                'Current Result': value
                # 'Status': 'Current',
                # 'Analysis Result': value
            })
        # column analysis
        for key, value in input_data['column_analyzes'][iteration].items():
            data_combined.append({
                'Object Name': obj_name,
                'Analysis Type': 'Column Analysis',
                'Analysis Name': f'{key}',
                'Current Result': str(value)
                # 'Status': 'Current',
                # 'Analysis Result': value
            })
        # expectations for table analysis
        if 'table_analyzes' in input_data['expectations'][iteration]:
            for exp in input_data['expectations'][iteration]['table_analyzes']:
                for key, value in exp.items():
                    expectation_combined.append({
                        'Object Name': obj_name,
                        'Analysis Type': 'Table Analysis',
                        'Analysis Name': f'{key}',
                        'Expectation Result': value
                        # 'Status': 'Expectation',
                        # 'Analysis Result': value
                    })
        # expectations for column analysis
        if 'column_analyzes' in input_data['expectations'][iteration]:
            for col in input_data['expectations'][iteration]['column_analyzes']:
                expectation_combined.append({
                    'Object Name': obj_name,
                    'Analysis Type': 'Column Analysis',
                    'Analysis Name': f'{col[1]} in {col[0]}',
                    'Expectation Result': col[2]
                    # 'Status': 'Expectation',
                    # 'Analysis Result': col[2]
                })
        # change_type for table analysis
        if 'table_analyzes' in input_data['change_type'][iteration]:
            for exp in input_data['change_type'][iteration]['table_analyzes']:
                for key, value in exp.items():
                    change_type_combined.append({
                        'Object Name': obj_name,
                        'Analysis Type': 'Table Analysis',
                        'Analysis Name': f'{key}',
                        'Change Type': value
                        # 'Status': 'Expectation',
                        # 'Analysis Result': value
                    })
        # change_type for column analysis
        if 'column_analyzes' in input_data['change_type'][iteration]:
            for col in input_data['change_type'][iteration]['column_analyzes']:
                change_type_combined.append({
                    'Object Name': obj_name,
                    'Analysis Type': 'Column Analysis',
                    'Analysis Name': f'{col[1]} in {col[0]}',
                    'Change Type': col[2]
                    # 'Status': 'Expectation',
                    # 'Analysis Result': col[2]
                })


    # create a dataframe for long data format
    df_long = pd.DataFrame(data_combined)
    df_long_expectations = pd.DataFrame(expectation_combined)
    df_long_change_types = pd.DataFrame(change_type_combined)

    df_long = pd.merge(
        df_long_expectations,       # it's the 1st to place column 'Expectation Result' before column 'Current Result'
        df_long, 
        on = ['Object Name', 'Analysis Type', 'Analysis Name'], 
        how = 'left'
    )

    df_long = pd.merge(
        df_long, 
        df_long_change_types, 
        on = ['Object Name', 'Analysis Type', 'Analysis Name'], 
        how = 'left'
    )


    # convert lists to strings in the 'Current Result' column
    #   this approach utilizes lambda function
    df_long['Current Result'] = \
        df_long['Current Result'].apply(lambda x:
            ', '.join(map(str, x)) if isinstance(x, list) and len(x) > 1
            else x[0] if isinstance(x, list) and len(x) == 1
            else x
        )

    # convert lists to strings in the 'Expectation Result' column
    #   this approach utilizes lambda function
    df_long['Expectation Result'] = \
        df_long['Expectation Result'].apply(lambda x:
            'unknown' if pd.isna(x)     # action: update this pd.isna(x) in all relevant places to work with more than 1 list element
            else ', '.join(map(str, x)) if isinstance(x, list) and len(x) > 1
            else x[0] if isinstance(x, list) and len(x) == 1
            else x
        )

    df_long['Change Type'] = \
        df_long['Change Type'].apply(lambda x:
            'unknown' if pd.isna(x)
            else ', '.join(map(str, x)) if isinstance(x, list) and len(x) > 1
            else x[0] if isinstance(x, list) and len(x) == 1
            else x
        )


    # sort data for a better readability
    df_long['Sort Key'] = df_long['Object Name'] + df_long['Analysis Type'] + df_long['Analysis Name']
    df_long.sort_values(by='Sort Key', inplace = True)
    df_long.drop(columns=['Sort Key'], inplace = True)
    
    # add column 'Row ID'
    df_long.insert(0, 'Row ID', range(1, len(df_long) + 1))

    return df_long


# Logic for the new 'Flag' column
#   Action: Using of float() or other data type conversion will need to be conditional
def __calculate_flag(row):      # action: update/fix this function
    # if row['Change Type'] == 'Exact':
    #     return 1 if str(row['Current Result']) == str(row['Expectation Result']) else 0
    # elif row['Change Type'] == 'ExactOrLess':
    #     return 1 if str(row['Current Result']) <= str(row['Expectation Result']) else 0
    # elif row['Change Type'] == 'ExactOrMore':
    #     return 1 if str(row['Current Result']) >= str(row['Expectation Result'].strip("'[]'")) else 0
    # elif row['Change Type'] == 'Any':
    #     return 1
    # else:
    #     return 0  # handles unexpected 'Change Type' values
    return 0


def export_to_snowflake(df_long):
    try:
        # Create a cursor object
        cursor = st_gv.target_dict['connection'].cursor()

        # Define the SQL INSERT statement
        sql_insert = """
        INSERT INTO DIM_PP_ANALYSIS_DATA (
                DATE_AND_TIME, OBJECT_NAME, ANALYSIS_TYPE, ANALYSIS_NAME,
                EXPECTATION_RESULT, CURRENT_RESULT, CHANGE_TYPE, FLAG
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Create a dataframe    
        df = pd.DataFrame(df_long)

        # Logic for the new 'Flag' column
        df['Flag'] = df.apply(lambda row: __calculate_flag(row), axis=1)

        # print(df)

        # Prepare a timestamp
        current_timestamp = datetime.datetime.now()

        # Iterate over DataFrame rows and insert each row into Snowflake
        for _, row in df.iterrows():    # underscore: 1) replaces a variable because it's a common convention to indicate that the variable is not used, 2) while variable presented by underscore is not used in the code but it's part of the return value of iterrows()
            cursor.execute(sql_insert, (
                current_timestamp,
                row['Object Name'],
                row['Analysis Type'],
                row['Analysis Name'],
                row['Expectation Result'],
                str(row['Current Result']),     # action: str() as a temporary solution because of NaN error 
                row['Change Type'],
                0
                #row['Flag']
            ))

        # Commit the transaction
        st_gv.target_dict['connection'].commit()
    
        print("Records inserted successfully")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        st_gv.target_dict['connection'].close()




# # # # import packages and modules
# # # import b_settings.global_variables as st_gv
# # # from   e_outputs import user_errors as ue
# # # from   openpyxl import Workbook
# # # from   openpyxl.utils.dataframe import dataframe_to_rows
# # # from   openpyxl.styles import Font
# # # import os
# # # import pandas as pd


# # # # transform data to a suitable form for export to an Excel file
# # # def __transform_for_excel(input_data):
# # #     '''
# # #     Parameters:
# # #     input_data - dictionary containing results from previous evaluations (modules); this data will undergo transformation
# # #     '''

# # #     # data preparation to transform data into long data format
# # #     data_combined = []
    
# # #     # get the analyzes (table, column) and expectations for every object
# # #     for iteration, obj_name in enumerate(input_data['object_names']):
# # #         # table analysis
# # #         for key, value in input_data['table_analyzes'][iteration].items():
# # #             data_combined.append({
# # #                 'Object Name': obj_name,
# # #                 'Analysis Type': 'Table Analysis',
# # #                 'Analysis Name': f'{key}',
# # #                 'Status': 'Current',
# # #                 'Analysis Result': value
# # #             })
# # #         # column analysis
# # #         for key, value in input_data['column_analyzes'][iteration].items():
# # #             data_combined.append({
# # #                 'Object Name': obj_name,
# # #                 'Analysis Type': 'Column Analysis',
# # #                 'Analysis Name': f'{key}',
# # #                 'Status': 'Current',
# # #                 'Analysis Result': value
# # #             })
# # #         # expectations for table analysis
# # #         if 'table_analyzes' in input_data['expectations'][iteration]:
# # #             for exp in input_data['expectations'][iteration]['table_analyzes']:
# # #                 for key, value in exp.items():
# # #                     data_combined.append({
# # #                         'Object Name': obj_name,
# # #                         'Analysis Type': 'Table Analysis',
# # #                         'Analysis Name': f'{key}',
# # #                         'Status': 'Expectation',
# # #                         'Analysis Result': value
# # #                     })
# # #         # expectations for column analysis
# # #         if 'column_analyzes' in input_data['expectations'][iteration]:
# # #             for col in input_data['expectations'][iteration]['column_analyzes']:
# # #                 data_combined.append({
# # #                     'Object Name': obj_name,
# # #                     'Analysis Type': 'Column Analysis',
# # #                     'Analysis Name': f'{col[1]} of {col[0]}',
# # #                     'Status': 'Expectation',
# # #                     'Analysis Result': col[2]
# # #                 })
    
# # #     # create a dataframe for long data format
# # #     df_long = pd.DataFrame(data_combined)
    
# # #     # convert lists to strings in the 'Analysis Result' column
# # #     #   this approach utilizes lambda function
# # #     df_long['Analysis Result'] = \
# # #         df_long['Analysis Result'].apply(lambda x:
# # #             ', '.join(map(str, x)) if isinstance(x, list) and len(x) > 1
# # #             else x[0] if isinstance(x, list) and len(x) == 1
# # #             else x
# # #         )

# # #     # sort data for a better readability
# # #     df_long['Sort Key'] = df_long['Object Name'] + df_long['Analysis Type'] + df_long['Analysis Name'] + df_long['Status']
# # #     df_long.sort_values(by='Sort Key', inplace=True)
# # #     df_long.drop(columns=['Sort Key'], inplace=True)
    
# # #     # add column 'Row ID'
# # #     df_long.insert(0, 'Row ID', range(1, len(df_long) + 1))

# # #     return df_long


# # # # save evaluated and transformed results to an Excel file
# # # def __save_to_excel(df_long, excel_file_name):
# # #     '''
# # #     Parameters:
# # #     df_long - evaluated and transformed results suitable for saving to an Excel file
# # #     excel_file_name - Excel file name where the evaluated and transformed results will be saved
# # #     '''

# # #     # create a new Excel workbook and add a worksheet
# # #     wb = Workbook()
# # #     ws = wb.active
# # #     ws.title = 'Analysis Data'

# # #     # append DataFrame to the worksheet
# # #     for row in dataframe_to_rows(df_long, index=False, header=True):
# # #         ws.append(row)
    
# # #     # set column widths
# # #     column_widths = {
# # #         'A': 6.75,      # Column Width in Excel = 6
# # #         'B': 40.75,     # Column Width in Excel = 40
# # #         'C': 15.75,     # Column Width in Excel = 15
# # #         'D': 35.75,     # Column Width in Excel = 35
# # #         'E': 12.75,     # Column Width in Excel = 12
# # #         'F': 65.75      # Column Width in Excel = 65
# # #     }
# # #     for col, width in column_widths.items():
# # #         ws.column_dimensions[col].width = width

# # #     # apply bold font to the header row
# # #     for cell in ws[1]:
# # #         cell.font = Font(bold=True)

# # #     # save the workbook
# # #     script_dir = os.path.dirname(os.path.abspath(__file__))
# # #     parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir))   # didn't work when debugging: parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
# # #     excel_file_path = os.path.join(parent_dir, 'e_outputs', excel_file_name)
# # #     wb.save(excel_file_path)

# # #     return None

# # # # export results as tidy long data table into an excel
# # # def export_to_excel(input_data = st_gv.dataset_dict, excel_file_name = r'tidy long data export.xlsx'):      # action: input_data = st_gv.dataset_dict - this does not have sense since I can use directly global variable but it might have a sence from readablity point of view - I will reconsider the final apporach
# # #     '''
# # #     Parameters:
# # #     input_data - dictionary containing results from previous evaluations (modules); this data will undergo transformation
# # #     excel_file_name - Excel file name where the evaluated and transformed results will be saved
# # #     '''

# # #     df_long = __transform_for_excel(input_data)
# # #     try:
# # #         __save_to_excel(df_long, excel_file_name)   # attempt to save the dataframe to an Excel file
# # #     except PermissionError as e:
# # #         ue.handle_permission_error(e)   # handle the PermissionError when the Excel file is already open or write permissions are insufficient
# # #     except Exception as e:
# # #         ue.handle_unexpected_error(e)   # handle any unexpected exceptions that occur during the process

# # #     return None