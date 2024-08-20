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


# Create string from row
#   This function is necessary in order to process records like {"candidate_keys": ["COUNTRY_ID", "COUNTRY_CODE", "COUNTRY_NAME", "COUNTRY_ORD_NUM", "ExactOrMore"]}, from .json file.
def __create_string_from_row(row):
    def __convert_item(item):
        return 'unknown' if pd.isna(item) else str(item)
    
    if isinstance(row, list): return ', '.join(__convert_item(item) for item in row)    # convert and join a list with any number of elements
    else: return __convert_item(row)        # convert single non-list value


# Transform data to a suitable form for export to the target object (Snowflake database table)
def transforms_for_export(input_data):
    # Data preparation to transform data into long data format
    data_combined = []
    expectation_combined = []
    change_type_combined = []

    # Get the analyzes (table, column) and expectations for every object
    for iteration, obj_name in enumerate(input_data['object_names']):
        # Table analysis
        for key, value in input_data['table_analyzes'][iteration].items():
            data_combined.append({
                'Object Name': obj_name,
                'Analysis Type': 'Table Analysis',
                'Analysis Name': f'{key}',
                'Current Result': value
                # 'Status': 'Current',
                # 'Analysis Result': value
            })
        # Column analysis
        for key, value in input_data['column_analyzes'][iteration].items():
            data_combined.append({
                'Object Name': obj_name,
                'Analysis Type': 'Column Analysis',
                'Analysis Name': f'{key}',
                'Current Result': str(value)
                # 'Status': 'Current',
                # 'Analysis Result': value
            })
        # Expectations for table analysis
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
        # Expectations for column analysis
        if 'column_analyzes' in input_data['expectations'][iteration]:
            for col in input_data['expectations'][iteration]['column_analyzes']:
                expectation_combined.append({
                    'Object Name': obj_name,
                    'Analysis Type': 'Column Analysis',
                    'Analysis Name': f'{col[1]} of {col[0]}',
                    'Expectation Result': col[2]
                    # 'Status': 'Expectation',
                    # 'Analysis Result': col[2]
                })
        # Change_type for table analysis
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
        # Change_type for column analysis
        if 'column_analyzes' in input_data['change_type'][iteration]:
            for col in input_data['change_type'][iteration]['column_analyzes']:
                change_type_combined.append({
                    'Object Name': obj_name,
                    'Analysis Type': 'Column Analysis',
                    'Analysis Name': f'{col[1]} of {col[0]}',
                    'Change Type': col[2]
                    # 'Status': 'Expectation',
                    # 'Analysis Result': col[2]
                })


    # Create a dataframe for long data format
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

    # convert lists to strings in columns 'Current Result' column, 'Expectation Result' and 'Change Type'
    #   this approach utilizes lambda function
    df_long['Current Result'] = df_long['Current Result'].apply(__create_string_from_row)
    df_long['Expectation Result'] = df_long['Expectation Result'].apply(__create_string_from_row)
    df_long['Change Type'] = df_long['Change Type'].apply(__create_string_from_row)

    # sort data for a better readability
    df_long['Sort Key'] = df_long['Object Name'] + df_long['Analysis Type'] + df_long['Analysis Name']
    df_long.sort_values(by='Sort Key', inplace = True)
    df_long.drop(columns=['Sort Key'], inplace = True)
    
    # add column 'Row ID'
    df_long.insert(0, 'Row ID', range(1, len(df_long) + 1))

    return df_long


# Logic for 'Flag' column
def __calculate_flag(row):

    # Attempt to convert value to float; if not possible, keep it as a string
    def to_comparable(value):
        if isinstance(value, bool):           return 1 if value else 0
        elif isinstance(value, (int, float)): return value
        elif isinstance(value, str):
            value = value.strip("'[]'").lower()
            if value == 'true':    return 1     # this may occur in expectations
            elif value == 'false': return 0     # this may occur in expectations
            
            # Convert to float if the value is numeric; otherwise, keep it as a string
            try: return float(value)
            except ValueError: return value
        return value

    current_result = to_comparable(row['Current Result'])
    expectation_result = to_comparable(row['Expectation Result'])

    # Attempt to convert both to float for comparison
    #   Comparing data as float, if possible, is preferable.
    try:
        current_result = float(current_result)
        expectation_result = float(expectation_result)
    except ValueError:
        # If conversion to float fails, fall back to string comparison
        current_result = str(current_result)
        expectation_result = str(expectation_result)

    # Return value for 'Flag' column based on Change Type
    if    row['Change Type'] == 'Exact':       return expectation_result == current_result
    elif  row['Change Type'] == 'ExactOrLess': return expectation_result <= current_result
    elif  row['Change Type'] == 'ExactOrMore': return expectation_result >= current_result
    elif  row['Change Type'] == 'Any':         return True
    else: return False  # handles unexpected 'Change Type' values


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
                str(row['Current Result']),     # action: str() as a temporary solution because of NaN error because ANALYSIS_TYPE = "Column Analysis" returns CURRENT_RESULT = "nan" every time
                row['Change Type'],
                row['Flag']
            ))

        # Commit the transaction
        st_gv.target_dict['connection'].commit()
    
        print("Records inserted successfully.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        st_gv.target_dict['connection'].close()
