# compare columns and export to excel
# -----------------------------------
# import packages and modules
import os
import snowflake.connector as sf_con        # for establishing connection to snowflake database
import pyodbc                               # for connecting to databases using ODBC (Open Database Connectivity) API
import easygui                              # for creating easy-to-use graphical user interfaces (GUIs) for user interactions
import pandas as pd
import sqlite3
from   sqlalchemy import create_engine
import time                                 # for working with time


# START - USER INPUT SETTINGS
# ---------------------------
# use your email address or other suitable snowflake login
snowflake_user = 'peter.pisar@adiglobal.com'

# define select statement between ''' ''' and rename column names by adding '__SF'
#   advice: write snowflake column names only using capitals
#   note: you can choose any column name that will be unique across all used column names in all used select statements
snowflake_select_statement = '''
    select  DS.SUPPLIER_ID as SUPPLIER_ID__SF,
            DC.COUNTRY_ID as COUNTRY_ID__SF,
            DS.SUPPLIER_ACCOUNT as SUPPLIER_ACCOUNT__SF,
            DS.SUPPLIER_NAME as SUPPLIER_NAME__SF,
            DS.SUPPLIER_LABEL as SUPPLIER_LABEL__SF,
            DS.INTERCO_TYPE as INTERCO_TYPE__SF,
            DS.COMPANY_CODE as COMPANY_CODE__SF,
            DS.CROSS_COMPANY_CODE as CROSS_COMPANY_CODE__SF
    from    DB_ADI_EMEA_QA.SCH_ADI_EMEA_ERP.VW_DIM_ADI_EMEA_SUPPLIER_STG as DS
        left join   DB_ADI_EMEA_QA.SCH_ADI_EMEA_ERP.VW_DIM_COUNTRY as DC
           on       DS.COUNTRY_CODE = DC.COUNTRY_CODE
    where   DS.COUNTRY_CODE = 'GB'
            and DS.COMPANY_CODE = '2103'
            and left(DS.SUPPLIER_ACCOUNT, 4) = 'P309'
    order by COMPANY_CODE asc
    '''

# define select statement between ''' ''' and rename column names by adding '__BW'
#   note: you can choose any column name that will be unique across all used column names in all used select statements
sql_server_select_statement = '''
    select  Sup_ID as Sup_ID__BW,
            Country_ID as Country_ID__BW,
            SupplierAccount as SupplierAccount__BW,
            SupplierName as SupplierName__BW,
            SupplierLabel as SupplierLabel__BW,
            IntercoType as IntercoType__BW,
            NULL as COMPANY_CODE__BW,
            NULL as CROSS_COMPANY_CODE__BW
    from    ADI_EMEA_DMT.dbo.V_DimSupplier
    where   Country_ID = 3
            and left(SupplierAccount, 4) = 'P309'
    '''

# define left side and right side columns of the join between the results of snowflake_select_statement and sql_server_select_statement respectively
#   if multiple columns are needed, use comma as a separator and write columns in a corresponding order
left_side_cols = ['SUPPLIER_ACCOUNT__SF', 'COUNTRY_ID__SF']
right_side_cols = ['SupplierAccount__BW', 'Country_ID__BW']

# define which columns will be compared to calculate difference columns
#   the 1st and the 2nd arguments of the outer [] have to be column names chosen from the result of previous 2 statements
#   the 3rd argument of the outer [] has to be the difference column name
#   it's unacceptable to select any 2 columns without a difference column therefore
#     if a difference column is not added for any 2 columns then these 2 columns cannot be exported
#   note: the order of inner [] defines the order in which columns will be exported into excel
#         but to keep this column order when exporting into SQL Server database table additional step is needed (see flat_dif_cols)
dif_cols = [
    ['SUPPLIER_ACCOUNT__SF', 'SupplierAccount__BW', 'SUPPLIER_ACCOUNT_vs_SupplierAccount__DIFF'],
    ['SUPPLIER_ID__SF', 'Sup_ID__BW', 'SUPPLIER_KEY_vs_Sup_ID__DIFF'],
    ['COUNTRY_ID__SF', 'Country_ID__BW', 'COUNTRY_ID_vs_Country_ID__DIFF'],
    ['SUPPLIER_NAME__SF', 'SupplierName__BW', 'SUPPLIER_NAME_vs_SupplierName__DIFF'],
    ['SUPPLIER_LABEL__SF', 'SupplierLabel__BW', 'SUPPLIER_LABEL_vs_SupplierLabel__DIFF'],
    ['INTERCO_TYPE__SF', 'IntercoType__BW', 'INTERCO_TYPE_vs_IntercoType__DIFF'],
    ['COMPANY_CODE__SF', 'COMPANY_CODE__BW', 'COMPANY_CODE_vs_COMPANY_CODE__DIFF'],
    ['CROSS_COMPANY_CODE__SF', 'CROSS_COMPANY_CODE__BW', 'CROSS_COMPANY_CODE_vs_CROSS_COMPANY_CODE__DIFF']
]

# define the final output type choosing from: excel, tsql, sqlite3
output_structure = ['sqlite3']         # acceptable: output_structure = ['excel', 'tsql', 'sqlite3']

# define sqlite3 database file - this can be ignored if you didn't choose sqlite3 as output_structure (see above)
#   it's better (to be sure to avoid error: sqlite3.OperationalError: database is locked) to close any connection
#     to sqlite3 database (e.g. choose "Close Database" in "DB Browser for SQLite") before exporting data to sqlite3 database
sqlite3_db_path = 'C:\\Users\\267800\\Work\\Development.!x!\\231218 - Snowflake\\231218 - Core zone\\Analyzes\\SQLite3_Database\\CoreZone_SQLite3.db'

# define a SQL Server database table name and excel sheet name for exported data in exported_table_name
exported_table_name = 'DataComparison'

# define the number of rows to upload into SQL Server database in a single batch (at a time)
num_rows_per_upload = 200
# -------------------------
# END - USER INPUT SETTINGS

# generate a connection string to snowflake
def __generate_snowlake_connector():            # __ = private method only accessible within the same module
    # prompt user to enter snowflake password
    password = easygui.passwordbox(msg = 'Enter Snowflake password', title = 'Password' )

    # generate a snowflake connection with the respective snowflake connection string or display an error
    sf_connection = sf_con.connect(
        account   = 'resideo-edh.privatelink',  # account name (it does not include the snowflakecomputing.com suffix),
        user      = snowflake_user,
        password  = password,
        warehouse = 'WH_ADI_DATA_ANALYTICS'     # warehouse = 'WH_ADI_DATA_ANALYTICS'; in Snowflake: SELECT CURRENT_WAREHOUSE(); to check the name of currently used warehouse
        # database = database,                  # database = 'DB_ADI_GLOBAL_SBX'
        # schema = schema                       # schema = 'SCH_ADI_EMEA_ANALYTICS_SBX'
    )
    return sf_connection


# generate a connection string to sql server
def __generate_sql_server_connector():  # __ = private method only accessible within the same module
    driver   = 'SQL Server'
    server   = 'NL85W1028\SQLMCP51'     # production environment
    # database = 'ADI_EMEA_DWH'
    # username = 'xxx'
    # password = 'yyy'

    sql_connection = pyodbc.connect(    # do not write any space before and after any of "=" below
        f'driver={driver};'             #'DRIVER = {ODBC Driver 17 for SQL Server};'
        f'server={server};'
        # f'database={database}'
        # f'UID={username}; PWD={password}'
    )

    return sql_connection


# generate sqlalchemy engine
def __generate_sqlalchemy_engine():     # __ = private method only accessible within the same module
    # driver   = 'SQL Server'
    server   = 'NL85W1028\SQLMCP51'     # production environment; 'NL85W1029\SQLMCQ51' = test environment
    database = 'ADI_EMEA_DMT'
    # username = 'xxx'
    # password = 'yyy'

    # construct the connection string
    connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

    # create the SQLAlchemy engine
    sqlalchemy_engine = create_engine(connection_string)

    return sqlalchemy_engine


# generate a dataframe from snowflake data
def __generate_dataframe_from_snowflake(select_statement):
    snowflake_connection = __generate_snowlake_connector()
    snowflake_df = pd.read_sql(select_statement, snowflake_connection)
    snowflake_connection.close()
    return snowflake_df


# generate a dataframe from sql server data
def __generate_dataframe_from_sql_server(select_statement):
    sql_server_connection = __generate_sql_server_connector()
    sql_server_df = pd.read_sql(select_statement, sql_server_connection)
    sql_server_connection.close()
    # df.columns = df.columns.str.strip()         # remove unwanted spaces from the beginning and ending of column names
    # for column in df.columns:                   # remove unwanted spaces from the beginning and ending of column values because (I don't know why those spaces can appear after loading data from sql server to dataframe)
    #     if df[column].dtype == 'object':
    #         df[column] = df[column].str.strip()
    return sql_server_df


# generate a dataframe containing data from snowflake, sql server and calculate columns measuring differences
def __generate_merged_dataframe():
    # generate dataframes
    snowflake_df = __generate_dataframe_from_snowflake( snowflake_select_statement )
    sql_server_df = __generate_dataframe_from_sql_server( sql_server_select_statement )

    # merge dataframes
    merged_df = pd.merge(
        snowflake_df, sql_server_df,
        how = 'outer', left_on = left_side_cols, right_on = right_side_cols,
        )
    
    # create new columns to display differences
    for col1, col2, col_diff in dif_cols:
        merged_df[col_diff] = merged_df[col1].fillna('NULL') != merged_df[col2].fillna('NULL')      # NULL (NaN) values would return True therefore fillna() is used; there can be any text or False/True instead of 'NULL'

    return merged_df


# export merged_df to excel files
def __export_to_excel(merged_df):
    total_rows = merged_df.shape[0]         # get number of rows in merged_df
    current_row = 0

    # adjust dif_cols to ensure the chosen order of columns after exported into sql server database table
    flat_dif_cols = [col for sublist in dif_cols for col in sublist]

    # store the number of exported data batches
    exported_count = 0

    # get a path to the current .py file
    current_directory = os.path.dirname(__file__)

    while current_row < total_rows:
        batch_df = merged_df.iloc[current_row:current_row + num_rows_per_upload]
        
        # build a patch to the output .xlsx file (new .xlsx will have the higher number in its file name)
        output_excel_path = os.path.join(current_directory, f"{os.path.splitext(os.path.basename(__file__))[0]}_{exported_count}.xlsx")

        # upload data to an excel table
        batch_df.to_excel(      # creating an ExcelWriter object with a file name that already exists will result in the contents of the existing file being erased. (https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_excel.html)
            excel_writer = output_excel_path,
            sheet_name = exported_table_name,
            columns = flat_dif_cols,
            index = False
        )

        # ask the user if they want to continue uploading if not all data is uploaded
        current_row += num_rows_per_upload
        if current_row < total_rows:
            response = easygui.buttonbox(
                msg =
                    f'''
                    Do you want to continue exporting?
                    {current_row} out of {total_rows} rows have been exported.
                    ''',
                title = 'Load data into excel files',
                choices = ['Yes', 'No']
            )
            if response == 'No':
                break

        exported_count += 1         # increment counter of exported files


# export merged_df to sql server database table
def __export_to_tsql(merged_df):
    total_rows = merged_df.shape[0]         # get number of rows in merged_df
    current_row = 0

    # adjust dif_cols to ensure the chosen order of columns after exported into SQL Server database table
    flat_dif_cols = [col for sublist in dif_cols for col in sublist]

    # generate sqlalchemy engine
    sqlalchemy_engine = __generate_sqlalchemy_engine()

    # add rows if needed
    while current_row < total_rows:         # execute until total rows is met
        # decide about append or replace
        if current_row > 0:
            append_replace = 'append'       # upload to existed sql server database table
        else:
            append_replace = 'replace'      # create or replace sql server database table

        # upload data to sql server database table
        merged_df.loc[current_row:current_row + num_rows_per_upload - 1, flat_dif_cols].to_sql(
            name = exported_table_name,
            con = sqlalchemy_engine,
            schema = 'snow',
            index = False,
            if_exists = append_replace
        )

        # ask the user if they want to continue uploading if not all data is uploaded
        current_row += num_rows_per_upload
        if current_row < total_rows:
            response = easygui.buttonbox(
                msg =
                    f'''
                    Do you want to continue uploading?
                    {current_row} out of {total_rows} rows have been exported.
                    ''',
                title = 'Load data into SQL Server database',
                choices = ['Yes', 'No']
                )
            if response == 'No':
                break

    # close the database connection
    sqlalchemy_engine.dispose()


# export merged_df to sqlite3 database table
def __export_to_sqlite3(merged_df):         # it's very similar to __export_to_tsql (I keep them separate instead merging to enhance readability)
    total_rows = merged_df.shape[0]         # get number of rows in merged_df
    current_row = 0

    # adjust dif_cols to ensure the chosen order of columns after exported into SQL Server database table
    flat_dif_cols = [col for sublist in dif_cols for col in sublist]

    # make a connection to sqlite3 database (file)
    sqlite3_connection = sqlite3.connect(sqlite3_db_path)

    # add rows if needed
    while current_row < total_rows:         # execute until total rows is met
        # decide about append or replace
        if current_row > 0:
            append_replace = 'append'       # upload to existed sql server database table
        else:
            append_replace = 'replace'      # create or replace sql server database table

        # upload data to sqlite3 database table
        merged_df.loc[current_row:current_row + num_rows_per_upload - 1, flat_dif_cols].to_sql(
            name = exported_table_name,
            con = sqlite3_connection,
            schema = None,
            index = False,
            if_exists = append_replace
        )

        # ask the user if they want to continue uploading if not all data is uploaded
        current_row += num_rows_per_upload
        if current_row < total_rows:
            response = easygui.buttonbox(
                msg =
                    f'''
                    Do you want to continue uploading?
                    {current_row} out of {total_rows} rows have been exported.
                    ''',
                title = 'Load data into SQLite3 database',
                choices = ['Yes', 'No']
                )
            if response == 'No':
                break

    # close the database connection
    sqlite3_connection.close()


# display the final confirmation
def __display_final_confirmation(start_time, end_time, rows_count):
    # calculate duration of data processing and convert duration to format HH:MM:SS
    duration = end_time - start_time
    hours, rem = divmod(duration, 3600)
    minutes, seconds = divmod(rem, 60)
    formatted_duration = '{:0>2}:{:0>2}:{:05.2f}'.format(int(hours), int(minutes), seconds)

    # display the final confirmation
    completion_message = \
        f'''
        The data was uploaded.

        Start time: {time.ctime(start_time)}
        End time  : {time.ctime(end_time)}
        Duration  : {formatted_duration}
        Rows      : {rows_count}
        '''
    easygui.msgbox(msg = completion_message, title = 'Data upload is completed')


def main():
    # save starting time
    start_time = time.time()

    # generate a dataframe containing data from snowflake, sql server and calculate columns measuring differences
    merged_df = __generate_merged_dataframe()

    # export merged_df to sql server database table
    # export merged_df to excel files
    if 'excel' in output_structure:
        __export_to_excel(merged_df)

    if 'tsql' in output_structure:
        __export_to_tsql(merged_df)

    # export merged_df to excel files
    if 'sqlite3' in output_structure:
        __export_to_sqlite3(merged_df)

    # save ending time
    end_time = time.time()

    # display the final confirmation
    __display_final_confirmation(start_time, end_time, merged_df.shape[0])


if __name__ == '__main__':
    main()

