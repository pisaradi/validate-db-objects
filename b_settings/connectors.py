#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This modul establishes a connection to Snowflake or SQL Server database.
# Dependencies: b_settings, easygui, pyodbc, snowflake.connector


# Import packages and modules
from   b_settings import connectors as st_cnt
import easygui                              # for creating easy-to-use graphical user interfaces (GUIs) for user interactions
import pyodbc                               # for connecting to databases using ODBC (Open Database Connectivity) API
import snowflake.connector as sf_con        # for establishing connection to snowflake database; to install (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-install): pip3 install snowflake-connector-python


# Generate a connection string to snowflake
def __generate_snowflake_connector(warehouse, database, schema, object):       ## __ = private method only accessible within the same module
    # Prompt user to enter snowflake password
    #   easygui.passwordbox() does not accept formatting specifiers (like {:>10}) to align text
    password = easygui.passwordbox(
            f'Enter Snowflake password to:\n'
            f'warehouse:   {warehouse}\n'
            f'database:     {database}\n'
            f'schema:        {schema}\n'
            f'object:            {object}'
    )

    if password is None or password == "":  # User clicked on Cancel or closed (using x button) the dialog
        return 'Error_2a'

    # password = ''     # ATTENTION; you can comment everything from the beginning until this row and keep only this row with filled password for testing purposes

    # Generate a Snowflake connection or display an error
    try:
        sf_connection = sf_con.connect(             # sf_connection will contain a Snowflake connection string
            account   = 'resideo-edh.privatelink',  # account name (it does not include the snowflakecomputing.com suffix),
            user      = 'peter.pisar@adiglobal.com',
            password  = password,
            warehouse = warehouse           # warehouse = 'WH_ADI_DATA_ANALYTICS'; in Snowflake: SELECT CURRENT_WAREHOUSE(); to check the name of currently used warehouse
            ## database = database,         # database = 'DB_ADI_GLOBAL_SBX'
            ## schema = schema              # schema = 'SCH_ADI_EMEA_ANALYTICS_SBX'
        )
    except Exception as e:
        return str(e)       # in general, using str() is safer because we know exactly what to expect as output

    return sf_connection


# Generate a connection string to sql server
def __generate_sql_server_connector(driver, server):       ## __ = private method only accessible within the same module
    sql_connection = pyodbc.connect(    # do not write any space before and after any of "=" below
        f'driver={driver};'             # 'SQL Server'; 'DRIVER = {ODBC Driver 17 for SQL Server};'
        f'server={server};'             # 'NL85W1028\SQLMCP51'
        ## f'database={database}'
        ## f'UID={username}; PWD={password}'
    )

    return sql_connection


# Establish a connection to Snowflake or SQL Server database
def connect_to_source_db(json_dict):
    for value_level_0 in json_dict.values():
        for value_level_1 in value_level_0:
            if value_level_1['source'][0] == 'Snowflake':
                # Establish connection to Snowflake database for every single object in a .json file individually
                #   Password is passed by a user individually for every object
                connection = st_cnt.__generate_snowflake_connector(
                        value_level_1['warehouse'][0],
                        value_level_1['database'][0],
                        value_level_1['schema'][0],
                        value_level_1['object'][0]
                    )
            elif value_level_1['source'][0] == 'SQL Server':
                # Establish connection to SQL Server database using ODBC for every single object in a .json file individually
                connection = st_cnt.__generate_sql_server_connector(
                        value_level_1['source'][0],
                        value_level_1['warehouse'][0]
                        ## value_level_1['database'][0]
                    )
            
            # Add key connection to json_dict
            #   Key connection does not exist in any level of json_dict therefore is always added and never appended
            value_level_1['connection'] = connection

    return json_dict
