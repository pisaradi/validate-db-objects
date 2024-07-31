#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This modul transforms data from a .json file into a global dictionary.
#               Datasets containing data from source objects written in the .json file are generated.
# Dependencies: b_settings, pandas


# import packages and modules
import b_settings.global_variables as st_gv
import pandas as pd


# generate Snowflake datasets from a .json file
def generate_snowflake_datasets(json_dict):
    # iterate through dictionaries from the chosen .json file
    for key_level_0, value_level_0 in json_dict.items():
        # iterate through the 1st level of sub-dictionaries
        for value_level_1 in value_level_0:
            # generate select statement
            select_columns = ', '.join(value_level_1['dim_cols'] + value_level_1['fact_cols'])
            select_object = '.'.join(value_level_1['database'] + value_level_1['schema'] + value_level_1['object'])
            select_statement = f'select {select_columns} from {select_object}'

            # prepare data to be returned to all individual keys of dataset_dict
            st_gv.dataset_dict['datasets'].append(pd.read_sql(select_statement, value_level_1['connection']))
            value_level_1['connection'].close()      # close Snowflake connection because dataframe is ready
            st_gv.dataset_dict['object_names'].append(key_level_0)
            st_gv.dataset_dict['table_analyzes'].append(value_level_1['table_analyzes'])
            st_gv.dataset_dict['column_analyzes'].append(value_level_1['column_analyzes'])
            st_gv.dataset_dict['expectations'].append(value_level_1['expectations'])
