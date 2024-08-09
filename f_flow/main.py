#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This module is the main control flow.
#               Executing main() is needed to start the whole process.
# Dependencies: b_settings, c_datasets, d_analyzes, e_outputs ,inspect, os, sys


# Preparation to import needed packages and modules
#   Steps required to ensure that Run Python File as well as Debug Python File will work
import os       # import required modules to calculate parent directory path
import sys      # import required module for importing from sibling directory
parent_dir_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir_path)     # working option 1: https://stackoverflow.com/questions/714063/importing-modules-from-parent-folder; absolute path to the parent directory is used when modifying sys.path
## sys.path.append(parent_dir_path)       # working option 2
## sys.path.append("..")                  # append the path of the parent directory; debugging does not work with relative path used

# Import packages and modules
from   b_settings import connectors as st_c, user_inputs as st_pui
import b_settings.global_variables as st_gv
import c_datasets as dt
import d_analyzes as anlz       # from d_analyzes import table_analyzes - it will also work
import e_outputs  as op


# Main control flow
def main():
    op.log.log_event('start')

    # Start - prepare json_dict to be tranformed into global variable snowflake_dict
    # ------------------------------------------------------------------------------
    # Load data from a .json file if it exists
    json_dict = st_pui.get_file_to_process(parent_dir_path)
    op.user_errors.display_input_file_error(json_dict)     # display a potential error with readiness of a selected .json file
    op.log.log_event('completed: Load data from a .json file')

    # Establish a connection to the source Snowflake or source SQL Server database
    json_dict = st_c.connect_to_source_db(json_dict)
    json_dict = op.user_errors.display_snowflake_connection_errors(json_dict)  # display potential errors with a connection to snowflake
    op.log.log_event('completed: Establish a connection to Snowflake')

    # Generate Snowflake datasets from the target object
    st_c.generate_target_dataset()
    # ----------------------------------------------------------------------------
    # End - prepare json_dict to be tranformed into global variable snowflake_dict

    # Start - process global variable snowflake_dict into final output shown to a user
    # --------------------------------------------------------------------------------
    # Generate snowflake datasets from a .json file
    #   Processed data will be stored in b_settings.global_variables.snowflake_dict
    dt.transformations.generate_snowflake_datasets(json_dict)
    op.log.log_event('completed: Generate Snowflake datasets')

    # Analyze snowflake datasets
    anlz.table_analyzes.analyze_table()
    anlz.column_analyzes.analyze_column()
    op.log.log_event('completed: Analyze all datasets')
    anlz.expectation_analyzes.compare_with_expectations()
    op.log.log_event('completed: Compare analyzed results with expectations')

    # Display results of snowflake dataset analyzes
    ## op.log.print_snowflake_dict()

    # Transform data for exporting and export the transformed data
    df_long = op.export.transforms_for_export(st_gv.dataset_dict)
    op.export.export_to_snowflake(df_long)
    op.log.log_event('completed: Display results of Snowflake dataset analyzes')
    # ------------------------------------------------------------------------------
    # End - process global variable snowflake_dict into final output shown to a user
    
    op.log.log_event('end')
    op.log.log_event('')        # create an empty line in log.txt


# Run only if the script is executed directly
if __name__ == "__main__": main()
