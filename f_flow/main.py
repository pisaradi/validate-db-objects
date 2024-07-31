#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This module is the main control flow.
#               Executing main() is needed to start the whole process.
# Dependencies: b_settings, c_datasets, d_analyzes, e_outputs ,inspect, os, sys


# preparation to import needed packages and modules
#   - steps required to ensure that Run Python File as well as Debug Python File will work
import inspect, os      # import required modules to calculate parent directory path
import sys              # import required module for importing from sibling directory
current_dir_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir_path = os.path.dirname(current_dir_path)
sys.path.insert(0, parent_dir_path)     # working option 1: https://stackoverflow.com/questions/714063/importing-modules-from-parent-folder; absolute path to the parent directory is used when modifying sys.path
## sys.path.append(parent_dir_path)       # working option 2
## sys.path.append("..")                  # append the path of the parent directory; debugging does not work with relative path used

# import packages and modules
from   b_settings import connectors as st_c, user_inputs as st_pui
import c_datasets as dt
import d_analyzes as anlz       # from d_analyzes import table_analyzes - it will also work
import e_outputs  as op


# main control flow
def main():
    op.log.log_event('start')

    # start - prepare json_dict to be tranformed into global variable snowflake_dict
    # ------------------------------------------------------------------------------
    # load data from a .json file if it exists
    json_dict = st_pui.get_file_to_process()
    op.user_errors.display_input_file_error(json_dict)     # display a potential error with readiness of a selected .json file
    op.log.log_event('completed: Load data from a .json file')

    # establish a connection to snowflake if possible
    json_dict = st_c.connect_to_source_db(json_dict)
    json_dict = op.user_errors.display_snowflake_connection_errors(json_dict)  # display potential errors with a connection to snowflake
    op.log.log_event('completed: Establish a connection to Snowflake')
    # ----------------------------------------------------------------------------
    # end - prepare json_dict to be tranformed into global variable snowflake_dict

    # start - process global variable snowflake_dict into final output shown to a user
    # --------------------------------------------------------------------------------
    # generate snowflake datasets from a .json file
    #   processed data will be stored in b_settings.global_variables.snowflake_dict
    dt.transformations.generate_snowflake_datasets(json_dict)
    op.log.log_event('completed: Generate Snowflake datasets')

    # analyze snowflake datasets
    anlz.table_analyzes.analyze_table()
    anlz.column_analyzes.analyze_column()
    op.log.log_event('completed: Analyze all datasets')
    anlz.expectation_analyzes.compare_with_expectations()
    op.log.log_event('completed: Compare analyzed results with expectations')

    # display results of snowflake dataset analyzes
    ## op.log.print_snowflake_dict()
    op.export.export_to_excel()
    op.log.log_event('completed: Display results of Snowflake dataset analyzes')
    # ------------------------------------------------------------------------------
    # end - process global variable snowflake_dict into final output shown to a user
    
    op.log.log_event('end')
    op.log.log_event('')        # create an empty line in log.txt

# only run if the script is run as the main program
if __name__ == "__main__":
    main()
