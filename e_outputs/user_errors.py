#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This module validates user inputs during software execution.
# Dependencies: sys, tkinter


# import packages and modules
import sys
from   tkinter import messagebox


# display a potential error with readiness of a selected .json file
def display_input_file_error(json_dict):
    if json_dict == 'Error_1a':
        messagebox.showerror(
            title = 'Error',
            message = 'No file chosen.\nThe whole process ends.'
            ## icon = 'error'       # parameter icon options: error, info, question, warning
        )
        sys.exit()
    elif json_dict == 'Error_1b':
        messagebox.showerror(
            title = 'Error',
            message = 'No input file found.\nThe whole process ends.'
        )
        sys.exit()


# display potential errors with a connection to snowflake
def display_snowflake_connection_errors(json_dict):
    # keys to be removed
    keys_to_remove = []

    # iterate through json_dict and validate snowflake connection
    for key_level_0, value_level_0 in json_dict.items():
        for value_level_1 in value_level_0:
            if isinstance(value_level_1['connection'], str):
                if value_level_1['connection'] == 'Error_2a':
                    messagebox.showerror(
                        title = 'Error',
                        message = f'Missing password for {key_level_0}.\nThis object will be not processed.'
                    )
                    keys_to_remove.append(key_level_0)
                    # no sys.exit() because there might be other objects that can be processed

                elif value_level_1['connection'][:14] == '250001 (08001)':   # Error_2b: specific error returned by str(e)
                    messagebox.showerror(
                        title = 'Error',
                        message = f'Incorrect username or password was specified for {key_level_0}.\nThis object will be not processed.'
                    )
                    keys_to_remove.append(key_level_0)
                    # no sys.exit() because there might be other objects that can be processed

    # remove keys gathered to be removed
    for key in keys_to_remove:
        del json_dict[key]

    # Error_2c: if no object remains after validations above (in this method)
    if not bool(json_dict):
        messagebox.showerror(
            title = 'Error',
            message = 'Nothing to process.\nThe whole process ends.'
        )
        sys.exit()
    
    return json_dict

# handle the PermissionError when the Excel file is already open or write permissions are insufficient
def handle_permission_error(e):
    messagebox.showerror(
        title = 'Permission Error',
        message = f'Failed to save Excel file.\n' +
                  f'Please check if a relevant Excel file is already open, or\n'+
                  f'if you have sufficient write permissions.\n\n'+
                  f'Error: {str(e)}'
    )
    sys.exit()

# handle any unexpected exceptions that occur during the process
def handle_unexpected_error(e):
    messagebox.showerror(
        title = 'Error',
        message = f'An unexpected error occurred: {str(e)}'
    )
    sys.exit()
