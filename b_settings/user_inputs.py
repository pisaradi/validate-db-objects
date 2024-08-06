#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This modul allows a user to select a .json file with data to be processed later.
#               Whole path to the selected file is evaluated.
# Dependencies: inspect, os, json, tkinter


# import packages and modules
import os
import json                 # to work with .json files
import tkinter as tk
from   tkinter import ttk

# global variable for tkinter methods
global_json_path = ''       # a technique without global variable is more complex


# allow a user to select a user input .json file
def get_file_to_process(parent_dir_path):
    # specify connection to a global variable
    global global_json_path

    # evaluate the correct input directory
    input_dir_path = os.path.join(parent_dir_path, 'a_inputs')

    # if a non-empty folder exists, save user input file names (or folder names if files do not exist) and user input file paths
    if os.path.exists(input_dir_path):
        file_names = os.listdir(input_dir_path)                                         # list file names in the directory
        file_paths = tuple(os.path.join(input_dir_path, file) for file in file_names)   # list paths to files
        input_files = tuple( zip( file_names, file_paths ) )                            # tuple of tuples
    else:
        input_files = ()

    # user selection from the available list of .json files
    if input_files:
        def process_file():
            selected_file = file_name.get()
            selected_file_path = tuple(file[1] for file in input_files if file[0] == selected_file)[0]
            root.quit()         # close root.mainloop; root.destroy() cannot be used => _tkinter.TclError: can't invoke "destroy" command: application has been destroyed
            global global_json_path
            global_json_path = selected_file_path     
            return

        # stop processing if x button in the top right corner is clicked
        def on_closing():
            root.quit()                                 # close root.mainloop()
            global global_json_path
            global_json_path = 'Error_1a'
            return

        root = tk.Tk()
        root.title("Choose file to process")
        root.protocol("WM_DELETE_WINDOW", on_closing)   # attach the on_closing function to window close event

        file_name = tk.StringVar(root)
        file_name.set(input_files[0][0])

        file_combobox = ttk.Combobox(
            root,
            textvariable = file_name,
            values = [file[0] for file in input_files],
            width = 50,
            state = 'readonly'                      # values in combobox can be selected but not changed
            )
        file_combobox.pack(padx = 10, pady = 10)    # padx, pady = x and y borders around combobox

        process_button = ttk.Button(root, text = 'Process', command = process_file)
        process_button.pack(pady = 10)

        root.mainloop()
        root.destroy()      # close the whole dialog window
    else:
        global_json_path = 'Error_1b'

    # decide of a final return
    if global_json_path[:5] != 'Error':
        # if a user selected a .json file
        json_dict = json.load( open( global_json_path, 'r' ) )  # read data from the selected .json file
        return json_dict
    else:
        return global_json_path     # relevant error identifier is returned
