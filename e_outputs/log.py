#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This module logs results of chosen steps to file log.txt.
# Dependencies: b_settings, datetime, os

 
# import packages and modules
## import b_settings.global_variables as st_gv
import datetime     # datetime modul for a datetime stamp
import os


# log events
def log_event(event):
    log_file_path = os.path.join(os.path.dirname(__file__), 'log.txt')      # otherwise log.txt is created in a package with main.py
    with open(log_file_path, 'a') as log_file:
        if event != '':
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_file.write(f'{timestamp}: {event}\n')
        else:
            log_file.write('\n')        # create an empty line in log.txt


# print results of datasets analyzes to Terminal
## def print_snowflake_dict():
##     print(st_gv.dataset_dict)