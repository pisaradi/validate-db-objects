#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This modul contains global variables.
# Dependencies: -


# dataset information transformed (inside methods) from a user input .json file
dataset_dict = {
    'object_labels': [],    # Main key in a .json file (the highest level of the first {})
    'datasets': [],         # Snowflake or SQL Server datasets
    'object_names': [],     # Snowflake or SQL Server object names
    'table_analyzes': [],   # analyzes required to be made over whole datasets
    'column_analyzes': [],  # analyzes required to be made over specified columns
    'expectations': [],     # expected results based on previous validations (in any tool)
    'change_type': []       # defined possibility of differences between current and expectation result
}

# Target object
target_dict = {
    'dataset': None,        # dataframe from the target object
    'connection': None
}