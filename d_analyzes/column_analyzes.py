#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This modul executes column analyzes (over transformed and clean data).
# Dependencies: b_settings, pandas, re


# import packages and modules
import b_settings.global_variables as st_gv
import pandas as pd
## import re       # module for regular expressions


# execute column analysis
def __execute_analysis(over, analysis, order) -> dict:       ## __ = private method only accessible within the same module
    '''
    Parameters:
    over (str): Specifies over which specific column of a dataframe the analysis should be performed.
    analysis (str): The analyzis name. Options: 'min', 'avg', 'max', 'unique', 'count_of_nulls'.
    order (int): The order of item in the highest (0th) level of a dictionary.

    Returns:
    dict: A dictionary containing the result of the analysis has to be returned every time.
    '''
    
    # column analysis
    if analysis == 'min':
        return {f'{analysis} of {over}': st_gv.dataset_dict['datasets'][order].loc[:, over].agg(analysis)}
    elif analysis == 'avg':
        # calculate the average of a column only if that column specified in the 'over' variable is of a numeric data type 
        if pd.api.types.is_numeric_dtype(st_gv.dataset_dict['datasets'][order].loc[:, over]):
            return {f'{analysis} of {over}': st_gv.dataset_dict['datasets'][order].loc[:, over].agg('mean')}
        else:
            return {f'{analysis} of {over}': f'NaN (reason: non numeric data type)'}    # NaN = Not a Number
    elif analysis == 'max':
        return {f'{analysis} of {over}': st_gv.dataset_dict['datasets'][order].loc[:, over].agg(analysis)}
    elif analysis == 'unique':
        return {f'{analysis} in {over}': st_gv.dataset_dict['datasets'][order].loc[:, over].agg(analysis).tolist()}
    elif analysis == 'count_of_nulls':
        return {f'{analysis} in {over}': st_gv.dataset_dict['datasets'][order].loc[:, over].agg('isnull').agg('sum')}
    else:
        return {'error': f'unknown analysis {analysis}'}


# analyze datasets on a column level
def analyze_column():
    # column analyzes results of all datasets
    column_analyzes_results = []

    # iterate through the length (number of items in) of dataset_dict
    for dict_index in range( len( st_gv.dataset_dict['object_names'] ) ):
        # analyzes results of a single dataset
        dataset_analyzes = {}

        # iterate through key value pairs of dataset_dict
        for key_level_0, value_level_0 in st_gv.dataset_dict.items():
            # execute column analyzes if key 'column_analyzes' is found
            if key_level_0 == 'column_analyzes':
                # iterate through key value pairs of 'column_analyzes' dictionary
                for key_level_1, value_level_1 in value_level_0[dict_index].items():
                    # process list values in every key of 'column_analyzes'
                    for analysis_values in value_level_1:   # value_level_1 contains list of values
                        # get analysis and added it to other analyzes
                        dataset_analyzes.update(
                            __execute_analysis(
                                over = key_level_1,
                                ## over = re.sub('^\d+', '', key_level_1),     # remove digits at the beginning using a regular expression
                                analysis = analysis_values,
                                order = dict_index
                            )
                        )

        # append found analyzes results of a relevant single dataset
        column_analyzes_results.append(dataset_analyzes)

    # update dataset_dict by replacing analyzes definitions from a user input .json file by found analyzes results
    st_gv.dataset_dict['column_analyzes'] = column_analyzes_results
