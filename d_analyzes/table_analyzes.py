#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This modul executes table analyzes (over transformed and clean data).
# Dependencies: b_settings


# import packages and modules
import b_settings.global_variables as st_gv


# execute table analysis
def __execute_analysis(analysis, order) -> dict:       ## __ = private method only accessible within the same module
    '''
    Parameters:
    analysis (str): The analyzis name. Options: 'count_of_rows', 'count_of_columns', 'count_of_nulls', 'candidate_keys'.
    order (int): The order of item in the highest (0th) level of a dictionary.

    Returns:
    dict: A dictionary containing the result of the analysis has to be returned every time.
    '''
    
    # table analysis
    if analysis == 'count_of_rows':
        return {analysis: st_gv.dataset_dict['datasets'][order].shape[0]}
    elif analysis == 'count_of_columns':
        return {analysis: st_gv.dataset_dict['datasets'][order].shape[1]}
    elif analysis == 'count_of_nulls':
        # count number null values in a data set
        null_counts = st_gv.dataset_dict['datasets'][order].agg('isnull').agg('sum')    # number of null values in all columns individually
        total_null_count = null_counts.agg('sum')                                       # number of null values in the whole dataframe
        return {analysis: total_null_count}
    elif analysis == 'candidate_keys':
        candidate_keys = []
        for column in st_gv.dataset_dict['datasets'][order].columns:                # iterate over the columns of the data frame
            if st_gv.dataset_dict['datasets'][order][column].nunique() == len(st_gv.dataset_dict['datasets'][order]) and \
                not st_gv.dataset_dict['datasets'][order][column].isnull().any():   # check if a column contains unique and non null values
                candidate_keys.append(column)
        return {analysis: candidate_keys}
    else:
        return {'error': f'unknown analysis {analysis}'}


# analyze datasets on a table level
def analyze_table():
    # table analyzes results of all datasets
    table_analyzes_results = []

    # iterate through the length (number of items in) of dataset_dict
    for dict_index in range( len( st_gv.dataset_dict['object_labels'] ) ):
        # analyzes results of a single dataset
        dataset_analyzes = {}

        # iterate through key value pairs of dataset_dict
        for key_level_0, value_level_0 in st_gv.dataset_dict.items():
            # execute table analyzes if key 'table_analyzes' is found
            if key_level_0 == 'table_analyzes':
                # iterate through values in 'table_analyzes' list
                for value_level_1 in value_level_0[dict_index]:
                    # get analysis and added it to other analyzes
                    dataset_analyzes.update(
                        __execute_analysis(
                            ## over = '',
                            analysis = value_level_1,
                            order = dict_index
                        )
                    )

        # append found analyzes results of a relevant single dataset
        table_analyzes_results.append(dataset_analyzes)

    # update dataset_dict by replacing of analyzes definitions from a user input .json file by found analyzes results
    st_gv.dataset_dict['table_analyzes'] = table_analyzes_results
