#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This modul executes expectation analyzes (over transformed and clean data).
# Dependencies: b_settings, re


# import packages and modules
import b_settings.global_variables as st_gv
import re       # module for regular expressions


# execute comparison analysis
def __execute_analysis(over, analysis) -> dict:       ## __ = private method only accessible within the same module
    '''
    Parameters:
    over (str): Specifies over which specific analyzes (keys of dictionary expectations) of a dataframe the analysis should be performed.
    analysis (str): The analyzis name. Options: value part of a dictionary under dictionary expectations.
    order (int): Does not exist as the input parameter because this function does not consider st_gv.dataset_dict.

    Returns:
    dict: A dictionary containing the result of the analysis has to be returned every time.
    '''
    
    # comparison analysis
    if over == 'table_analyzes':
        for key_level_1, value_level_1 in analysis.items():     # actually only one key value pair is available every time
            return {f'{over}': [{key_level_1: value_level_1}]}
    elif over == 'column_analyzes':
        return {f'{over}': [[analysis]]}


# compare expectations with results of analyzes
def compare_with_expectations():
    # expectation analyzes results of all comparisons set in .json
    expectation_analyzes_results = []

    # iterate through the length (number of items in) of dataset_dict
    for dict_index in range( len( st_gv.dataset_dict['object_names'] ) ):
        # analyzes results of a single dataset
        comparison_analyzes = {}

        # iterate through key value pairs of dataset_dict
        for key_level_0, value_level_0 in st_gv.dataset_dict.items():
            # execute table analyzes if key 'expectations' is found
            if key_level_0 == 'expectations':
                # iterate through key value pairs of 'expectations' dictionary
                for key_level_1, value_level_1 in value_level_0[dict_index].items():
                    # get lists inside values_level_1
                    for value_level_2 in value_level_1:
                        # get analysis and added it to other analyzes
                        result_for_comparison_analyzes = (
                            __execute_analysis(
                                over = key_level_1,
                                ## over = re.sub('^\d+', '', key_level_1),     # remove digits at the beginning using a regular expression
                                analysis = value_level_2
                                ## order = dict_index
                            )
                        )

                        # append or insert the 1st value into the analysis result to comparison_analyzes
                        for key, value in result_for_comparison_analyzes.items():
                            if key in comparison_analyzes:      # append a value
                                if isinstance(comparison_analyzes[key], list):
                                    if isinstance(value[0], list):  # extend if value[0] is a list otherwise append
                                        comparison_analyzes[key].extend(value[0])
                                    else:
                                        comparison_analyzes[key].append(value[0])       # table analyzes: needed if more than 2 items are processed; # append() also adds [] as part of value
                                else:
                                    if isinstance(value[0], list):  # concatenate if value[0] is a list otherwise create a new list
                                        comparison_analyzes[key] = [comparison_analyzes[key]] + value[0]
                                    else:
                                        comparison_analyzes[key] = [comparison_analyzes[key], value[0]]    # table analyzes: needed for the 2nd item 
                            else:   # insert the 1st value
                                if isinstance(value[0], list):  # assign directly if value[0] is a list otherwise wrap in a list
                                    comparison_analyzes[key] = value[0]                 # table analyzes: needed for the 1st item
                                else:
                                    comparison_analyzes[key] = [value[0]]

        # append found analyzes results of a relevant single comparison
        expectation_analyzes_results.append(comparison_analyzes)

    # update dataset_dict by replacing of analyzes definitions from a user input .json file by found analyzes results
    st_gv.dataset_dict['expectations'] = expectation_analyzes_results
