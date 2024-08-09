#!/usr/bin/env python3
# Date:         2024-07-02
# Version:      0.1.0
# Description:  This modul executes expectation analyzes (over transformed and clean data).
# Dependencies: b_settings, re


# import packages and modules
import b_settings.global_variables as st_gv
###import re       # module for regular expressions


# execute comparison analysis
def __execute_analysis(object_name, over, analysis) -> dict:       ## __ = private method only accessible within the same module
    '''
    Parameters:
    object_name: Contains the value of column OBJECT_NAME from the final Snowflake table; it's the main key of each top level dictionary from JSON, e.g. SRC DIM_PRODUCT
    over (str): Specifies over which specific analyzes (keys of dictionary expectations) of a dataframe the analysis should be performed. Can be used to get value of column ANALYSIS_TYPE from the final Snowflake table.
    analysis (str): The analyzis name. Options: value part of a dictionary under dictionary expectations.
    order (int): Does not exist as the input parameter because this function does not consider st_gv.dataset_dict.

    Returns:
    dict: A dictionary containing the result of the analysis has to be returned every time.
    '''

    # comparison analysis
    if over == 'table_analyzes':
        for key_level_1, value_level_1 in analysis.items():     # actually only one key value pair is available every time
            # key_level_1 = ANALYSIS_NAME from the final Snowflake table

            # remove and keep the last value from value_level_1
            change_type = value_level_1.pop()

            filtered_df = \
                st_gv.target_dict['dataset'][
                    (st_gv.target_dict['dataset']['OBJECT_NAME'] == object_name)
                    & (st_gv.target_dict['dataset']['ANALYSIS_TYPE'] == 'Table Analysis')
                    & (st_gv.target_dict['dataset']['ANALYSIS_NAME'] == key_level_1)
                ]
            
            # Get value from column "CURRENT_RESULT"
            if not filtered_df.empty:
                analysis_result = filtered_df['CURRENT_RESULT'].iloc[0]
            else:
                analysis_result = value_level_1[0]

            if analysis_result != 'unknown':
                if value_level_1[0] != analysis_result:
                    value_level_1[0] = analysis_result

            return {f'{over}': [{key_level_1: value_level_1}]}, {f'{over}': [{key_level_1: [change_type]}]}  # 1st part returns (potentially) updated expectation value

    elif over == 'column_analyzes':
        #analysis[1] = the 1st part of ANALYSIS_NAME from the final Snowflake table
        #analysis[0] = the 2nd part of ANALYSIS_NAME from the final Snowflake table

        # remove and keep the last value from value_level_1
        change_type = analysis.pop()

        filtered_df = \
            st_gv.target_dict['dataset'][
                (st_gv.target_dict['dataset']['OBJECT_NAME'] == object_name)
                & (st_gv.target_dict['dataset']['ANALYSIS_TYPE'] == 'Column Analysis')
                & (st_gv.target_dict['dataset']['ANALYSIS_NAME'] == f'{analysis[1]} in {analysis[0]}')
            ]

        # Get value from column "CURRENT_RESULT"
        if not filtered_df.empty:
            analysis_result = filtered_df['CURRENT_RESULT'].iloc[0]
        else:
            analysis_result = analysis[2]

        if analysis_result != 'unknown':
            if isinstance(analysis[2], bool):
                if analysis[2].lower() in ('false', 'true'):
                    analysis[2] = analysis[2].capitalize()      # to ensure texts False and Text beginning with the capital letters
            if analysis[2] != analysis_result:
                analysis[2] = analysis_result       # e.g. also True is change to true
        
        change_type_list = analysis[0:2] + [change_type]
        
        return {f'{over}': [[analysis]]}, {f'{over}': [[change_type_list]]}            # 1st part returns (potentially) updated expectation value


# Compare expectations with results of analyzes
def compare_with_expectations():
    # Expectation analyzes results of all comparisons set in .json
    expectation_analyzes_results = []
    changed_type_results = []

    # Iterate through the length (number of items in) of dataset_dict
    for dict_index in range( len( st_gv.dataset_dict['object_names'] ) ):
        # Analyzes results of a single dataset
        comparison_analyzes = {}
        change_type_analyzes = {}

        # Iterate through key value pairs of dataset_dict
        for key_level_0, value_level_0 in st_gv.dataset_dict.items():
            # Execute table analyzes if key 'expectations' is found
            if key_level_0 == 'expectations':
                # Iterate through key value pairs of 'expectations' dictionary
                for key_level_1, value_level_1 in value_level_0[dict_index].items():
                    # Get lists inside values_level_1
                    for value_level_2 in value_level_1:
                        # Get analysis and added it to other analyzes
                        result_for_comparison_analyzes, result_for_changed_type = (
                            __execute_analysis(
                                object_name = st_gv.dataset_dict['object_names'][dict_index],
                                over = key_level_1,
                                ## over = re.sub('^\d+', '', key_level_1),     # remove digits at the beginning using a regular expression
                                analysis = value_level_2
                                ## order = dict_index
                            )
                        )

                        # Append or insert the 1st value into the analysis result to comparison_analyzes
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

                        # Append or insert the 1st value into the analysis result to change_type_analyzes
                        for key, value in result_for_changed_type.items():
                            if key in change_type_analyzes:          # Append a value
                                if isinstance(change_type_analyzes[key], list):
                                    if isinstance(value[0], list):  # Extend if value[0] is a list otherwise append
                                        change_type_analyzes[key].extend(value[0])
                                    else:
                                        change_type_analyzes[key].append(value[0])       # Table analyzes: needed if more than 2 items are processed; # append() also adds [] as part of value
                                else:
                                    if isinstance(value[0], list):  # Concatenate if value[0] is a list otherwise create a new list
                                        change_type_analyzes[key] = [change_type_analyzes[key]] + value[0]
                                    else:
                                        change_type_analyzes[key] = [change_type_analyzes[key], value[0]]    # Table analyzes: needed for the 2nd item 
                            else:   # Insert the 1st value
                                if isinstance(value[0], list):  # assign directly if value[0] is a list otherwise wrap in a list
                                    change_type_analyzes[key] = value[0]                 # table analyzes: needed for the 1st item
                                else:
                                    change_type_analyzes[key] = [value[0]]

        # Append found analyzes results of a relevant single comparison
        expectation_analyzes_results.append(comparison_analyzes)
        changed_type_results.append(change_type_analyzes)

    # update dataset_dict by replacing of analyzes definitions from a user input .json file by found analyzes results
    st_gv.dataset_dict['expectations'] = expectation_analyzes_results
    st_gv.dataset_dict['change_type'] = changed_type_results