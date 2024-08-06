# Project Snowflake Objects

## Purpose

- Project Snowflake Objects is focused on calculating chosen statistics of objects (or joined groups of objects) from Snowflake and/or Bluewater.

## Author

- Peter Pisár \
- peter.pisar@adiglobal.com

## License

- license.md

## Description

### Packages

| Folder/Package | Description                                                                                     |
| -------------- | ----------------------------------------------------------------------------------------------- |
| a_inputs       | The only location with all user input files in .json format                                     |
| b_settings     | Developer-defined configurations for connectors, fonts, display styles, etc.                    |
| c_data         | Dataframes (e.g., derived from user input .json files), other data structures, global variables |
| d_analyzes     | Analytical results derived from user input files                                                |
| e_outputs      | Displayed or saved results of analyzes                                                          |
| f_flow         | Control flows managing the processing from inputs to outputs                                    |
| g_tests        | Any kind of test codes not used in the final solution                                           |
| h_help         | Standalone solutions with the potential for integration into the final solution                 |

### JSON File Content

- "snowflake_object_definition.json" serves as a template, outlining the following general rules to filling the JSON file.
- The JSON file:
  - must begin with "{" and end with "}",
  - must contain correctly placed brackets, commas and quotation marks, and other elements, \
    as no validation is provided to assist the user.
  - Any main object name (the 1<sup>st</sup> level after "{") in the same JSON file:
    - can be any name chosen by the user,
    - can appear any number of times,
    - can contain almost any character in its name,
    - must be unique in the file.
- For better readability:
  - empty rows can be inserted between sections as needed,
  - it's recommended to place corresponding opening and closing brackets:
    - either on the same row with the text or nothing in between, or
    - on separate rows with the text indented in the following row.
- The table below explains the significance of sections under the main objects' names.
  - Text enclosed in \|\| does not represent actual options. \
    Instead, it indicates that the user must determine the appropriate text based on all information provided in the corresponding table row.

| Section         | Option           | Description                                                                  |
| --------------- | ---------------- | ---------------------------------------------------------------------------- |
| source          | Snowflake        | Data from Snowflake will be loaded                                           |
|                 | SQL Server       | Data from SQL Server will be loaded                                          |
| warehouse       | Snowflake        | Warehouse                                                                    |
|                 | SQL Server       | server_name\instance_name; '\\' are used to represent one '\' in .json files |
| database        | \|many options\| | Database name                                                                |
| schema          | \|many options\| | Schema name                                                                  |
| object          | \|many options\| | Table name or view name                                                      |
| dim_cols        | \|many options\| | Dimension columns                                                            |
| fact_cols       | \|many options\| | Fact columns                                                                 |
| table_analyzes  | count_of_rows    | Count of rows in a table                                                     |
|                 | count_of_columns | Count of columns in a table                                                  |
|                 | count_of_nulls   | Count of null values in a table                                              |
|                 | candidate_keys   | Columns that are candidate keys                                              |
|                 |                  | If a candidate key doesn't exist, then a primary key doesn't exist           |
| column_analyzes | min              | Minimum value in a selected column (is null included?)                       |
|                 | avg              | Average value in a selected column (is null included?)                       |
|                 | max              | Maximum value in a selected column (is null included?)                       |
|                 | is_unique        | Value 0 indicates the presence of duplicates, value 1 indicates uniqueness.  |
|                 | unique           | List of distinct (unique) values in a selected column                        |
|                 | count_of_nulls   | Count of null values in a selected column                                    |
| expectations    | table_analyzes   | Any expectations (not only from options) for table analyzes                  |
|                 | column_analyzes  | Any expectations (not only from options) for column analyzes                 |
| comments        | \|many options\| | Any suitable text that will not be further processed                         |
