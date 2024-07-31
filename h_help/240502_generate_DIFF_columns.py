# calculate the 3rd part with postfix __DIFF
#   this is a complementary script for variable dif_cols in 240407_compare_select_statements.py
# ------------------------------------------
user_input = '''
    ['COUNTRY_ID__SF', 'Country_ID__BW', ''],
    ['CS_ACCOUNT__SF', 'CsAccount__BW', ''],
    ['HASH_KEY__SF', 'HashKey__BW', ''],
    ['CS_CUSTOMER_NAME__SF', 'CsCustomerName__BW', ''],
    ['SALESPERSON_ID1__SF', 'SalesPerson_ID1__BW', ''],
    ['OWNINGBRANCH_ID__SF', 'OwningBranch_ID__BW', '']
    '''

# Split the text string into lines and remove empty lines
lines = [line.strip() for line in user_input.split('\n') if line.strip()]

# get index (iii) and value from every list element
for iii, line in enumerate(lines):
    # split a line into individual values
    values = [val.strip() for val in line.strip('[],').split(',')]  # strip() removes all spaces and characters between '' from the beginning and ending of a string
    
    # get snowflake and bluewater object names without postfixes
    text_sf = values[0].split('__SF')[0].strip("'")      # also working .strip('\'')
    text_bw = values[1].split('__BW')[0].strip("'")
   
    # replace the 3rd (empty) part by a new calculated value with postfix __DIFF
    values[2] = f"'{text_sf}_vs_{text_bw}__DIFF'"
    
    # update index (see above) line to the update line containing the 3rd part with postfix __DIFF
    lines[iii] = f"[{', '.join(values)}]"

# open file "transformed_lines.txt" and overwrite it
with open('transformed_lines.txt', 'w') as file:
    file.write(',\n'.join([' ' * 4 + line for line in lines]) + ',\n')      # join (transform) lines containing the 3rd part with postfix __DIFF into string
