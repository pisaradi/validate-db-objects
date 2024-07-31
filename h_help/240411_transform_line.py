# convert a string separated by commas into individual lines
# ----------------------------------------------------------
# START - USER INPUT SETTINGS
# ---------------------------
# define the text that you want to split by commas between ''' '''
columns = '''
    CUSTOMER_NUMBER, CUSTOMER_NAME, PEN_CUSTOMER_CODE, PEN_CUSTOMER_NAME, ADDRESS1, ADDRESS2, ADDRESS3, ADDRESS4, POST_CODE, COUNTRY_OF_ORIGIN, COUNTRY_OF_ORIGIN_ISO_CODE, BUYING_GROUP_L1, BUYING_GROUP_L2, LEGACY_ERP_ACCOUNT, INTER_GROUP_IND, VENDOR_PARTNERSHIP, PARTNER_FLAG_HIK, PARTNER_FLAG_SMT, PARTNER_FLAG_DAH, CUSTOMER_CREATED_DATE, SOURCE_SYSTEM, GLOBAL_REGION_NAME, WORLD_ZONE_NAME, PRICE_GROUP, TRADE_PROFILE, BUSINESS_OWNERSHIP_TYPE, SALES_PERSON_SALES_TEAM1, SALES_PERSON_SALES_ROLE1, SALES_PERSON_NAME1, SALES_PERSON_CODE1, SALES_PERSON_NAME2, SALES_PERSON_CODE2, SALES_PERSON_NAME3, SALES_PERSON_CODE3, SALES_PERSON_NAME4, SALES_PERSON_CODE4, SALES_PERSON_NAME5, SALES_PERSON_CODE5, SALES_PERSON_ID1, SALES_PERSON_ID2, SALES_PERSON_ID3, SALES_PERSON_ID4, SALES_PERSON_ID5, SALES_PERSON_PID1, SALES_PERSON_PID2, SALES_PERSON_PID3, SALES_PERSON_PID4, SALES_PERSON_PID5, PAYMENT_TERM_ID, EXPORT_INDICATOR, CLASS_OF_BUSINESS, CUSTOMER_CTS, CUSTOMER_CARE_REP, OWNING_BRANCH_CODE, OWNING_BRANCH_NAME, OWNING_BRANCH_LABEL, STRATIFICATION_SEGMENT_CODE, STRATIFICATION_SEGMENT, COUNTRY_ID, SALES_ORG, DIST_CHAN, DIVISION, CUST_SALES_ID
    '''

# define the suffix for each value
suffix = '__SF'
# -------------------------
# END - USER INPUT SETTINGS

def convert_line_to_lines():
    # open file "transformed_line.txt" and overwrite it
    with open('transformed_line.txt', 'w') as file:
        # split the string by comma and write splitted values with a comma to the end (new line) of the opened file
        for value in columns.split(','):
            file.write(' ' * 8 + value.strip() + ' as ' + value.strip() + suffix + ',\n')


if __name__ == '__main__':
    convert_line_to_lines()
