# export SQLite database to excel
# -------------------------------
import sqlite3
import pandas as pd

# path to the source SQLite database - update before every execution if needed
db_path = r'C:\Users\267800\Work\Development.!x!\231218 - Snowflake\231218 - Core zone\analyzes\SQLite3_Database\240515-Phase_1-DCu06.db'

# source table name - update before every execution if needed
table_name = 'TblPostCode'

# connect to the SQLite database
conn = sqlite3.connect(db_path)

# load data from the table to dataframe
query = f'SELECT * FROM {table_name}'
df = pd.read_sql_query(query, conn)

# close the connection to the SQLite database
conn.close()

# export data to an excel file
output_path = r'C:\Users\267800\Work\Development.!x!\231218 - Snowflake\231218 - Core zone\analyzes\SQLite3_Database\SQLite_export.xlsx'
df.to_excel(output_path, index=False)
