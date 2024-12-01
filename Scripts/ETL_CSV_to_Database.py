# -*- coding: utf-8 -*-
"""
Created on Sun Nov 24 17:59:01 2024

@author: karth
"""

import pandas as pd
import psycopg2

# -------------------------------------  Stage - 1: Extract Data  --------------------------------------------- #
# This stage is responsible for extracting data from the provided CSV file.

# Read the CSV file into a pandas DataFrame from the local directory.
read = pd.read_csv(r"C:\MyStuff\DataEngineering\Projects\ETL_CSV_to_DataBase\Input\Input_DataSet.csv")
df = pd.DataFrame(read)

# Uncomment the following line to display the entire DataFrame for verification.
print("Extracted Data:\n", df.to_string())

# Uncomment the following line to display the list of column names.
# print("Columns in the dataset:", df.columns.tolist())


# ------------------------------------  Stage - 2: Transform Data  --------------------------------------------- #
# This stage transforms the raw data by renaming columns for better readability and consistency.

# Rename specific columns for database compatibility and readability.
renamed_data = df.rename(
    columns={
        'Weight (kg)': 'Weight_kg',
        'Height (m)': 'Height_cm',
        'Session_Duration (hours)': 'Session_Duration_hours',
        'Water_Intake (liters)': 'Water_Intake_liters',
        'Workout_Frequency (days/week)': 'Workout_Frequency_days_week'
    }
)

# Uncomment the following lines to display the renamed column list and transformed DataFrame.
# print("\nColumn Names after Renaming:\n", renamed_data.columns.tolist())
# print("Transformed Data:\n", renamed_data)

# Display the data types of the transformed DataFrame for verification.
print("\nData Types of Transformed Data:\n", read.dtypes)


# ------------------------------------  Stage - 3: Load Data  -------------------------------------------------- #
# This stage loads the transformed data into a PostgreSQL database.

# Database configuration parameters for connecting to the PostgreSQL database.
db_config = {
    'host': 'localhost',
    'database': 'Practice',
    'user': 'postgres',
    'password': 'Karthik@1261'
}

# Establish a connection to the database.
conn = psycopg2.connect(**db_config)
cur = conn.cursor()
print("\nDatabase Connection Successful")

# Declare the table name where the data will be inserted.
table_name = 'GYM'

# Dynamically generate column definitions for the SQL CREATE TABLE statement based on the DataFrame's data types.
columns = []
for column_name, dtype in zip(renamed_data.columns, renamed_data.dtypes):
    if dtype == 'int64':
        pg_type = 'INTEGER'
    elif dtype == 'float64':
        pg_type = 'FLOAT'
    elif dtype == 'bool':
        pg_type = 'BOOLEAN'
    else:
        pg_type = 'TEXT'
    columns.append(f"{column_name} {pg_type}")

print(columns)
# Create the table in the database if it does not already exist.
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"
cur.execute(create_table_query)
conn.commit()
print(f"Table '{table_name}' created successfully.")

# Iterate over the rows of the transformed DataFrame and insert them into the database.
for index, row in renamed_data.iterrows():
    # Prepare the column names and values for the INSERT statement.
    columns = ', '.join(row.index)
    values = ', '.join(['%s'] * len(row))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
    
    # Execute the INSERT query with the row values.
    cur.execute(insert_query, tuple(row.values))

# Commit the transaction to save changes to the database.
conn.commit()
print("Data loaded successfully")

# Close the database cursor and connection.
cur.close()
conn.close()








