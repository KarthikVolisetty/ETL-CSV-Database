**ETL: CSV to Database**

**README.md**
# ETL: CSV to Database

## Description
This ETL process reads data from a CSV file, transforms the column names for readability, and loads the data into a PostgreSQL database table.

## Key Features
- **Data Source**: CSV file (`Input_DataSet.csv`).
- **Transformations**:
  - Renamed columns to a standardized format for readability.
- **Database**: PostgreSQL.

## Prerequisites
1. Install the required Python libraries:
   ```bash
   pip install pandas psycopg2
   ```
2. Ensure you have PostgreSQL installed and running.
3. Create a 'Practice' database in PostgreSQL (or update the script to match your database).

## Files
- `Input_DataSet.csv`: Input file containing the data.
- `ETL_CSV_to_Database.py`: Python script to execute the ETL process.

## How to Run
1. Place `Input_DataSet.csv` in the specified directory.
2. Update the `db_config` in the script with your database credentials.
3. Execute the script:
   ```bash
   python etl_csv_to_db.py
   ```
4. Check the `GYM` table in your PostgreSQL database for the loaded data.

## Output
- The script will create a table named `GYM` in your PostgreSQL database and load the transformed data into it.
