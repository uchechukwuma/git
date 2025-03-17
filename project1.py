# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

# Wikipedia link used as scrapping source
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
# Exchange rate extracted from zip file as CSV
# Source: 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attribs = ['Name', 'MC_USD_Billion']
output_path = './Largest_banks_data.csv'
DatabaseName = "Banks.db"
tableName = 'Largest_banks'
logFileName = "./code_log.txt"

# Function to log progress messages
def logProgress(message):
    """
    Logs progress messages with a timestamp.

    Parameters:
    message (str): The message to log.
    """
    timeStampFormat = '%Y-%h-%d : %H:%M:%S' 
    now = datetime.now()
    timeStamp = now.strftime(timeStampFormat)
    with open(logFileName, "a") as f:
        f.write(timeStamp + ' : ' + message + '\n')

logProgress("Preliminaries complete. Initiating ETL process")

# Function to extract data from the given URL
def extract(url, table_attribs):
    """
    Extracts data from the given URL and returns a DataFrame.

    Parameters:
    url (str): The URL to scrape data from.
    table_attribs (list): List of table attributes to extract.

    Returns:
    DataFrame: Extracted data in a DataFrame.
    """
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    print(f"Found {len(tables)} tables.")
    rows = tables[0].find_all('tr')
    print(f"Rows found: {len(rows)}")
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[1].find('a') is not None:
                data_dict = {
                    "Name": [col[1].get_text(strip=True)],  
                    "MC_USD_Billion": [col[2].contents[0].strip()]
                }
                df1 = pd.DataFrame(data_dict) 
                df = pd.concat([df, df1], ignore_index=True)
    return df

df = extract(url, table_attribs)

# Function to transform the extracted data
def transform(df, csv_path):
    """
    Transforms the extracted data by converting market capitalization to multiple currencies.

    Parameters:
    df (DataFrame): The extracted data.
    csv_path (str): Path to the CSV file containing exchange rates.

    Returns:
    DataFrame: Transformed data.
    """
    # Ensure the column is numeric
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
    exchangeRate = pd.read_csv(csv_path)
    exchange_dict = exchangeRate.set_index('Currency').to_dict()['Rate']
    # Convert USD to other currencies
    df['MC_GBP_Billion'] = df['MC_USD_Billion'].apply(lambda x: np.round(x * exchange_dict['GBP'], 2))
    df['MC_EUR_Billion'] = df['MC_USD_Billion'].apply(lambda x: np.round(x * exchange_dict['EUR'], 2))
    df['MC_INR_Billion'] = df['MC_USD_Billion'].apply(lambda x: np.round(x * exchange_dict['INR'], 2))
    return df

csv_path = './exchange_rate.csv'
transform_df = transform(df, csv_path)

print(transform_df)
print(df['MC_EUR_Billion'][4])

# Function to load data to a CSV file
def load_to_csv(df, output_path):
    """
    Loads the DataFrame to a CSV file.

    Parameters:
    df (DataFrame): The data to save.
    output_path (str): The path to save the CSV file.
    """
    df.to_csv(output_path, index=False)

load_to_csv(df, output_path)
logProgress('Data saved to CSV file')

# Function to load data to a SQLite database
def load_to_db(df, sql_connection, table_name):
    """
    Loads the DataFrame to a SQLite database.

    Parameters:
    df (DataFrame): The data to load.
    sql_connection (Connection): SQLite connection object.
    table_name (str): The name of the table to create.
    """
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

logProgress('SQL Connection initiated.')
sql_connection = sqlite3.connect(DatabaseName)
load_to_db(df, sql_connection, tableName)
logProgress('Data loaded to Database as table.')

# Function to run SQL queries
def run_query(query_statement, sql_connection):
    """
    Runs a SQL query and prints the result.

    Parameters:
    query_statement (str): The SQL query to run.
    sql_connection (Connection): SQLite connection object.
    """
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

logProgress('Running the query')

query_statement = "SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)
logProgress('First query completed')

query_statement = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)
logProgress('Second query completed')

query_statement = "SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)
logProgress('Third query completed')

logProgress('Process Complete.')
sql_connection.close()