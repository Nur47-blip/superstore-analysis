'''
=================================================
Graded Challenge 7

Nama  : Muhammad Nur Alamsyah
Batch : FTDS-023-RMT

Program ini dibuat untuk melakukan load data dari file CSV ke PostgreSQL (menggunakan pgAdmin4),
kemudian insert data tersebut ke ElasticSearch. Adapun dataset yang dipakai adalah dataset penjualan Superstore (toko retail online)
di United States dari tahun 2014 hingga 2017
=================================================
'''

import pandas as pd
import psycopg2 as db
import warnings
from elasticsearch import Elasticsearch, helpers


def get_data_from_postgresql(dbname, host, user, password, table):
    '''
    This function fetches data from a PostgreSQL database, which can then be used for data cleaning purposes.

    Parameters:
    - dbname (str): The name of the database where data is stored.
    - host (str): The location/address of the PostgreSQL server.
    - user (str): The username used for accessing the database.
    - password (str): The password associated with the given user.
    - table (str): The name of the table containing the desired data.

    Returns:
    - data (Pandas DataFrame): A pandas DataFrame containing the queried data.

    Example usage:
    data = get_data_from_postgresql('gc7', 'localhost', 'postgres', 'password', 'table_gc7')
    '''

    # Construct the connection string for PostgreSQL using the provided variables
    conn_string = f"dbname={dbname} host={host} user={user} password={password}"

    # Establish a connection to the PostgreSQL database using the constructed connection string
    conn = db.connect(conn_string)

    # Query the database using the provided table name and store the result in a pandas DataFrame
    data = pd.read_sql(f"select * from {table}", conn)

    # Return the DataFrame containing the queried data
    return data


def clean_data(df, date_cols):
    '''
    This function cleans the input data by handling missing values, removing duplicates, 
    and standardizing column names. Additionally, specified columns are converted to pandas datetime format.

    Parameters:
    - df (Pandas DataFrame): The input DataFrame to be processed.
    - date_cols (list of str): Columns within the DataFrame to be converted into pandas datetime format.

    Returns:
    - data (Pandas DataFrame): The processed and cleaned DataFrame.

    Example usage:
    cleaned_data = clean_data(data, ['Order Date', 'Ship Date'])
    '''

    # Convert specified columns to pandas datetime format
    df[date_cols] = df[date_cols].apply(pd.to_datetime)

    # Standardize column names by converting to lowercase and replacing spaces and dashes with underscores
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')

    # Drop rows with missing values
    df = df.dropna()

    # Remove any duplicate rows
    df = df.drop_duplicates()

    # Return the cleaned and processed dataframe
    return df


def save_clean_data(data, filename):
    '''
    Save the provided DataFrame to a CSV file.

    Parameters:
    - data (Pandas DataFrame): DataFrame to be saved.
    - filename (str): Desired file name for the CSV.

    Returns:
    This function has no return value

    Example usage:
    save_to_csv(cleaned_data, 'P2G7_nur_alamsyah_data_clean.csv')
    '''

    # Write the DataFrame to a CSV file without the index column
    data.to_csv(filename, index=False)

    # Notify the user of successful save operation
    print(f'Data successfully saved to {filename}')


def insert_into_es(url, df, index_name):
    '''
    This function inserts the provided DataFrame into an ElasticSearch instance at the specified URL.

    Parameters:
    - url (str): Endpoint URL of the ElasticSearch instance.
    - df (Pandas DataFrame): DataFrame to be inserted into ElasticSearch.
    - index_name (str): Name of the target index in ElasticSearch.

    Returns:
    - This function has no return value

    Example usage:
    insert_into_es('http://localhost:9200', cleaned_data, 'superstore')
    '''

    # Initialize the ElasticSearch instance
    es = Elasticsearch(url)

    # Display the connectivity status of the ElasticSearch instance
    print(es.ping())

    # Process each row from the DataFrame for insertion
    for _, row in df.iterrows():
        # Transform the row to a dictionary format
        doc = row.to_dict()
        # Insert the transformed row (document) into the specified Elasticsearch index
        es.index(index=index_name, body=doc)



if __name__ == "__main__":
    
    # Ignore display of UserWarnings for cleaner output
    warnings.simplefilter(action='ignore', category=UserWarning)

    # Retrieve data from the PostgreSQL database.
    data = get_data_from_postgresql('gc7', 'localhost', 'postgres', 'password', 'table_gc7')
    
    # Clean data by handling missing values, standardizing column names, and removing duplicates, while also converting the date column to the datetime format.
    cleaned_data = clean_data(data, ['Order Date', 'Ship Date'])
    
    # Store the cleaned data into a CSV file.
    save_clean_data(cleaned_data, 'P2G7_nur_alamsyah_data_clean.csv')
    
    # Insert the cleaned data into an ElasticSearch index.
    insert_into_es('http://localhost:9200', cleaned_data, 'superstore')


