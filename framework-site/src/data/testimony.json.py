import duckdb
import pandas as pd
import json
import sys

def load_data_from_duckdb(query):
    # Connect to the DuckDB database
    conn = duckdb.connect('../data/maine_legislation_and_testimony.duckdb', read_only=True)

    # Execute the query and fetch the results as a pandas DataFrame
    df = conn.query(query).df()

    # Convert the DataFrame to a JSON string
    json_data = df.to_json(orient='records')

    # Close the connection
    conn.close()

    return json_data

# Example query
query = '''
    SELECT *
    FROM BCON.V_TESTIMONY_HEADER
    LIMIT 10
'''

# Load the data
data = load_data_from_duckdb(query)

# Write the JSON data to a file
sys.stdout.write(data)