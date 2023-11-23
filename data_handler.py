from asyncio import DatagramProtocol
import dataclasses
import pandas as pd
import psycopg2 as ps
from lookups import FileType,ErrorHandling,HandledType
from db_handler import create_connection,close_connection,execute_query
import os
from datetime import time
import psycopg2
import json
from datetime import datetime

def returns_query_as_dataframe(db_session, sql_query):
    return_dataframe =  pd.read_sql_query(sql_query, db_session)
    return return_dataframe

def read_data_as_dataframe(source_type, source_path_or_query):
    if source_type == 'csv':
        return pd.read_csv(source_path_or_query)
    elif source_type == 'excel':
        return pd.read_excel(source_path_or_query)
    elif source_type == 'database':
        raise NotImplementedError("Database source is not implemented.")
    else:
        raise ValueError("Unsupported source type: " + source_type)
    
    
def generate_create_table_statement(dataframe, table_name, primary_key=None):
    if dataframe.empty:
        raise ValueError("DataFrame is empty.")

    if not table_name:
        raise ValueError("Table name is required.")

    # Create an empty list to store column definitions
    column_definitions = []

    # Iterate through columns in the DataFrame and define column names and data types
    for column_name, dtype in dataframe.dtypes.items():
        if dtype == 'object':
            data_type = 'TEXT'
        elif dtype == 'int64':
            data_type = 'INTEGER'
        elif dtype == 'float64':
            data_type = 'REAL'
        else:
            data_type = 'TEXT'  # Use TEXT for unsupported data types

        column_definition = f"{column_name} {data_type}"
        column_definitions.append(column_definition)

    # If a primary key is specified, include it in the column definitions
    if primary_key and primary_key in dataframe.columns:
        column_definitions.append(f"PRIMARY KEY ({primary_key})")

    # Combine the column definitions into a CREATE TABLE statement
    create_table_statement = f"CREATE TABLE {table_name} ({', '.join(column_definitions)})"

    return create_table_statement

def generate_insert_statement(dataframe, table_name):
    if dataframe.empty:
        raise ValueError("DataFrame is empty.")
    if not table_name:
        raise ValueError("Table name is required.")
    columns = ', '.join(dataframe.columns)
    placeholders = ', '.join(['%s'] * len(dataframe.columns))
    insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    return insert_statement

def create_etl_watermark(connection, staging_table, etl_timestamp):
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS etl_watermark (staging_table TEXT PRIMARY KEY, last_etl_timestamp TIMESTAMP);")

        cursor.execute("SELECT * FROM etl_watermark WHERE staging_table = %s;", (staging_table,))
        existing_watermark = cursor.fetchone()

        if existing_watermark:
            cursor.execute("UPDATE etl_watermark SET last_etl_timestamp = %s WHERE staging_table = %s;", (etl_timestamp, staging_table))
        else:
            cursor.execute("INSERT INTO etl_watermark (staging_table, last_etl_timestamp) VALUES (%s, %s);", (staging_table, etl_timestamp))
        connection.commit()
        print(f"ETL watermark for {staging_table} updated successfully.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        cursor.close()

# create the same function and have them both being the same
# in case you need to create a dim / fct you can create those inside the SQL_Commands (predefined columns)
#  if you need them to be here, then ok, but have them both under a function called create_tables(table_name)
def create_dimension(connection, table_name, table_type, columns):
    try:
        cursor = connection.cursor()
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} {data_type}' for col, data_type in columns.items()])});"
        cursor.execute(create_table_query)
        connection.commit()
        # remove print and add logger
        if table_type == lookups.TableType.DIMENSION:
            print(f"Dimension table {table_name} created successfully.")
        elif table_type == lookups.TableType.FACT:
            print(f"Fact table {table_name} created successfully.")            
    except Exception as e:
        # remove print and add logger
        print(f"An error occurred: {str(e)}")
    finally:
        cursor.close()


    if data is None:
        print("Error: Data is None.")
        return
    
    conn = create_connection()
    
    if conn is None:
        return
    
    try:
        with conn.cursor() as cursor:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                insert_statements = insert_statements_dataframe(batch, schema_name, table_name)
                
                with conn:
                    with conn.cursor() as cursor:
                        for statement in insert_statements:
                            execute_query(conn, statement)
                    
                    reco_etlwatermark(conn, schema_name, watermark_table_name,table_name)
        
        conn.commit()
        print("Data inserted successfully in batches.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error inserting data in batches: {error}")
    finally:
        if conn:
            conn.close()