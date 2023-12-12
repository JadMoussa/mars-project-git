import pandas as pd
import lookups
import error_handler
import json
import requests
import psycopg2
from datetime import datetime
import database_handler
import os

def execute_sql_commands(db_session, etl_step):
        # Zahi
        sql_command_directory_path = './SQL_Commands'
        sql_files = [sqlfile for sqlfile in os.listdir(sql_command_directory_path) if sqlfile.endswith('.sql')]        
        sorted_sql_files = sql_files 
        for sql_file in sorted_sql_files:
            if sql_file.split('__')[1].split('_')[0] == etl_step.value:
                # Read the SQL query from the file
                with open(os.path.join(sql_command_directory_path, sql_file), 'r') as file:
                    sql_query = file.read()
                    database_handler.execute_query(db_session, sql_query)

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
    
    #fouad create statement
def return_create_statement_from_df(dataframe, schema_name, table_name):
    type_mapping = {
        'int64':'INT',
        'float64':'FLOAT',
        'object':'TEXT',
        'datetime64[ns]':'TIMESTAMP'
    }
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
        # remove print and add logger
        print(f"ETL watermark for {staging_table} updated successfully.")
    except Exception as e:
        # remove print and add logger
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



def generate_insert_statement(dataframe, table_name):
    if dataframe.empty:
        raise ValueError("DataFrame is empty.")
    if not table_name:
        raise ValueError("Table name is required.")
    # Generate the list of column names
    columns = ', '.join(dataframe.columns)
    # Generate placeholders for values based on the number of columns
    placeholders = ', '.join(['%s'] * len(dataframe.columns))
    # Create the SQL INSERT statement
    insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    return insert_statement

def return_data_as_dataframe(file_path, file_type, conn=None):
    df = None
    try:

        if file_type == lookups.FileHandling.CSV.value:
           df = pd.read_csv(file_path)
        elif file_type == lookups.FileHandling.XLSX:
             df = pd.read_excel(file_path)
        elif file_type == lookups.FileHandling.JSON:
          with open(file_path, 'r') as json_file:
            df = json.load(json_file)
        elif file_type == lookups.FileHandling.API:        
            response = requests.get(file_path)
            if response.status_code == 200:
               df = response.json()
        elif file_type == lookups.FileHandling.SQL:
             query = return_sql_file(file_path)
             df = pd.read_sql(query,conn)
    except Exception as error:
        prefix = lookups.ErrorHandling.Data_handler_error.value
        suffix = str(error)
        level = lookups.ErrorLevel.ERROR.value
        message = 'Error happenend'
        error_handler.print_error(suffix,prefix,level,message)
        return None
    finally:
        return df

    

def return_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
        return sql_query 
    

def return_create_statement_from_df(dataframe, schema_name, table_name):
    type_mapping = {
        'int64':'INT',
        'float64':'FLOAT',
        'object':'TEXT',
        'datetime64[ns]':'TIMESTAMP'
    }
    fields = []
    for column, dtype in dataframe.dtypes.items():
        sql_type = type_mapping.get(str(dtype), 'TEXT')
        fields.append(f"{column} {sql_type}")
   
    create_table_statement = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ( \n"
    create_table_statement += "ID SERIAL PRIMARY KEY,\n"
    create_table_statement += ',\n'.join(fields)
    create_table_statement += ");"
    return create_table_statement


def return_insert_statement(dataframe, table_name, schema):
    columns = ','.join(dataframe.columns)
    insert_statements = []

    for index, row in dataframe.iterrows():
        values_list = []
        for val in row.values:
            val_type = str(type(val))
            if val_type == lookups.HandledType.TIMESTAMP.value:
                values_list.append(str(val))
            elif val_type == lookups.HandledType.STRING.value:
                values_list.append(f"'{val}'")
            elif val_type == lookups.HandledType.LIST.value:
                val_item = ';'.join(val)
                values_list.append(f"'{val_item}'")
            else:
                values_list.append(str(val))

        values = ', '.join(values_list)
        insert_statement = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({values});"
        insert_statements.append(insert_statement)

    return insert_statements


def populate_dfs():
    df_list = list()
    dict_item = {}
    dict_item['table_name'] = 'stg_alfsite_player'
    dict_item['file_id'] = lookups.DataSourceFileIDs.Players.value
    df_list.append(dict_item)
    dict_item = {}
    dict_item['table_name'] = 'stg_alfsite_winner'
    dict_item['file_id'] = lookups.DataSourceFileIDs.Winners.value
    df_list.append(dict_item)
    dict_item = {}
    dict_item['table_name'] = 'stg_alfsite_group'
    dict_item['file_id'] = lookups.DataSourceFileIDs.Groups.value
    df_list.append(dict_item)
    dict_item = {}
    dict_item['table_name'] = 'stg_alfsite_team'
    dict_item['file_id'] = lookups.DataSourceFileIDs.Teams.value
    df_list.append(dict_item)
    return df_list


def return_insert_statement_for_watermark(timestamp_str, table_name, schema):
     
    timestamp = datetime.strptime(timestamp_str, '%d/%m/%Y %H:%M')
    insert_statement = f"INSERT INTO {schema}.{table_name} (etl_last_execution_time) VALUES ('{timestamp}') RETURNING id;"

    return insert_statement


def return_df_from_drive(file_id_value):
    url = f'https://drive.google.com/uc?id={file_id_value}'
    return pd.read_csv(url,encoding='latin-1')
