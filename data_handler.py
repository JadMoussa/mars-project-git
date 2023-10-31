import pandas as pd
import psycopg2 as ps

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
    
    
# generate create statement from a dataframe
# generate an insert statement from a da taframe.

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

#     if dataframe.empty:
#         raise ValueError("DataFrame is empty.")

#     if not table_name:
#         raise ValueError("Table name is required.")

#     # Generate the list of column names
#     columns = ', '.join(dataframe.columns)

#     # Generate placeholders for values based on the number of columns
#     placeholders = ', '.join(['%s'] * len(dataframe.columns))

#     # Create the SQL INSERT statement
#     insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

#     return insert_statement

def update_and_insert(dataframe, table_name, connection):
    try:
        cursor = connection.cursor()

        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, name VARCHAR);")
        connection.commit()

        for index, row in dataframe.iterrows():
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = %s;", (row['id'],))
            existing_record = cursor.fetchone()

            if existing_record:
                cursor.execute(f"UPDATE {table_name} SET name = %s WHERE id = %s;", (row['name'], row['id']))
            else:
                cursor.execute(f"INSERT INTO {table_name} (name) VALUES (%s);", (row['name'],))

        connection.commit()

        print(f"Data updated and inserted into {table_name} successfully.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        cursor.close()

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

def create_dimension(connection, dimension_name, columns):
   
    try:
        cursor = connection.cursor()

        create_table_query = f"CREATE TABLE IF NOT EXISTS {dimension_name} ({', '.join([f'{col} {data_type}' for col, data_type in columns.items()])});"
        cursor.execute(create_table_query)

        connection.commit()

        print(f"Dimension table {dimension_name} created successfully.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        cursor.close()

def create_fact(connection, fact_name, columns):
    try:
        cursor = connection.cursor()

        create_table_query = f"CREATE TABLE IF NOT EXISTS {fact_name} ({', '.join([f'{col} {data_type}' for col, data_type in columns.items()])});"
        cursor.execute(create_table_query)

        connection.commit()

        print(f"Fact table {fact_name} created successfully.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        cursor.close()

