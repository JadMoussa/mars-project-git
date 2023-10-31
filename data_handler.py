import pandas as pd

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
