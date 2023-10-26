import pandas as pd

dataframe = pd.read_csv('your_data.csv')
def return_dataframe_info(dataframe):
    info_dictionary = dict()
    info_dictionary['dataframe_length'] = len(dataframe)
    info_dictionary['total_count_of_table_id'] = int(dataframe.describe().iloc[0][0])
    return info_dictionary

def return_dataframe_subset_from_to_location(dataframe, from_loc, to_loc):
    return dataframe.loc[from_loc,to_loc]


def return_filtered_dataframe(dataframe, filter_field, filter_value):
    return dataframe[dataframe[filter_field.value] > filter_value]



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



# Generate a CREATE TABLE statement
create_table_statement = generate_create_table_statement(dataframe, 'your_table_name', primary_key='id')

# Print the generated CREATE TABLE statement
print(create_table_statement)



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

