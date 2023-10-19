import pandas as pd

def returns_query_as_dataframe(db_session, sql_query):
    return_dataframe =  pd.read_sql_query(sql_query, db_session)
    return return_dataframe

def read_data_as_dataframe(source_type, source_path_or_query, **kwargs):
    if source_type == 'csv':
        return pd.read_csv(source_path_or_query, **kwargs)
    elif source_type == 'excel':
        return pd.read_excel(source_path_or_query, **kwargs)
    elif source_type == 'database':
        raise NotImplementedError("Database source is not implemented.")
    else:
        raise ValueError("Unsupported source type: " + source_type)
    
csv_dataframe = read_data_as_dataframe('csv', 'data.csv')

excel_dataframe = read_data_as_dataframe('excel', 'Fifa_world_cup_matches.xlsx', sheet_name='Sheet1')
