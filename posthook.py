import data_handler
import database_handler

def truncate_staging_tables(db_session):
    dict_list = data_handler.populate_dfs()
    for dict_item in dict_list:
        table_name = dict_item.get('table_name')
        query = f"TRUNCATE TABLE IF EXISTS {table_name}"
        database_handler.execute_query(db_session, query)


def execute():
    db_session  = database_handler.create_connection()
    truncate_staging_tables(db_session)
    database_handler.close_connection(db_session)

