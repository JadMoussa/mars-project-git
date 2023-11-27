import database_handler
import data_handler
import lookups
# execute sql commands that are for the prehook.

def create_staging_tables(db_session):
    return_val = None
    try:

        df_list = data_handler.populate_dfs()
        for df in df_list:
            table_name = df.get('table_name')
            file_id_value = df.get('file_id')
            df = data_handler.return_df_from_drive(file_id_value)
            create_statement = data_handler.return_create_statement_from_df(df, 'dw_reporting', table_name)
            database_handler.execute_query(db_session, create_statement)
            return_val = True
    except Exception as e:
        print(str(e))
        return_val = False
    finally:
        return return_val


def execute(db_session):

    db_session = database_handler.create_connection()
    data_handler.execute_sql_commands(db_session, lookups.ETLStep.PREHOOK)
    create_staging_tables(db_session)
    database_handler.close_connection(db_session)
    # insert to staging tables.
   