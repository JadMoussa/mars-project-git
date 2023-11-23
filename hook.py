import database_handler
import data_handler
import lookups

def execute():
    db_session = database_handler.create_connection()
    data_handler.execute_sql_commands(db_session, lookups.ETLStep.HOOK)
    # execute sql commands that are for the prehook.