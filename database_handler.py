import psycopg2
import misc_handler
import lookups
import error_handler
import data_handler

# foufou
# create a config file
def create_connection():
    db_session = None
    try:
        # db_host, db_name, db_user, db_pass = misc_handler.get_db_params_from_config_file(config_file)
        #  i want you to read the data from a config file
        db_host = 'localhost'
        db_name = 'MarsProjet'
        db_user = 'postgres'
        db_pass = 'Fouadkb1'
        db_session = psycopg2.connect(
            host = db_host,
            database = db_name,
            user = db_user,
            password = db_pass,
            port = 5432
        )
    except Exception as error:
        prefix = lookups.ErrorHandling.DB_CONNECTION_ERROR.value
        suffix = str(error)
        error_handler.print_error(suffix, prefix)
    finally:
        return db_session

def execute_query(db_session, db_query):
    cursor = db_session.cursor()
    cursor.execute(db_query)
    cursor.close()  
    

def close_connection(db_session):
    if db_session.closed == 1:
        db_session.close()