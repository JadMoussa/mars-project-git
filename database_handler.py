import psycopg2
import misc_handler
import lookups
import error_handler


# create a config file
def create_connection(config_file):
    db_session = None
    try:
        # db_host, db_name, db_user, db_pass = misc_handler.get_db_params_from_config_file(config_file)

        db_host = 'server'
        db_name = 'MarsProjet'
        db_user = 'postgres'
        db_pass = 'sql22'

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
