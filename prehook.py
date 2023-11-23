import database_handler
import data_handler
import glob
# execute sql commands that are for the prehook.
def execute_sql_commands(db_session):
    # read all files ending with ".sql"
    sql_files = []
    for sql_file in sql_files:
        if sql_file.split('_')[1] == 'prehook':
            # read the content of the file
            sql_file_content = None
            database_handler.execute_query(db_session, sql_file_content)
            
def return_list_of_all_csvs():
    csv_list = []
    csv_list.append('')
    csv_list.append('')
    csv_list.append('')
    csv_list.append('')
    csv_list.append('')
    return csv_list

def create_staging_tables(db_session, csv_list):
    for csv_item in csv_list:
        table_name = csv_item.split('\\')[-1].replace('.csv', '').lower()
        csv_df = data_handler.read_data_as_dataframe(csv_item)
        create_statement = data_handler.generate_create_table_statement(csv_df, table_name)
        database_handler.execute_query(db_session, create_statement)

def execute():
    db_session = database_handler.create_connection()
    execute_sql_commands(db_session)
    return_list_of_all_csvs()
    create_staging_tables()
    # close connection

def execute(db_session):
    sql_files = glob.glob("**/*.sql")

    for sql_file in sql_files:
         
        file_name = sql_file.split("\\")[-1]
         
        if "_hook" in file_name:
            query = None
            print(file_name)   
            
            with open(sql_file, "r") as f:
                query = f.read()
            database_handler.execute_query(db_session, query)
            db_session.commit()