from enum import Enum


class ErrorHandling(Enum):
    DB_CONNECTION_ERROR = "Failed to connect to database (database_handler.py)"

class FilterFields(Enum):
    RENTAL_RATE = 'rental_rate'

class TableType(Enum):
    DIMENSION = 'dim'
    FACT = 'fact'


class ETLStep(Enum):
    PREHOOK = 'prehook'
    HOOK = 'hook'
    POSTHOOK = 'posthook'
    
class DataSourceFileIDs(Enum):
    Players = '1WJdoQ8oQ7qETcbu-OOgkd6t7UAAN59sx'
    Groups = '1WJdoQ8oQ7qETcbu-OOgkd6t7UAAN59sx'
    Teams = '1XS9INg_SJ33Wp9MuTKi-PKCyKUJbgqH7'
    Winners = '1On6XlGXAdGk3vJkbdlM8A8xvQbG7_pRh'