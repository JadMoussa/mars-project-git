from enum import Enum


class ErrorHandling(Enum):
    DB_CONNECTION_ERROR = "Failed to connect to database (database_handler.py)"

class FilterFields(Enum):
    RENTAL_RATE = 'rental_rate'

class TableType(Enum):
    DIMENSION = 'dim'
    FACT = 'fact'