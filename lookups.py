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
    Players = '10Gr3uIseswYztzYoPYGuLv4Fz2QCroe_'
    Groups = '-1Xj-21aJ-nLjSGI2h25vTE5g7uQ5sdeM3'
    Teams = '1izgB8Wn62xzs0a-RcTs-Pu_iFOCgMhr1'
    Winners = '10YN4jxi_nx1FHMlxlklGzXu_H5Zk5DTp'