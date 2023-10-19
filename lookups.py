from enum import Enum


class ErrorHandling(Enum):
    DB_CONNECTION_ERROR = "Failed to connect to database (database_handler.py)"

class FilterFields(Enum):
    RENTAL_RATE = 'rental_rate'

# Import the FileTypes class from the lookups module
from lookups import FileTypes

# Example usage
data_source = "data.csv"

if data_source.endswith(FileTypes.CSV):
    # Process the data as a CSV file
    print("Processing a CSV file")
elif data_source.endswith(FileTypes.JSON):
    # Process the data as a JSON file
    print("Processing a JSON file")
elif data_source.startswith(FileTypes.API):
    # Fetch data from an API
    print("Fetching data from an API")
else:
    print("Unsupported data source")
