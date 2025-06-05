class DataframeCreateException(Exception):
    """Exception raised when a dataframe cannot be created."""
    pass

class StorageReadException(Exception):
    """Exception raised when storage cannot be read."""
    pass

class StorageWriteException(Exception):
    """Exception raised when storage cannot be written."""
    pass