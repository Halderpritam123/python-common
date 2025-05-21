class UnsupportedDataSourceException(Exception):
    def __init__(self, source_type) -> None:
        self.message = f"Unsupported data source '{source_type}'"
        super().__init__(self.message)
    def __str__(self) -> str:
        return self.message
class UnsupportedFormatException(Exception):
    def __init__(self, file) -> None:
        self.message = f"Unsupported file type: {file}"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class ColumnLimitExceededException(Exception):
    def __init__(self, limit) -> None:
        self.message = (
            f"Failed: Column limit exceeded. Maximum {limit} columns are supported."
        )
        super().__init__(self.message)
    def __str__(self):
        return self.message
class ConnectionNotFound(Exception):
    def __init__(self, id) -> None:
        self.message = f"No connection found with the ID: {id}"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class M3ProgramNotFound(Exception):
    def __init__(self, program) -> None:
        self.message = f"No M3 Program found with the name: {program}"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class PythonLibraryConfigFileNotFound(Exception):
    def __init__(self, message="No config file was found"):
        self.message = message
        super().__init__(message)
    def __str__(self):
        return self.message
class EC2InstanceNotFoundException(Exception):
    def __init__(self):
        self.message = "No EC2 instance was found"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class ConnectionNotFoundException(Exception):
    def __init__(self) -> None:
        self.message = "Unable to find a connection to the chosen database or datastore or deltalake."
        super().__init__(self.message)
    def __str__(self):
        return self.message
class ColumnsNotFoundException(Exception):
    def __init__(self, table):
        self.message = f"No columns found for the given table: {table}."
        super().__init__(self.message)
    def __str__(self):
        return self.message
