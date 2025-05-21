class ExistingMongoConnection(Exception):
    def __init__(self) -> None:
        self.message = "Mongodb connection already exists"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class ExistingKafkaConnection(Exception):
    def __init__(self) -> None:
        self.message = "Kafka connection already exists"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class EnvironmentConfigMissing(Exception):
    def __init__(self, e) -> None:
        self.message = f"Environment variable {e} is missing in the configuration file"
        super().__init__(self.message)
    def __str__(self) -> str:
        return self.message
class IonAPIFetchException(Exception):
    def __init__(self, err):
        self.message = f"Error in fetching Ion API Credentials {err}"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class DelimiterUpdateFailedException(Exception):
    def __init__(self, object) -> None:
        self.message = f"Failed to set or update the delimiters for {object}"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class M3ProgramMetadataRetrievalFailure(Exception):
    def __init__(self, message) -> None:
        self.message = f"Error in fetching for M3 program metadata: {message}"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class MetadataFetchFailedException(Exception):
    def __init__(self, object) -> None:
        self.message = f"Metadata fetch operation failed for object: {object}"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class SaveMetadataFailedException(Exception):
    def __init__(self, object) -> None:
        self.message = f"Failed to save the metadata for object: {object}"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class DatabaseOrDataStoreDetailsRetrievalException(Exception):
    def __init__(self, message) -> None:
        self.message = f"Failed to fetch the database details: {message}"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class EnvironmentNameMissing(Exception):
    def __init__(self) -> None:
        self.message = "Environment name is missing"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class DeltaLakeConnectionDetailsRetrievalException(Exception):
    def __init__(self, message) -> None:
        self.message = f"Failed to fetch the deltalake connection details: {message}"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class UnableToGenerateM3TokenException(Exception):
    def __init__(self) -> None:
        self.message = "Error occurred while generating the token."
        super().__init__(self.message)
    def __str__(self):
        return self.message
class UnableToFetchUserDetailsException(Exception):
    def __init__(self, user_id) -> None:
        self.message = f"Failed to fetch the user information for user id {user_id}."
        super().__init__(self.message)
    def __str__(self):
        return self.message
class DatameshConfigurationExceptions(Exception):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)
    def __str__(self):
        return self.message
New code
class TokenGenerationExceptions(Exception):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)
class DA2MetaDataMismatch(Exception):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)
    def __str__(self):
        return self.message
class DA2MetaDataNotFound(Exception):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)
    def __str__(self):
        return self.message
class DATMetaDataNotFound(Exception):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)
    def __str__(self):
        return self.message
class ServiceTokenException(Exception):
    def __init__(self, message) -> None:
        self.message = f"Unable to retrieve the service token: {message}."
        super().__init__(self.message)
    def __str__(self):
        return self.message
class ServiceRegistrationFailed(Exception):
    def __init__(self) -> None:
        self.message = f"Service registration with IAM failed."
        super().__init__(self.message)
Uncovered code
    def __str__(self):
        return self.message