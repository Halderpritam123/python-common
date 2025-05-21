from .custom_exceptions import (
    ExistingMongoConnection,
    ExistingKafkaConnection,
    EnvironmentConfigMissing,
    IonAPIFetchException,
    DelimiterUpdateFailedException,
    M3ProgramMetadataRetrievalFailure,
    MetadataFetchFailedException,
    DatabaseOrDataStoreDetailsRetrievalException,
    SaveMetadataFailedException,
    EnvironmentNameMissing,
    UnableToFetchUserDetailsException,
    UnableToGenerateM3TokenException,
    DatameshConfigurationExceptions,
    DA2MetaDataNotFound,
    DATMetaDataNotFound,
)
from .s3_exceptions import S3BucketNotFound, S3BucketAccessDenied, S3ObjectNotFound
from .unsupported_format import (
    UnsupportedDataSourceException,
    UnsupportedFormatException,
    ColumnLimitExceededException,
    ConnectionNotFound,
    M3ProgramNotFound,
    PythonLibraryConfigFileNotFound,
    EC2InstanceNotFoundException,
    ConnectionNotFoundException,
    ColumnsNotFoundException,
)
from flask import request, jsonify
from ..utils import logger
class DataMeshExceptionHandler:
    """Order of below conditions is important, please do not change it"""
    @classmethod
    def parse_message(cls, err: Exception, return_err: bool = False):
        message = None
        logger.exception(f"Error occurred:{str(err)}")
        if isinstance(err, (S3BucketAccessDenied, S3BucketNotFound)):
            message = str(err)
        elif isinstance(
            err,
            (
                ExistingMongoConnection,
                ExistingKafkaConnection,
                UnsupportedDataSourceException,
                EnvironmentConfigMissing,
                IonAPIFetchException,
                UnsupportedFormatException,
                ColumnLimitExceededException,
                DelimiterUpdateFailedException,
                M3ProgramMetadataRetrievalFailure,
                ColumnsNotFoundException,
                S3ObjectNotFound,
                MetadataFetchFailedException,
                DatabaseOrDataStoreDetailsRetrievalException,
                SaveMetadataFailedException,
                EnvironmentNameMissing,
                ConnectionNotFound,
                M3ProgramNotFound,
                EC2InstanceNotFoundException,
                PythonLibraryConfigFileNotFound,
                ConnectionNotFoundException,
                UnableToGenerateM3TokenException,
                UnableToFetchUserDetailsException,
                DatameshConfigurationExceptions,
                DA2MetaDataNotFound,
                DATMetaDataNotFound,
            ),
        ):
            message = str(err)
        elif isinstance(err, KeyError):
            message = f"{str(err)} is missing"
        elif return_err:
            return err
        else:
            message = str(err)
        return message
    @classmethod
    def handle_cors(cls, response):
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "authorization,content-type,x-Org-Id,x-Project-Id,x-Username,x-Service-Token",
        )
        response.headers.add(
            "Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS"
        )
        response.headers.add("Access-Control-Allow-Credentials", "true")
        if request.method == "OPTIONS":
            response.status = 200
        return response
    @classmethod
    def handle_exception(cls, e):
        if hasattr(e, "original_exception"):
            message = DataMeshExceptionHandler.parse_message(e.original_exception)
        else:
            message = DataMeshExceptionHandler.parse_message(e)
        if message:
            data = {
                "message": message,
                "success": False,
                "payload": None,
            }
New code
            return jsonify(data), 400
Uncovered code
        return jsonify({}), 500