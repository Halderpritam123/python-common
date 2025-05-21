from ..storage.mongo import MongoDBConnector
from ..utils.helpers import current_time_ms
from ..config_loader import get_config
from ..dataclasses import S3Table, UserDetails, CSVDelimiters, DA2Delimiters, DATDelimiters
from ..utils import logger
from ..enums import DataFormats
class DelimitersConfig:
    @staticmethod
    def get_delimiters(source_details: S3Table, user_details: UserDetails):
        """
        Check for the presence of delimiters in the database. If no delimiters exist,
        the function returns the default delimiters.
        Default delimiters - delimiters that exist at the bucket level.
        Args:
            source_details (dict): A dictionary containing details of the source file, including connectorId, bucket, and filePath.
            user_details (dict): A dictionary containing details of the user, including orgId and projected.
        Returns:
            dict: A dict containing delimiters if found in the database, or the default delimiters.
        """
        logger.debug(
            f"Started fetching delimiters for {source_details.table_name}.",
            source_details.job_id,
        )
        if not source_details.table_name.lower().endswith((".csv", ".da2", ".dat")):
            logger.debug(
                f"The source file '{source_details.table_name}' is not a valid file format. Returning None.",
                source_details.job_id,
            )
            return None
        result = DelimitersConfig.__get_delimiters_from_db(source_details, user_details)
        logger.debug(
            f"Delimiters obtained from database: {result}", source_details.job_id
        )
        if not result and source_details.table_name.lower().endswith(DataFormats.DA2.value):
            result = DA2Delimiters.get_default(source_details.table_name).to_dict()
        elif not result and source_details.table_name.lower().endswith(DataFormats.DAT.value):
            result = DATDelimiters.get_default(source_details.table_name).to_dict()
        if not result:
            logger.debug(
                "No delimiters found in the database. Attempting to get bucket-level delimiters.",
                source_details.job_id,
            )
            result = DelimitersConfig.__get_bucket_level_delimiters(
                source_details, user_details
            )
            logger.debug(
                f"Bucket-level delimiters obtained: {result}", source_details.job_id
            )
        if not result:
            logger.debug(
                "No delimiters found. Using default delimiters.", source_details.job_id
            )
            result = CSVDelimiters.get_default().to_dict()
        logger.debug(f"Final result: {result}", source_details.job_id)
        return result
    @staticmethod
    def __get_delimiters_from_db(source_details: S3Table, user_details: UserDetails):
        """
        Retrieve delimiters from the database if they are present.
        Args:
            source_details (dict): A dictionary containing details of the source file, including connectorId, bucket, and filePath.
            user_details (dict): A dictionary containing details of the user, including orgId and projected.
        Returns:
            dict or None: A dictionary containing delimiters if found in the database, or None if not found.
        """
        logger.debug("Started fetching delimiters from db.", source_details.job_id)
        results = None
        mongo_client = MongoDBConnector.get_instance()
        db = mongo_client[get_config("DB_NAME_DATA_SOURCES")]
        collection = db[get_config("COL_NAME_DATA_SOURCES_CSV_DELIMITERS")]
        filter_by = {
            "orgId": user_details.org_id,
            "projectId": user_details.project_id,
            "connectorId": source_details.connector_id,
            "bucketName": source_details.database_name,
            "objectName": source_details.table_name,
        }
        projection = {
            "_id": 0,
            "fieldDelimiter": 1,
            "quoteCharacter": 1,
            "quoteEscapeCharacter": 1,
            "programName": 1,
            "recordDelimiter": 1,
            "encoding": 1,
            "targetSystem": 1,
        }
        cursor = collection.find(filter_by, projection)
        results = list(cursor)
        if not len(results):
            logger.debug("No delimiters found in the database.", source_details.job_id)
            return None
        logger.debug(
            f"Delimiters found in the database: {results[0]}", source_details.job_id
        )
        return results[0]
    @staticmethod
    def __get_bucket_level_delimiters(source_details: S3Table, user_details: UserDetails):
        """
        Retrieve delimiters from the database at the bucket level if they exist.
        Args:
            source_details (dict): Details of the source file, including connectorId, bucket, and filePath.
            user_details (dict): User details, including orgId and projected.
        Returns:
            dict or None: Delimiters found in the database or None if not found.
        """
        logger.debug(
            "Started fetching default bucket level delimiters from db.",
            source_details.job_id,
        )
        mongo_client = MongoDBConnector.get_instance()
        db = mongo_client[get_config("DB_NAME_DATA_SOURCES")]
        collection = db[get_config("COL_NAME_DATA_SOURCES_DEFAULT_BUCKET_DELIMITERS")]
        filter_by = {
            "orgId": user_details.org_id,
            "projectId": user_details.project_id,
            "connectorId": source_details.connector_id,
            "bucketName": source_details.database_name,
        }
        projection = {
            "_id": 0,
            "fieldDelimiter": 1,
            "quoteCharacter": 1,
            "quoteEscapeCharacter": 1,
            "recordDelimiter": 1,
            "encoding": 1,
        }
        cursor = collection.find(filter_by, projection)
        results = list(cursor)
        if not len(results):
            logger.debug(
                "No bucket-level delimiters found in the database.",
                source_details.job_id,
            )
            return None
        logger.debug(
            f"Bucket-level delimiters found in the database: {results[0]}",
            source_details.job_id,
        )
        return results[0]
    @staticmethod
    def set_or_update_delimiters(
        source_details: S3Table, delimiters: dict, user_details: UserDetails
    ):
        """
        Update or set delimiters for a specific file in the database.
        Args:
            source_details (dict): Details of the source file, including connectorId, bucket, and filePath.
            user_details (dict): User details, including orgId. projectId, userId and username.
        """
        logger.debug(
            f"Started setting or updating the delimiters for {source_details.table_name}.",
            source_details.job_id,
        )
        mongo_client = MongoDBConnector.get_instance()
        db = mongo_client[get_config("DB_NAME_DATA_SOURCES")]
        collection = db[get_config("COL_NAME_DATA_SOURCES_CSV_DELIMITERS")]
        filter_by = {
            "orgId": user_details.org_id,
            "projectId": user_details.project_id,
            "connectorId": source_details.connector_id,
            "bucketName": source_details.database_name,
            "objectName": source_details.table_name,
        }
        update = {
            "$set": {
                "fieldDelimiter": delimiters["fieldDelimiter"],
                "quoteCharacter": delimiters["quoteCharacter"],
                "quoteEscapeCharacter": delimiters["quoteEscapeCharacter"],
                "recordDelimiter": delimiters["recordDelimiter"],
                "encoding": delimiters["encoding"],
                "updateAt": current_time_ms(),
                "programName": delimiters.get("programName"),
                "targetSystem": delimiters.get("targetSystem"),
                "updateBy": {
                    "userId": user_details.user_id,
                    "username": user_details.username,
                },
            }
        }
Uncovered code
        collection.update_one(filter_by, update, upsert=True)
        logger.info("Delimiters are updated successfully.", source_details.job_id)