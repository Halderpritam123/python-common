from ..dataclasses import UserDetails, S3Table, MultiTableDetails
from ..enums import DataFormats
from ..storage.mongo import MongoDBConnector
from ..utils import helpers
from ..utils.apicaller import ApiCaller
from ..config_loader import get_config
from ..utils import logger
from ..exceptions.exception_handler import DataMeshExceptionHandler
from ..exceptions.custom_exceptions import (
    MetadataFetchFailedException,
    SaveMetadataFailedException,
)
from ..exceptions.unsupported_format import (
    UnsupportedFormatException,
    UnsupportedDataSourceException,
)
from ..enums import SourceTargetTypes
def get_table_metadata(table_info: S3Table, user_info: UserDetails):
    """
    Check if metadata for a table exists in the database. If found, return the existing metadata.
    If not present, extract (API call to metadata extract service) the metadata from the table using the first 2000 rows,
    save it in the db and return it.
    Args:
        table_info (S3Table): Details of the source table, including connectorId, tablename, and bucket.
        user_info (UserDetails): User details, including orgId and projected.
    Returns:
        dict: Metadata found for the table, or None if it is not present.
    """
    try:
        logger.debug(
            f"Started fetching metadata for {table_info.table_name}.", table_info.job_id
        )
        result = None
        if table_info.source_type != SourceTargetTypes.S3:
            raise UnsupportedDataSourceException(table_info.source_type)
        if table_info.table_name.lower().endswith(DataFormats.CSV):
            result = __get_table_metadata_from_db(table_info, user_info)
            if not result:
                result = __extract_metadata_from_table(table_info, user_info)
                if result:
                    __save_table_metadata(table_info, user_info, result)
            else:
                return result[0]
            logger.debug(
                f"Successfully fetched the metadata for {table_info.table_name}.",
                table_info.job_id,
            )
            return result
        else:
            raise UnsupportedFormatException(table_info.table_name)
    except Exception as err:
        message = DataMeshExceptionHandler.parse_message(err)
        logger.error(f"Failed with the error: {message}", table_info.job_id)
        raise MetadataFetchFailedException(table_info.table_name)
def __get_table_metadata_from_db(table_info: S3Table, user_info: UserDetails):
    """
    Check if metadata for a table exists in the database. If found, return the existing metadata.
    Args:
        table_info (S3Table): Details of the source table, including connectorId, tablename, and bucket.
        user_info (UserDetails): User details, including orgId and projected.
    Returns:
        List: Metadata found in the table or extracted metadata.
    """
    try:
        logger.debug("Started fetching metadata from db.", table_info.job_id)
        mongo_client = MongoDBConnector.get_instance()
        db = mongo_client[get_config("DB_NAME_DATA_SOURCES_CSV_METADATA")]
        collection = db[get_config("COL_NAME_DATA_SOURCES_S3_OBJECTS_METADATA")]
        match_filter = {
            "bucket": table_info.database_name,
            "projectId": user_info.project_id,
            "orgId": user_info.org_id,
            "connectorId": table_info.connector_id,
        }
        if isinstance(table_info.table_name, str):
            match_filter["object"] = table_info.table_name
        else:
            match_filter["object"] = {"$in": table_info.table_name}
        pipeline = [
            {"$match": match_filter},
            {
                "$project": {
                    "_id": 0,
                    "db": "$bucket",
                    "connectorId": 1,
                    "dataFormat": 1,
                    "table": "$object",
                    "extractionId": 1,
                    "tableMetadata": "$objectMetadata.schema",
                }
            },
        ]
        result = list(collection.aggregate(pipeline))
        if result:
            logger.debug("Metadata found in the database.", table_info.job_id)
            return result
        else:
            logger.debug("No metadata found in the database.", table_info.job_id)
            return None
    except Exception as err:
        message = DataMeshExceptionHandler.parse_message(err)
        logger.error(f"Failed with the error: {message}", table_info.job_id)
        raise err
def __extract_metadata_from_table(table_info: S3Table, user_info: UserDetails):
    """
    Extract metadata from the table using the first 2000 rows via the extract API and return it.
    Args:
        table_info (S3Table): Details of the source table, including connectorId, tablename, and bucket.
        user_info (UserDetails): User details, including orgId and projected.
    Returns:
        dict or None: Extracted metadata from the table, or None if the API fails.
    """
    try:
        logger.debug(
            f"Started extracting metadata for the file {table_info.table_name}.",
            table_info.job_id,
        )
        auth_headers = {
            "Authorization": user_info.token,
            "x-Org-Id": user_info.org_id,
            "x-Project-Id": user_info.project_id,
            "Content-Type": "application/json",
        }
        data = {
            "connectorId": table_info.connector_id,
            "bucket": table_info.database_name,
            "object": table_info.table_name,
        }
        url = get_config("ALBANERO_BASE_ROUTE_URI") + "/metadata-spark/extract"
        response = ApiCaller.post(url=url, data=data, headers=auth_headers)
        if response.status_code == 200:
            response_data = response.json()
            logger.debug("Metadata extracted successfully.", table_info.job_id)
            return response_data["payload"]
        else:
            logger.debug("Metadata extraction failed.", table_info.job_id)
            return None
    except Exception as err:
        logger.error(
            f"Metadata extraction failed {table_info.table_name} with error: {err}",
            table_info.job_id,
        )
        raise err
def __save_table_metadata(
    table_info: S3Table, user_info: UserDetails, table_metadata: dict
):
    """
    Save the extracted metadata to the database using the specified filename.
    Args:
        table_info (S3Table): Details of the source table, including connectorId, tablename, and bucket or databaseName.
        user_info (UserDetails): User details, including orgId and projected.
    """
    try:
        logger.debug(
            f"Started saving the metadata for {table_info.table_name}.",
            table_info.job_id,
        )
        mongo_client = MongoDBConnector.get_instance()
        db = mongo_client[get_config("DB_NAME_DATA_SOURCES_CSV_METADATA")]
        collection = db[get_config("COL_NAME_DATA_SOURCES_S3_OBJECTS_METADATA")]
        filter_by = {
            "connectorId": table_info.connector_id,
            "object": table_info.table_name,
            "bucket": table_info.database_name,
            "projectId": user_info.project_id,
            "orgId": user_info.org_id,
        }
        update = {
            "$set": {
                "extractionId": helpers.uuid4_str(),
                "dataFormat": DataFormats.CSV,
                "objectMetadata.schema": table_metadata,
            }
        }
        collection.update_one(filter_by, update, upsert=True)
        logger.debug("Metadata saved successfully.", table_info.job_id)
    except Exception as err:
        message = DataMeshExceptionHandler.parse_message(err)
        logger.error(
            f"Failed to save the metadata with the error: {message}", table_info.job_id
        )
        raise SaveMetadataFailedException(table_info.table_name)
def get_multi_table_metadata(
    table_info: MultiTableDetails, user_info: UserDetails
) -> dict:
    """
    Check if metadata for a table exists in the database. If found, return the existing metadata.
    If not present, extract (API call to metadata extract service) the metadata from the table using the first 2000 rows,
    save it in the db and return it.
    Args:
        table_info (MultiTableDetails): Details of the multiple source table, including connectorId, name, and bucket.
        user_info (UserDetails): User details, including orgId and projected.
    Returns:
        list: Metadata found for the multiple tables, or empty list if it is not present.
    """
    try:
        if table_info.source_type != SourceTargetTypes.S3:
            raise UnsupportedDataSourceException(table_info.source_type)
        for table_name in table_info.table_name:
            if not table_name.lower().endswith(DataFormats.CSV):
                raise UnsupportedFormatException(table_name)
        resultant_data = __get_table_metadata_from_db(table_info, user_info)
        if len(table_info.table_name) != len(resultant_data):
            metadata_table_names = [table_info["table"] for table_info in resultant_data]
            for table in table_info.table_name:
                if table not in metadata_table_names:
                    table_details = {
                        "connectorId": table_info.connector_id,
                        "tableName": table,
                        "databaseName": table_info.database_name,
                        "sourceType": table_info.source_type,
                        "region": table_info.region,
                    }
                    tem_table_info = S3Table.from_dict(table_details)
                    result = __extract_metadata_from_table(tem_table_info, user_info)
                    if result:
                        __save_table_metadata(table_info, user_info, result)
                        resultant_data.append(result)
                logger.debug(
                    f"Successfully fetched the metadata for {table_info.table_name}.",
                    table_info.job_id,
                )
        return resultant_data
Uncovered code
    except Exception as err:
        message = DataMeshExceptionHandler.parse_message(err)
        logger.exception(f"Failed with the error: {message}", table_info.job_id)
        raise MetadataFetchFailedException(table_info.table_name)