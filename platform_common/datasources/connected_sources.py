from platform_common.enums import Versions
from multiprocessing import connection
from ..exceptions.custom_exceptions import (
    DatabaseOrDataStoreDetailsRetrievalException,
    DeltaLakeConnectionDetailsRetrievalException,
)
from ..storage.mongo import MongoDBConnector
from ..exceptions.unsupported_format import (
    UnsupportedDataSourceException,
    ConnectionNotFoundException,
)
from ..utils import logger, secrets_manager
from ..dataclasses import UserDetails, ConnectionConfig, DatabaseConnectionConfig
from ..config_loader import get_config
from ..exceptions.exception_handler import DataMeshExceptionHandler
from ..utils.apicaller import ApiCaller
from platform_common.enums import SourceTargetTypes
from typing import Optional
DATASTORES = [SourceTargetTypes.S3]
DATABASES = [
    SourceTargetTypes.MS_SQL,
    SourceTargetTypes.IBM_DB2,
    SourceTargetTypes.ORACLE,
    SourceTargetTypes.SNOWFLAKE,
]
LAKEHOUSE = [SourceTargetTypes.DELTALAKE]
def __get_datastore(
    connector_id: str, user_details: UserDetails, isv2: bool = False, options: dict = {}
):
    """
    Retrieves information about the active datastore connection, including details like SecretKey, AccessId, connection name, etc.
    Args:
        connector_id (str): A unique identifier for each connection, known as ConnectorId.
        user_details (UserDetails): User-related information, which may include orgId and other relevant details.
        isv2 (bool): Flag indicating whether the V2 version of details should be retrieved.
    Returns:
        dict or None: If the connection is found in the db, the function returns a dictionary containing them;
        otherwise, it returns None.
    """
    try:
        logger.debug("Started fetching the datastore details.")
        mongo_client = MongoDBConnector.get_instance()
        db = mongo_client[get_config("DB_NAME_DATA_SOURCES")]
        collection = db[get_config("COL_NAME_DATA_SOURCES_DATASTORE_DETAILS")]
        project = {"_id": 0, "orgId": 0, "projectId": 0, "isDeleted": 0}
        if isv2:
            project = {
                "_id": 0,
                "password": 0,
                "accessKeyId": 0,
                "secretAccessKey": 0,
                "authFile": 0,
                "isDeleted": 0,
            }
        filter_by = {
            "orgId": user_details.org_id,
            "projectId": user_details.project_id,
            "$or": [{"isDeleted": False}, {"isDeleted": {"$exists": False}}],
            "connectorId": connector_id,
        }
        result = collection.find_one(filter_by, projection=project)
        logger.debug(f"Successfully fetched the datastore details:{result}")
        return result
    except Exception as err:
        message = DataMeshExceptionHandler.parse_message(err)
        logger.error(f"Failed with the error: {message}")
        raise DatabaseOrDataStoreDetailsRetrievalException(message)
def __get_database(
    connector_id: str, user_details: UserDetails, isv2: bool = False, options: dict = {}
):
    """
    Retrieves information about the active connection, including details like SecretKey, AccessId, connection name, etc.
    Args:
        connector_id (str): A unique identifier for each connection, known as ConnectorId.
        user_details (UserDetails): User-related information, which may include orgId and other relevant details.
        isv2 (bool): Flag indicating whether the V2 version of details should be retrieved.
    Returns:
        dict or None: If the connection is found in the db, the function returns a dictionary containing them;
        otherwise, it returns None.
    """
    try:
        logger.debug("Started fetching the database details")
        mongo_client = MongoDBConnector.get_instance()
        db = mongo_client[get_config("DB_NAME_DATA_SOURCES")]
        collection = db[get_config("COL_NAME_DATA_SOURCES_DATABASE_DETAILS")]
        project = {"_id": 0, "orgId": 0, "projectId": 0, "isDeleted": 0}
        if isv2:
            project = {"_id": 0, "password": 0, "isDeleted": 0}
        filter_by = {
            "orgId": user_details.org_id,
            "projectId": user_details.project_id,
            "$or": [{"isDeleted": False}, {"isDeleted": {"$exists": False}}],
            "connectorId": connector_id,
        }
        result = collection.find_one(filter_by, projection=project)
        logger.debug(f"Successfully fetched the database details: {result}")
        return result
    except Exception as err:
        message = DataMeshExceptionHandler.parse_message(err)
        logger.error(f"Failed with the error: {message}")
        raise DatabaseOrDataStoreDetailsRetrievalException(message)
def __get_lakehouse(connector_id: str, user_details: UserDetails, options: dict = {}):
    try:
        logger.debug("Started fetching the delta lake connection details")
        payload = {"connector_id": connector_id}
        api_version = options.get("apiVersion", "v2")
        headers = {
            "x-org-id": user_details.org_id,
            "x-project-id": user_details.project_id,
            "Authorization": user_details.token,
            "Content-Type": "application/json",
        }
        if api_version == Versions.V1:
            headers.pop("Authorization")
            headers["X-Username"] = user_details.username
        url = f"{get_config('DELTALAKE_SERVICE_URI')}/system-db/api/{api_version}/connectors/get-connector"
        response = ApiCaller.post(url, data=payload, headers=headers)
        logger.info(f"delta lake connection details api response:{response.text}")
        if response.status_code == 200 and response.json().get("payload"):
            response_data = response.json()
            logger.debug("Successfully extracted the deltalake connection details.")
            connector = response_data["payload"]
            connector["accountName"] = connector["awsAccountName"]
            return connector
        else:
            logger.error(
                f"Failed to fetch metadata for connector {connector_id}. "
                f"Status Code: {response.status_code}, Response: {response.text}"
            )
            response.raise_for_status()  # Raise an HTTPError for non-200 responses
    except Exception as e:
        message = DataMeshExceptionHandler.parse_message(e)
        logger.error(
            f"An error occurred while fetching the delta lake connection details: {message}"
        )
        raise DeltaLakeConnectionDetailsRetrievalException(message)
    return None
def get_connection_config(
    source_type: str, connector_id: str, user_details: UserDetails, options: dict = {}
):
    """
    This function retrieves information about an active connection, specifically details such as the SecretKey, AccessId,
    and connection name, but only if the source type is one of the supported databases (DATASTORES).
    Args:
        source_type (str): Represents the source type
        connector_id (str): A unique identifier for each connection, referred to as ConnectorId.
        user_details (UserDetails): User-related information, including orgId and other pertinent details.
        options (dict): Additional options that can be passed to the function.
    Returns:
        dict or None: If the connection is found in the db, the function returns a dictionary containing them;
        otherwise, it returns None.
    """
    logger.debug(
        f"Retrieval of the {source_type} connection configuration has been initiated.",
    )
    connector: Optional[dict] = None
    if source_type in DATASTORES:
        connector = __get_datastore(connector_id, user_details, options)
    elif source_type in DATABASES:
        connector = __get_database(connector_id, user_details, options)
    elif source_type in LAKEHOUSE:
        connector = __get_lakehouse(connector_id, user_details, options)
    else:
        logger.debug(f"Unsupported source type :{source_type}")
        raise UnsupportedDataSourceException(source_type)
    if not connector:
        logger.debug(f"No {source_type} connection found")
        raise ConnectionNotFoundException()
    if source_type not in LAKEHOUSE:
        credentials = secrets_manager.get_connection_credentials(
            connector_id, user_details
        )
        connector.update(credentials)
    logger.info("Successfully fetched the connection config details.")
    db_type = connector.pop("dbType", None)
    if db_type in [
        SourceTargetTypes.SNOWFLAKE,
        SourceTargetTypes.MS_SQL,
        SourceTargetTypes.IBM_DB2,
        SourceTargetTypes.ORACLE,
    ]:
        config = DatabaseConnectionConfig.from_dict(connector)
    else:
        config = ConnectionConfig.from_dict(connector)
    return config
