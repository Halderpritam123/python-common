from ..dataclasses import UserDetails
from ..storage.mongo import MongoDBConnector
from ..exceptions.exception_handler import DataMeshExceptionHandler
from ..exceptions.custom_exceptions import (
    IonAPIFetchException,
    M3ProgramMetadataRetrievalFailure,
    UnableToGenerateM3TokenException,
)
from ..exceptions.unsupported_format import (
    ConnectionNotFound,
    M3ProgramNotFound,
    UnsupportedDataSourceException,
)
from ..config_loader import get_config
from ..utils import logger, secrets_manager
from ..utils.apicaller import ApiCaller
from ..enums import SourceTargetTypes
import base64
def get_m3_datalake_config(
    source_type: str, connector_id: str, user_info: UserDetails
) -> dict:
    """
    This function retrieves details of the active M3 connection, including all Ion API information in JSON format.
    Args:
        connector_id (str): A unique identifier for each connection, referred to as ConnectorId.
        user_details (UserDetails): User-related information, which may include orgId and other pertinent details.
    Returns:
        dict or None: If the connection exists in the database, the function returns a dictionary containing the details; otherwise, it returns None.
    """
    try:
        logger.debug(
            f"[{connector_id}]: Retrieval of the m3 connection configuration has been initiated.",
        )
        if source_type != SourceTargetTypes.INFOR_DATALAKE:
            logger.debug(f"Unsupported source type :{source_type}")
            raise UnsupportedDataSourceException(source_type)
        mongo_client = MongoDBConnector.get_instance()
        db = mongo_client[get_config("DB_NAME_DATA_SOURCES")]
        collection = db[get_config("COL_NAME_DATA_SOURCES_M3_DATALAKE_DETAILS")]
        result = collection.find_one(
            {
                "projectId": user_info.project_id,
                "orgId": user_info.org_id,
                "connectorId": connector_id,
            },
            {"_id": 0},
        )
        if result:
            logger.debug(f"Configuration details retrieved: {result}")
            logger.debug(
                f"[{connector_id}]: Successfully retrieved the M3 datalake connection conifg(ion api data)"
            )
            credentials = secrets_manager.get_connection_credentials(
                connector_id, user_info
            )
            result.update(credentials)
            return result
        else:
            raise ConnectionNotFound(connector_id)
    except Exception as e:
        message = DataMeshExceptionHandler.parse_message(e)
        logger.error(f"[{connector_id}]: Failed with the error: {message}")
        raise IonAPIFetchException(message)
def get_m3_datalake_program_metadata(program_name: str) -> dict:
    """
    This function retrieves metadata of each M3 program, including all the validation details, data types, transactions, etc.
    Args:
        program_name (str): A unique identifier for each program, referred to as program_name.
    Returns:
        dict or None: If the connection exists in the database, the function returns a dictionary containing the details; otherwise, it returns None.
    """
    try:
        logger.debug(f"Initiating the retrieval of metadata for {program_name}")
        mongo_client = MongoDBConnector.get_instance()
        db = mongo_client[get_config("DB_NAME_M3_METADATA")]
        collection = db[get_config("COL_NAME_M3_PROGRAMS_METADATA")]
        result = collection.find_one(
            {
                "programCode": program_name,
            },
            {"_id": 0},
        )
        if result:
            logger.debug(
                f" Successfully fetched the metadata for M3 program {program_name}: {result}"
            )
            return result
        else:
            raise M3ProgramNotFound(program_name)
    except Exception as e:
        message = DataMeshExceptionHandler.parse_message(e)
        logger.error(f"[{program_name}]: Failed with the error: {message}")
        raise M3ProgramMetadataRetrievalFailure(message)
def __generate_basic_authorization_header(user_name: str, password: str) -> str:
    client_id_secret = f"{user_name}:{password}"
    encoded_client_id_secret = base64.b64encode(client_id_secret.encode()).decode("utf-8")
    basic_authorization_header = f"Basic {encoded_client_id_secret}"
    return basic_authorization_header
def generate_m3_token(m3_ion_credentials: dict) -> str:
    """
     This function is useful generate the M3 token.
    Args:
        m3_ion_credentials (dict): To generate the M3 api token we are using ion credentials as a input.
    Returns:
        str :If the given credentials are valid, then function will generate and return the token.
    """
    try:
        m3_access_token_header_opt = None
        headers_to_send = {
            "Authorization": __generate_basic_authorization_header(
                m3_ion_credentials["ci"], m3_ion_credentials["cs"]
            ),
            "Token-Name": m3_ion_credentials["ti"],
            "Content-Type": "application/x-www-form-urlencoded",
        }
        body_to_send = {
            "grant_type": "password",
            "username": m3_ion_credentials["saak"],
            "password": m3_ion_credentials["sask"],
        }
        api_url = f"{get_config('M3_ION_API_TOKEN_GENERATOR_API')}/{m3_ion_credentials['ti']}/as/token.oauth2"
        response = ApiCaller.post(
            api_url,
            headers=headers_to_send,
            data=body_to_send,
        )
        oauth2_api_response = response.json()
        if response.status_code == 200:
            access_token = oauth2_api_response.get("access_token")
            m3_access_token_header_opt = (
                f"Bearer {access_token}" if access_token else None
            )
            logger.info("Generated token for the APIs.")
            return m3_access_token_header_opt
        else:
            raise UnableToGenerateM3TokenException()
Uncovered code
    except Exception as ex:
        logger.exception(f"Failed to generate token with an error: {ex}")
        raise UnableToGenerateM3TokenException()