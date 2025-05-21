import json
import logging
import requests
import os
from .exceptions.unsupported_format import PythonLibraryConfigFileNotFound
from .exceptions.exception_handler import DataMeshExceptionHandler
from .utils import logger, secrets_manager
from .enums import Environments
from .exceptions.custom_exceptions import DatameshConfigurationExceptions
from .exceptions.custom_exceptions import ServiceTokenException
config_vars = None
log_level_dict = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}
def get_environment_config(config_file_path: str, dev_mode: bool):
    """
    This method is responsible for extracting all the envs or config that we are using in the package from AWS Secrets Manager
    Args:
        config_file_path (str): Secret name in AWS Secrets Manager
        dev_mode (bool): This is for development purpose, if the developer installed this package on his local machine, then it will be true, else it will be false
    Raises:
        FileNotFoundError: File is not found in the s3
    """
    global config_vars  # Use the global keyword to indicate you're working with the global variable
    try:
        logger.debug(f"Started fetching the configs from {config_file_path}")
        if dev_mode:
            # Reading the file locally
            with open(config_file_path, "r") as json_file:
                config_vars = json.load(json_file)
        else:
            # Reading the configs from AWS secrets manager
            config_vars = secrets_manager.get_secret_by_name(config_file_path)
        logger.debug(
            f"Successfully fetched the configs from {config_file_path}: {config_vars}"
        )
        logger.info(f"Successfully fetched the configs from {config_file_path}")
        default_log_level = os.environ.get("DEFAULT_LOG_LEVEL", None)
        if not default_log_level:
            default_log_level = config_vars.get("ALBANERO_DEFAULT_LOG_LEVEL", "info")
        default_log_level = log_level_dict.get(default_log_level)
        if default_log_level:
            logging.getLogger().setLevel(default_log_level)
        if not config_vars:
            raise PythonLibraryConfigFileNotFound(config_file_path)
    except Exception as e:
        message = DataMeshExceptionHandler.parse_message(e)
        logger.error(f"Exception occurred while fetching connection configs: {message}")
        raise DatameshConfigurationExceptions(
            f"Error parsing JSON {config_file_path}: {e}"
        )
def get_config(key_name) -> str:
    try:
        config_value = config_vars[key_name]
        return config_value
    except Exception as e:
        message = DataMeshExceptionHandler.parse_message(e)
        logger.error(message)
        raise e
def set_config(key_name, value) -> None:
    try:
        if key_name in config_vars:
            logger.warning(
                f"The {key_name} key already exists in the configs, so setting it up will overwrite its current value."
            )
        config_vars[key_name] = value
    except Exception as e:
        message = DataMeshExceptionHandler.parse_message(e)
        logger.error(message)
        raise e
def get_service_token(service_name: str = None) -> None:
    """
    Fetching the current service service token from IAM.
    Raises:
        ServiceTokenException: Failed to fetch the service token.
    Returns:
        _type_: None
    """
    api_url = f"{get_config('ALBANERO_BASE_ROUTE_URI')}/iam/v2/authentication/"
    payload = {
        "nameSpace": get_config("PLATFORM_NAME_SPACE"),
        "applicationName": (
            service_name
            if service_name
            else os.environ.get("PLATFORM_SERVICE_NAME", None)
        ),
        "applicationSecret": get_config("SERVICE_SECRET_KEY"),
    }
    auth_headers = {"Content-Type": "application/json"}
    response = requests.post(url=api_url, data=json.dumps(payload), headers=auth_headers)
    if response.status_code == 200:
        response_data = response.json()
        service_token = response_data["token"]
        set_config("SERVICE_TOKEN", service_token)
        logger.debug("Exiting service token generation function.")
    else:
        message = f"Failed to fetch the service token: {response.text}"
        raise ServiceTokenException(message)
def get_datamesh_configurations(env_name: str, service_name: str = None):
    """
    Retrieves DataMesh configurations based on the provided parameters.
    This method retrieves the "env_name" from the EC2 instance and
    then proceeds to extract the configuration from an S3 bucket only if the "env_name" is available.
    If the "env_name" is not present, it will instead fetch the configuration from the local file "python_library_config.json.
    Args:
        cluster_mode (bool, optional): A flag indicating whether the function is running in cluster mode. Defaults to False.
        env_name (str, optional): The environment name. Defaults to None.
    Raises:
        Exception: Raised if the provided environment name is invalid or if the function fails to retrieve the environment name.
        Exception: Raised if an error occurs during the retrieval of configuration from S3.
        err: Other exceptions that might occur during the function execution.
    """
    try:
        logger.debug(
            "Started fetching the configs from S3 and executing the get_datamesh_configurations function."
        )
        service_secret_key = ""
        if not env_name:
            raise DatameshConfigurationExceptions("Invalid environment name.")
        elif env_name.lower() == Environments.DEVELOPMENT:
            config_file = "config.json"
            get_environment_config(config_file, True)
            service_secret_key = get_config("SERVICE_SECRET_KEY")
        else:
            config_file = f"{env_name}/python-config"
            logger.info(f"Config file path: {config_file}")
            get_environment_config(config_file, False)
            os.environ["PLATFORM_ENVIRONMENT_NAME"] = env_name
            if not service_name:
                service_name = os.environ.get("PLATFORM_SERVICE_NAME")
            # Fetching the service secret key
            secret_file_path = f"{env_name}/iam/v2/application/{service_name}"
            logger.info(f"Fetching service token from {secret_file_path}.")
            config_vars = secrets_manager.get_secret_by_name(secret_file_path)
            service_secret_key = config_vars["secret"]
            set_config("SERVICE_SECRET_KEY", service_secret_key)
        get_service_token(service_name)
        logger.debug(
            "Successfully fetched the configs and exiting the get_datamesh_configurations function."
        )
    except Exception as err:
        # Handle specific exceptions or log generic exception details
Uncovered code
        message = DataMeshExceptionHandler.parse_message(err)
        logger.error(f"Data mesh configs retrieval form s3 failed with error: {message}")
        raise err