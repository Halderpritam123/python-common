from .config_loader import (
    get_environment_config,
    get_service_token,
    get_config,
    set_config,
)
from platform_common.environment_name_provider import EnvironmentNameProvider
from platform_common.exceptions.exception_handler import DataMeshExceptionHandler
from .utils import logger
import os
from .enums import Environments
from .exceptions.custom_exceptions import DatameshConfigurationExceptions
from .utils import secrets_manager
logger.setup_logger()
os.environ["PYTHONUNBUFFERED"] = "1"
def get_datamesh_configurations():
    """
    This method retrieves the "env_name" from the EC2 instance and
    then proceeds to extract the configuration from an S3 bucket only if the "env_name" is available.
    If the "env_name" is not present, it will instead fetch the configuration from the local file "python_library_config.json."
    In DEVELOPMENT mode, the service token should be included in the config.json file, while in PRODUCTION mode,
    the service token will be retrieved from the secrets manager.
    """
    try:
        logger.debug(
            "Started fetching the configs from s3 and started get_datamesh_configurations function."
        )
        service_secret_key = ""
        env_mode = os.environ.get("ALBANERO_SERVICE_ENVIRONMENT", None)
        env = EnvironmentNameProvider()
        if env_mode and env_mode.lower() == Environments.DEVELOPMENT:
            config_file = "config.json"
            get_environment_config(config_file, True)
            get_service_token()
        elif env_mode and env_mode.lower() == Environments.PRODUCTION:
            env_name = env.get_environment()
            if env_name:
                config_file = f"{env_name}/python-config"
                get_environment_config(config_file, False)
                os.environ["PLATFORM_ENVIRONMENT_NAME"] = env_name
                # Fetching the service secret key
                service_name = os.environ.get("PLATFORM_SERVICE_NAME")
                secret_file_path = f"{env_name}/iam/v2/application/{service_name}"
                config_vars = secrets_manager.get_secret_by_name(secret_file_path)
                service_secret_key = config_vars["secret"]
                set_config("SERVICE_SECRET_KEY", service_secret_key)
                get_service_token()
            else:
                raise DatameshConfigurationExceptions(
                    "Failed to retrieve the environment name"
                )
        else:
            if env_mode:
                message = "The value being passed to the ALBANERO_SERVICE_ENVIRONMENT environment variable is invalid."
                logger.error(message)
                raise DatameshConfigurationExceptions("Invalid environment name")
            else:
                message = "Since ALBANERO_SERVICE_ENVIRONMENT is None, please ensure to call get_datamesh_configurations() method explicitly at the root level of your service."
                logger.warning(message)
        logger.debug(
            "successfully fetched the configs and exiting get_datamesh_configurations function."
        )
    except Exception as err:
        message = DataMeshExceptionHandler.parse_message(err)
        logger.error(f"Retrieval of configs from s3 failed with error: {message} ")
        raise err
Uncovered code
get_datamesh_configurations()