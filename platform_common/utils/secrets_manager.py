import boto3
import json
import os
def create_secret(secret_name: str, secret_value: dict) -> None:
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-west-2"),
    )
    secret_string = json.dumps(secret_value)
    client.create_secret(Name=secret_name, SecretString=secret_string)
def update_secret(secret_name: str, secret_value: dict) -> None:
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-west-2"),
    )
    secret_string = json.dumps(secret_value)
    client.update_secret(SecretId=secret_name, SecretString=secret_string)
def delete_secret(secret_name: str) -> None:
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-west-2"),
    )
    client.delete_secret(SecretId=secret_name, RecoveryWindowInDays=7)
def get_secret_by_name(secret_name: str) -> dict:
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-west-2"),
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret_string = get_secret_value_response["SecretString"]
    secret_dict = json.loads(secret_string)
    return secret_dict
def secret_name_for_credentials(connector_id: str, user_details) -> str:
    platform_env = os.environ.get("PLATFORM_ENVIRONMENT_NAME")
    if not platform_env:
        raise ValueError(
            "Could not get the value of `PLATFORM_ENVIRONMENT_NAME`. Please make sure you are running this code on an instance where environment name is configured or make sure to call `get_datamesh_configurations` with an environment name."
        )
    return f"client/{platform_env}/{user_details.org_id}/{user_details.project_id}/connected-sources/{connector_id}"
def store_connection_credentials(
    connector_id: str, credentials: dict, user_details
) -> None:
    secret_name = secret_name_for_credentials(connector_id, user_details)
    create_secret(secret_name, credentials)
def update_connection_credentials(
    connector_id: str, credentials: dict, user_details
) -> None:
    secret_name = secret_name_for_credentials(connector_id, user_details)
    update_secret(secret_name, credentials)
def get_connection_credentials(connector_id: str, user_details) -> dict:
    secret_name = secret_name_for_credentials(connector_id, user_details)
    return get_secret_by_name(secret_name)
Uncovered code
def delete_connection_credentials(connector_id: str, user_details) -> None:
    secret_name = secret_name_for_credentials(connector_id, user_details)
    delete_secret(secret_name)