import re
import requests
import json
from flask import request, jsonify, Request, url_for
from .exceptions.exception_handler import DataMeshExceptionHandler
from .exceptions.custom_exceptions import (
    UnableToFetchUserDetailsException,
    ServiceRegistrationFailed,
    TokenGenerationExceptions,
)
from .utils import logger
from .config_loader import get_config, set_config
from .dataclasses import UserDetails
from typing import Dict
import os
from .storage.mongo import MongoDBConnector
content_type = "application/json"
class IAM:
    @classmethod
    def register_service(cls, app):
        """Registers the service and all routes to IAM"""
        routes_info = []
        replacements = str.maketrans({"<": "{", ">": "}"})
        # Iterate over all rules in the Flask app
        for rule in app.url_map.iter_rules():
            # Filter out 'HEAD' and 'OPTIONS' methods
            endpoint = rule.endpoint
            methods = rule.methods - {"HEAD", "OPTIONS"}  # Exclude 'HEAD' and 'OPTIONS'
            # Skip routes that have no desired methods
            if methods and endpoint not in ["health", "static"]:
                view_function = app.view_functions.get(endpoint)
                routes_info.append(
                    {
                        "nameSpace": get_config("PLATFORM_NAME_SPACE"),
                        "resourceName": os.environ.get("PLATFORM_SERVICE_NAME", None),
                        "path": str(rule).translate(replacements),
                        "actionName": endpoint,
                        "method": list(methods)[0],
                        "isRegistered": True,
                    }
                )
                logger.debug(
                    f"Route: {rule}, View Function: {view_function}, Methods: {methods}"
                )
        logger.info(f"routes list:{routes_info}")
        auth_headers = {
            "X-Service-Token": get_config("SERVICE_TOKEN"),
            "Content-Type": content_type,
        }
        register_api = f"{get_config('ALBANERO_BASE_ROUTE_URI')}/iam/v2/mgmt/application/registration"
        data = {
            "application": {
                "nameSpace": get_config("PLATFORM_NAME_SPACE"),
                "resourceName": os.environ.get("PLATFORM_SERVICE_NAME", None),
                "isRegistered": True,
            },
            "actions": routes_info,
        }
        response = requests.post(
            url=register_api,
            data=json.dumps(data),
            headers=auth_headers,
        )
        if response.status_code == 200:
            logger.info("Successfully registered the service to IAM.")
        else:
            message = response.text
            logger.exception(f"message: {message}, route: {register_api}")
            raise ServiceRegistrationFailed()
    @classmethod
    def authorize(cls):
        """
        This method is responsible authorizing the user,
        It sends a request to one of the IAM APIs to authorize the user.
        If the authorization is successful, it sets the user details in the request otherwise returns the appropriate response.
        """
        try:
            if is_request_exceptional(request):
                return
            token = request.headers.get("Authorization")
            org_id = request.headers.get("X-Org-Id")
            project_id = request.headers.get("X-Project-Id")
            service_token = request.headers.get("X-Service-Token")
            if consider_request_without_auth(request):
                username = request.headers.get("X-Username", "")
                if username:
                    user_details = {
                        "username": username,
                        "userId": username,
                        "orgId": org_id,
                        "projectId": project_id,
                    }
                    request.user_details = UserDetails.from_dict(user_details)
                return
            headers = {
                "Authorization": token,
                "X-Service-Token": service_token,
                "Content-Type": content_type,
                "x-Org-Id": org_id,
                "x-Project-Id": project_id,
            }
            path = (
                request.raw_url.split("?")[0]
                if hasattr(request, "raw_url")
                else request.path
            )
            url = f"{get_config('ALBANERO_BASE_ROUTE_URI')}/iam/v2/authorization"
            data = {
                "applicationName": os.environ.get("PLATFORM_SERVICE_NAME", None),
                "requestPath": path,
                "method": request.method,
                "nameSpace": get_config("PLATFORM_NAME_SPACE"),
                "request": {
                    "contextPath": get_config("ALBANERO_BASE_ROUTE_URI"),
                    "headers": convert_headers(request),
                    "method": request.method,
                    "pathInfo": path,
                    "pathTranslated": f"/{path}",
                    "queryString": (
                        request.query_string.decode("utf-8")
                        if isinstance(request.query_string, bytes)
                        else request.query_string
                    ),
                    "requestURI": request.url_root,
                    "requestURL": request.url,
                    "servletPath": request.endpoint,
                },
                "authToken": token,
                "serviceToken": service_token,
                "orgId": org_id,
                "projectId": project_id,
            }
            response = requests.post(url, data=json.dumps(data), headers=headers)
            if response.status_code == 200:
                response_data = response.json()
                user_id = None
                for principal in response_data["requestPrincipal"]["principalList"]:
                    if principal["type"] == "User":
                        user_id = principal["id"]
                        break
                user_details = {
                    "orgId": org_id,
                    "projectId": project_id,
                    "userId": user_id,
                    "token": token,
                }
                user_details: UserDetails = UserDetails.from_dict(user_details)
                user_info = IAM.get_user_details(user_details, service_token)
                user_details.username = user_info["username"]
                user_details.email = user_info["email"]
                user_details.full_name = (
                    f"{user_info['firstName']} {user_info['lastName']}"
                )
                request.user_details = user_details
            else:
                message = response.text
                response_data = response.json()
                logger.exception(f"message: {message}, route: {url}")
                return (
                    jsonify(
                        {
                            "message": "Request failed during authorization.",
                            "success": False,
                        }
                    ),
                    response.status_code,
                )
        except Exception as e:
            message = DataMeshExceptionHandler.parse_message(e)
            logger.exception(f"Exception occurred during authorization: {message}")
            return (
                jsonify(
                    {
                        "message": "Request failed during authorization.",
                        "success": False,
                    }
                ),
                500,
            )
    @staticmethod
    def get_user_details(
        user_details: UserDetails, service_token: str = None
    ) -> Dict[str, any]:
        """
        This function is useful to get the user information based on the user id.
        Args:
            user_id (str) :  Unique identifier to identify the user.
            user_details (UserDetails): User-related information, which may include orgId and other pertinent details.
        Returns:
            dict: Gives back the user info if details are found otherwise None
        """
        x_service_token = service_token or get_config("SERVICE_TOKEN")
        headers = {
            "accept": "application/json",
            "Authorization": user_details.token,
            "X-Service-Token": x_service_token,
        }
        url = f"{get_config('ALBANERO_BASE_ROUTE_URI')}/iam/v2/mgmt/user/id/{user_details.user_id}"
        response = requests.get(url=url, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code >= 400 and response.status_code <= 500:
            raise UnableToFetchUserDetailsException(user_details.user_id)
    @staticmethod
    def generate_auth_token(token: str, id: str = None):
        """Generates a proxy access token for internal use. If a proxy_auth_token is already available,
        it uses that to generate the proxy access token. Otherwise, it first generates the proxy_auth_token and
        then uses it to generate the proxy access token.
        Args:
            token (str): authorization token
            proxy_auth_token (str): proxy authorization token
        Returns: It returns the proxy access token
        """
        try:
            mongo_client = MongoDBConnector.get_instance()
            db = mongo_client[get_config("DB_NAME_PROXY_TOKEN_DETAILS")]
            collection = db[get_config("COL_NAME_PROXY_TOKEN_DETAILS")]
            proxy_auth_token = None
            proxy_access_token = None
            if id:
                token_details = collection.find_one(
                    {"id": id},
                    {"_id": 0},
                )
                if token_details:
                    proxy_auth_token = token_details["token"]
                    data = {
                        "serviceToken": get_config("SERVICE_TOKEN"),
                        "proxyAuthorizationToken": proxy_auth_token,
                    }
                    proxy_access_token = generate_proxy_token(data)
                    return proxy_access_token
                else:
                    return generate_and_save_proxy_token(token, id, collection)
            else:
                return generate_and_save_proxy_token(token, id, collection)
        except Exception as e:
            message = DataMeshExceptionHandler.parse_message(e)
            logger.exception(f"Token generation failed. Error details: {message}")
            raise TokenGenerationExceptions(message)
    @staticmethod
    def validate_service_token() -> bool:
        """
        This function is useful to get the user information based on the user id.
        Returns:
            bool: Returns True if is valid else it returns false
        """
        url = f"{get_config('ALBANERO_BASE_ROUTE_URI')}/iam/v2/authorization/token/validation"
        headers = {"X-Service-Token": get_config("SERVICE_TOKEN")}
        response = requests.get(url, headers=headers)
        if response.status_code == 400:
            return False
        else:
            return True
def generate_proxy_token(data: dict):
    logger.debug("Started generating proxy tokens.")
    url = f"{get_config('ALBANERO_BASE_ROUTE_URI')}/iam/v2/authentication"
    headers = {
        "Content-Type": content_type
    }
    response = requests.post(url, data=json.dumps(data), headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        raw_token = response_data["token"]
        bearer_token = f"Bearer {raw_token}"
        return bearer_token
    else:
        response_data = response.json()
        message = response_data["message"]
        logger.exception(message)
        raise TokenGenerationExceptions(message)
def generate_and_save_proxy_token(token: str, id: str, collection):
    data = {
        "serviceToken": get_config("SERVICE_TOKEN"),
        "authToken": token,
    }
    proxy_auth_token = generate_proxy_token(data)
    data = {
        "serviceToken": get_config("SERVICE_TOKEN"),
        "proxyAuthorizationToken": proxy_auth_token,
    }
    proxy_access_token = generate_proxy_token(data)
    if id:
        update_fields = {"token": proxy_auth_token, "id": id}
        collection.update_one(
            {"id": id},
            {"$set": update_fields},
            upsert=True,
        )
    return proxy_access_token
def convert_headers(req):
    headers_dict = {}
    for header, value in req.headers.items():
        if header in headers_dict:
            headers_dict[header].append(value)
        else:
            headers_dict[header] = [value]
    return headers_dict
def is_request_exceptional(request: Request) -> bool:
    path = request.path.lower()
    if (
        re.match(r"^/$", path)
        or re.match(r".*api-docs.*", path)
        or re.match("^/s3/download/([^/]+)$", path)
        or re.match(r"/[^/]+/health$", path)
        or re.match(r"/health$", path)
    ):
        return True
    return False
def consider_request_without_auth(request: Request) -> True:
    path = request.path.lower()
    pattern = r"(?=.*\/(system-db|ops|read|maintenance)\/api\/v1\/)"
    if re.match(pattern, path):
        return True
    return False
def get_api_request_metadata(user_details: UserDetails, request: Request):
    MEDIA_TYPE = "application/json"
    if consider_request_without_auth(request):
        api_version = "v1"
        headers = {
            "Authorization": request.headers.get("Authorization", None),
            "Content-Type": MEDIA_TYPE,
            "X-Org-Id": user_details.org_id,
            "X-Project-Id": user_details.project_id,
            "X-Username": user_details.username,
        }
    else:
        api_version = "v2"
        headers = {
            "Authorization": user_details.token,
            "Content-Type": MEDIA_TYPE,
            "X-Org-Id": user_details.org_id,
            "X-Project-Id": user_details.project_id,
            "X-Service-Token": request.headers.get("X-Service-Token", None),
        }
Uncovered code
    return api_version, headers