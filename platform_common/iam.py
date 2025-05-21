import re
from .utils.apicaller import ApiCaller
from flask import request, jsonify, Request
from .exceptions.exception_handler import DataMeshExceptionHandler
from .exceptions.custom_exceptions import (
    UnableToFetchUserDetailsException,
    TokenGenerationExceptions,
)
from .utils import logger
from .config_loader import get_config
from typing import Union
from .dataclasses import UserDetails
from typing import Dict
content_type = "application/json"
class IAM:
    @staticmethod
    def authenticate():
        """
        This method is responsible for user authentication using a token.
        It sends a request to one of the IAM APIs to authenticate the user.
        If the authentication is successful, it sets the user details in the request.
        In case of failure, it returns the appropriate response.
        """
        try:
            if is_request_exceptional(request):
                return
            token = request.headers.get("Authorization")
            if token is None:
                logger.debug("Authentication token is missing.")
                return (
                    jsonify(
                        {
                            "message": "Authentication token is missing.",
                            "success": False,
                        }
                    ),
                    401,
                )
            auth_headers = {"Authorization": token, "Content-Type": content_type}
            authentication_api = (
                f"{get_config('ALBANERO_BASE_ROUTE_URI')}/auth/api/token/validate"
            )
            response = ApiCaller.get(url=authentication_api, headers=auth_headers)
            if response.status_code == 200:
                response_data = response.json()["payload"]
                request.user_details = {
                    "userId": response_data["userId"],
                    "username": response_data["username"],
                    "token": token,
                    "email": response_data["emailId"],
                    "fullName": f"{response_data['firstName']} {response_data['lastName']}",
                }
            else:
                message = response.text
                logger.exception(
                    f"message: {message}, route: {authentication_api}, response: {response.json()}"
                )
                return (jsonify({"message": message, "success": False}), 401)
        except Exception as e:
            message = DataMeshExceptionHandler.parse_message(e)
            logger.exception(f"Exception occurred during authentication: {message}")
            return (
                jsonify(
                    {
                        "message": "Request failed during authentication.",
                        "success": False,
                    }
                ),
                500,
            )
    @staticmethod
    def authorize():
        """
        This method handles the authorization of headers.
        It sends a request to one of the IAM APIs to validate the headers.
        Returns:
           JSON Response: The method returns the API response directly.
        """
        try:
            if is_request_exceptional(request):
                return
            org_id = request.headers.get("X-Org-Id")
            project_id = request.headers.get("X-Project-Id")
            if org_id is None or project_id is None:
                logger.debug("Authorization headers are missing.")
                return (
                    jsonify(
                        {
                            "message": "Authorization headers are missing.",
                            "success": False,
                        }
                    ),
                    403,
                )
            request.user_details["orgId"] = org_id
            request.user_details["projectId"] = project_id
            request.user_details: UserDetails = UserDetails.from_dict(
                request.user_details
            )
            headers = {
                "Authorization": request.user_details.token,
                "Content-Type": content_type,
            }
            data = {
                "apiRoute": request.path,
                "apiMethod": request.method,
                "orgDetails": {
                    "orgId": org_id,
                    "roleId": None,
                },
                "projectLevelDetails": {
                    "projectId": project_id,
                    "roleId": None,
                },
            }
            authorization_api = (
                f"{get_config('ALBANERO_BASE_ROUTE_URI')}/auth-user/api/authorize-route"
            )
            response = ApiCaller.post(url=authorization_api, data=data, headers=headers)
            if response.status_code != 200 or response.json()["success"] != True:
                message = response.text
                if response.json().get("message"):
                    message = response.json().get("message")
                logger.exception(
                    f"message: {message}, route: {authorization_api}, payload: {data}, response: {response.json()}"
                )
                return jsonify({"message": message, "success": False}), 403
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
    def generate_token(user_id: str) -> Union[str, None]:
        """Generate a new access token for the given user"""
        try:
            logger.debug("Started generate_token function.")
            auth_headers = {
                "x-secret": get_config("ALBANERO_TOKEN_SERVICE_SECRET_KEY"),
                "Content-Type": content_type,
            }
            token_gen_api = (
                f"{get_config('ALBANERO_BASE_ROUTE_URI')}/auth/api/internal-token"
            )
            response = ApiCaller.get(
                url=token_gen_api, headers=auth_headers, params={"userId": user_id}
            )
            if response.status_code == 200:
                response_data = response.json()
                raw_token = response_data["payload"]["token"]
                bearer_token = f"Bearer {raw_token}"
                logger.debug("Exiting generate_token function.")
                return bearer_token
            else:
                message = f"Failed to generate token for user_id [{user_id}], Error: {response.text}"
                logger.exception(message)
                raise TokenGenerationExceptions(message)
        except Exception as e:
            message = DataMeshExceptionHandler.parse_message(e)
            logger.exception(message)
            raise TokenGenerationExceptions(message)
    @staticmethod
    def get_user_details(user_id: str, user_details: UserDetails) -> Dict[str, any]:
        """
        This function is useful to get the user information based on the user id.
        Args:
            user_id (str) :  Unique identifier to identify the user.
            user_details (UserDetails): User-related information, which may include orgId and other pertinent details.
        Returns:
            dict: Gives back the user info if details are found otherwise None
        """
        auth_headers = {
            "x-org-id": user_details.org_id,
            "x-project-id": user_details.project_id,
            "Authorization": user_details.token,
        }
        user_details_api = (
            f"{get_config('ALBANERO_BASE_ROUTE_URI')}/auth-user/api/user/{user_id}"
        )
        response = ApiCaller.get(user_details_api, headers=auth_headers)
        if response.status_code == 200:
            response_in_json = response.json()
            return response_in_json["payload"]
        elif response.status_code >= 400 and response.status_code <= 500:
            raise UnableToFetchUserDetailsException(user_id)
New code
def is_request_exceptional(request: Request) -> bool:
    path = request.path.lower()
    if (
        re.match(r".*api-docs.*", path)
        or re.match(r"/[^/]+/health$", path)
        or re.match(r"/health$", path)
    ):
Uncovered code
        return True
    return False