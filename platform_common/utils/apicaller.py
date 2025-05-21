import requests
import json
from ..utils import logger
from flask import jsonify
from ..exceptions.exception_handler import DataMeshExceptionHandler
from platform_common.config_loader import get_config, get_service_token
from platform_common.auth_kit import IAM
default_error_message = "Something unexpected went wrong"
class ApiCaller:
    @staticmethod
    def get(url, params=None, headers=None):
        headers["X-Service-Token"] = get_config("SERVICE_TOKEN")
        try:
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 401:
                service_token_valid = IAM.validate_service_token()
                if not service_token_valid:
                    get_service_token()
                    return ApiCaller.get(url, params, headers)
                else:
                    return response
            else:
                return response
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred: {e}")
            message = DataMeshExceptionHandler.parse_message(e) or default_error_message
            return (
                jsonify(
                    {
                        "message": message,
                        "success": False,
                    }
                ),
                400,
            )
    @staticmethod
    def post(url, data=None, params=None, headers=None):
        headers["X-Service-Token"] = get_config("SERVICE_TOKEN")
        try:
            response = requests.post(
                url, data=json.dumps(data), params=params, headers=headers
            )
            if response.status_code == 401:
                service_token_valid = IAM.validate_service_token()
                if not service_token_valid:
                    get_service_token()
                    return ApiCaller.post(url, data, params, headers)
                else:
                    return response
            else:
                return response
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred: {e}")
            message = DataMeshExceptionHandler.parse_message(e) or default_error_message
            return (
                jsonify(
                    {
                        "message": message,
                        "success": False,
                    }
                ),
                400,
            )
    @staticmethod
    def put(url, data=None, params=None, headers=None):
        headers["X-Service-Token"] = get_config("SERVICE_TOKEN")
        try:
            response = requests.put(url, data=data, params=params, headers=headers)
            if response.status_code == 401:
                service_token_valid = IAM.validate_service_token()
                if not service_token_valid:
                    get_service_token()
                    return ApiCaller.put(url, data, params, headers)
                else:
                    return response
            else:
                return response
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred: {e}")
            message = DataMeshExceptionHandler.parse_message(e) or default_error_message
            return (
                jsonify(
                    {
                        "message": message,
                        "success": False,
                    }
                ),
                400,
            )
    @staticmethod
    def delete(url, params=None, headers=None):
        headers["X-Service-Token"] = get_config("SERVICE_TOKEN")
        try:
            response = requests.delete(url, params=params, headers=headers)
            if response.status_code == 401:
                service_token_valid = IAM.validate_service_token()
                if not service_token_valid:
                    get_service_token()
                    return ApiCaller.delete(url, params, headers)
                else:
                    return response
            else:
                return response
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred: {e}")
            message = DataMeshExceptionHandler.parse_message(e) or default_error_message
            return (
                jsonify(
                    {
                        "message": message,
                        "success": False,
                    }
                ),
                400,
            )
