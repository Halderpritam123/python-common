from ..utils import logger
from ..dataclasses import S3Table, UserDetails
from ..utils.apicaller import ApiCaller
from ..exceptions.exception_handler import DataMeshExceptionHandler
from ..config_loader import get_config
def trigger_rescan(table_info: S3Table, user_details: UserDetails, is_manual_scan: bool = False):
    """
    Trigger a rescan for the specified S3 path.
    Args:
        table_info (S3Table): Details of the source table, including connectorId, bucket or database_name, region, and table_name.
        user_details (UserDetails): User details, including orgId, project_id, and token.
    """
    try:
        logger.debug("Scan has been initiated.", table_info.job_id)
        scan_api = f"{get_config('ALBANERO_BASE_ROUTE_URI')}/s3-scanner/scan/folder"
        data = {
            "connectorId": table_info.connector_id,
            "bucket": table_info.database_name,
            "region": table_info.region,
            "path": table_info.table_name,
        }
        if is_manual_scan:
            data["triggerType"] = "Manual"
        auth_headers = {
            "Authorization": user_details.token,
            "X-Org-Id": user_details.org_id,
            "X-Project-Id": user_details.project_id,
            "Content-Type": "application/json",
        }
        response = ApiCaller.post(
            url=scan_api,
            data=data,
            headers=auth_headers,
        )
        if response.status_code == 202 and response.json()["success"] == True:
            logger.info(f"Triggered scan: {str(data)}", table_info.job_id)
        elif response.status_code >= 400:
            logger.error(
                f"Could not trigger scan: {response.status_code} {response.json()}"
            )
Uncovered code
    except Exception as err:
        message = DataMeshExceptionHandler.parse_message(err)
        logger.error(f"Scan failed with the error: {message}", table_info.job_id)