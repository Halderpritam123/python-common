from copy import deepcopy
from time import time
import re
from uuid import uuid4
from ..dataclasses import (
    S3Table,
    DeltaLakeTable,
    TableDetails,
    IbmDb2Table,
    SnowflakeTable,
    MSSQLTable,
    OracleTable,
)
from ..enums import SourceTargetTypes
from ..config_loader import get_config
from platform_common.stream.kafka import KafkaConnector, ExistingKafkaConnection
from platform_common.storage.mongo import MongoDBConnector
from . import logger
from flask import Request as FlaskRequest, jsonify
class RawURLMiddleware:
    def __init__(self, app):
        self.app = app
    def __call__(self, environ, start_response):
        raw_path = environ.get("RAW_URI", None)
        environ["RAW_REQUEST_URI"] = raw_path
        return self.app(environ, start_response)
class CustomRequest(FlaskRequest):
    @property
    def raw_url(self):
        return self.environ.get("RAW_REQUEST_URI", None)
def current_time_ms() -> int:
    return round(time() * 1000)
def str_from_hex(input: str) -> str:
    if input == "0x0d0a":
        return "\r\n"
    return chr(int(input, base=16))
def uuid4_str() -> str:
    return str(uuid4())
def append_before_format(file_path: str, *append_strings: str) -> str:
    append_str = "_".join(append_strings)
    str_list = file_path.split(".")
    if len(str_list) == 1:
        return f"{file_path}_{append_str}"
    file_format = str_list[-1]
    path = ".".join(str_list[:-1])
    return f"{path}_{append_str}.{file_format}"
def health_check(check_options: dict = None):
    if not check_options:
        check_options = {}
    try:
        if check_options.get("kafka"):
            KafkaConnector()
        if check_options.get("mongo"):
            MongoDBConnector.get_instance()
        return jsonify({"status": "healthy"}), 200
    except ExistingKafkaConnection:
        return jsonify({"status": "healthy"}), 200
    except Exception as e:
        logger.error(f"Error in health check: {str(e)}")
        return jsonify({"status": "unhealthy", "message": str(e)}), 500
def file_name_split_with_target(file_path: str, target_path=None) -> str:
    file_table = file_path.split("/")[-1]
    reg = r"_(\d\d)_(Jan(uary)?|Feb(ruary)?|Mar(ch)?|Apr(il)?|May|Jun(e)?|Jul(y)?|Aug(ust)?|Sep(tember)?|Oct(ober)?|Nov(ember)?|Dec(ember)?)_(\d{4})_(\d{2})_(\d{2})_(\d{2})"
    file_table = re.sub(reg, "", file_table)
    if target_path:
        target_table_name = f"{target_path}/{file_table}"
    else:
        target_table_name = f"{file_table}"
    target_table_name = target_table_name.split(".")[0]
    return f"{target_table_name}"
def create_table_instance(details: dict) -> TableDetails:
    source_type = details.get("sourceType")
    # Mapping of source types to their corresponding from_dict methods
    source_type_map = {
        SourceTargetTypes.S3: S3Table,
        SourceTargetTypes.DELTALAKE: DeltaLakeTable,
        SourceTargetTypes.IBM_DB2: IbmDb2Table,
        SourceTargetTypes.SNOWFLAKE: SnowflakeTable,
        SourceTargetTypes.MS_SQL: MSSQLTable,
        SourceTargetTypes.ORACLE: OracleTable,
    }
    table_class = source_type_map.get(source_type)
    if table_class:
        return table_class.from_dict(details)
    else:
        raise ValueError(f"Unknown source type: {source_type}")
New code
def get_ln_metadata_columns(table_name: str, send_with_datatype: bool = False):
    """
    This method is used to get the column headers for ln meta data based on program name.
    Args:
        table_name (str): ln table name
    Returns:
        List : Returns list of columns names if meta data found or a empty list.
    """
    table_name = table_name.lower()
    mongo_client = MongoDBConnector.get_instance()
    db = mongo_client[get_config("DB_NAME_LN_METADATA")]
    collection = db[get_config("COL_NAME_LN_TABLE_METADATA")]
    result = collection.find_one({"tableName": table_name}, {"_id": 0})
    if result and result.get("columns"):
        if send_with_datatype:
            column_meta_dict = {}
            for column_info in result["columns"]:
                column_name = column_info["columnName"][-4:].upper()
                column_meta_dict[column_name] = column_info["datatype"]
            return column_meta_dict
        else:
            column_list = []
            for column_data in result["columns"]:
                col_name = column_data["columnName"][-4:].upper()
                column_list.append(col_name)
            return column_list
def get_baan_metadata_columns(table_name: str, send_with_datatype: bool = False):
    """
    This method is used to get the column headers for Baan meta data based on program name.
    Args:
        table_name (str): ln table name
    Returns:
        List : Returns list of columns names if meta data found or a empty list.
    """
    mongo_client = MongoDBConnector.get_instance()
    db = mongo_client[get_config("DB_NAME_LN_METADATA")]
    collection = db[get_config("COL_NAME_BAAN_TABLE_METADATA")]
    result = collection.find_one({"tableName": table_name.lower()}, {"_id": 0})
    if result and result.get("columns"):
        if send_with_datatype:
            column_meta_dict = {}
            for column_info in result["columns"]:
                column_name = column_info["columnDbName"].upper()
                column_meta_dict[column_name] = column_info["datatype"]
            return column_meta_dict
        else:
            column_list = []
            for column_data in result["columns"]:
                col_name = column_data["columnDbName"].upper()
                column_list.append(col_name)
            return column_list
def replace_dollar_dot(obj, invert=False):
    """Provides bidirectional conversion of dictionary keys
        to make it compatible with MongoDB
    Args:
        obj (): Input object, it could be of any type
        invert (bool, optional):
            If True, replaces key strings with symbols
                e.g. __dot__ is replaced by dot[.]
                    __dollar__ is replaced by dollar[$]
            If False, replaces key symbols with strings
                e.g. dot[.] is replaced by __dot__
                    dollar[$] is replaced by __dollar__
            Defaults to False.
    Returns:
        obj : Converted object
    """
    update_obj = deepcopy(obj)
    if isinstance(obj, list):
        update_obj = []
        for items in obj:
            update_obj.append(replace_dollar_dot(items, invert))
    elif isinstance(obj, dict):
        for key, value in obj.items():
            update_key = key
            if invert:
                if "__dollar__" in key:
                    update_key = update_key.replace("__dollar__", "$")
                if "__dot__" in key:
                    update_key = update_key.replace("__dot__", ".")
            else:
                if "$" in key:
                    update_key = update_key.replace("$", "__dollar__")
                if "." in key:
                    update_key = update_key.replace(".", "__dot__")
            update_obj[update_key] = update_obj.pop(key)
            if isinstance(value, (dict, list)):
                update_obj[update_key] = replace_dollar_dot(value, invert)
Uncovered code
    return update_obj