from dataclasses import dataclass, field
from dataclass_wizard import JSONSerializable
from typing import Optional, Dict, List
from .enums import IncrementalReadOption, DataFormats, SourceSystems
from .storage.mongo import MongoDBConnector
from .utils import logger
from .config_loader import get_config
@dataclass
class UserDetails(JSONSerializable):
    org_id: str
    project_id: str
    token: Optional[str] = None
    user_id: Optional[str] = None
    username: Optional[str] = None
    email: Optional[str] = None
    full_name: Optional[str] = None
@dataclass
class ConnectionConfig(JSONSerializable):
    access_key_id: str
    secret_access_key: str
    region: str
    account_name: Optional[str] = "infor-albanero"
    source_type: Optional[str] = None
    catalog_id: Optional[str] = None
    bucket: Optional[str] = None
    base_folder_path: Optional[str] = None
    db_prefix: Optional[str] = None
@dataclass
class DatabaseConnectionConfig(JSONSerializable):
    username: str
    password: str
    host: Optional[str] = None
    port: Optional[str] = None
    account: Optional[str] = None
    auth_db: Optional[str] = None
    organization: Optional[str] = None
    account_name: Optional[str] = "infor-albanero"
@dataclass
class CSVDelimiters(JSONSerializable):
    field_delimiter: str
    quote_character: str
    quote_escape_character: str
    record_delimiter: str
    encoding: str
    quoting_method: Optional[str] = None
    program_name: Optional[str] = None
    @classmethod
    def get_default(
        cls,
        field_delimiter: str = "0x2c",
        quote_character: str = "0x22",
        quote_escape_character: str = "0x5c",
        record_delimiter: str = "0x0a",
        encoding: str = "utf-8",
        quoting_method: str = "DEFAULT",
    ):
        return cls(
            field_delimiter,
            quote_character,
            quote_escape_character,
            record_delimiter,
            encoding,
            quoting_method,
        )
@dataclass
class DA2Delimiters(JSONSerializable):
    field_delimiter: str
    quote_character: str
    quote_escape_character: str
    record_delimiter: str
    encoding: str
    quoting_method: Optional[str] = None
    program_name: Optional[str] = None
    target_system: SourceSystems = SourceSystems.BAAN
    @classmethod
    def get_default(
        cls,
        table_name: Optional[str] = None,
        field_delimiter: str = "0x01",
        quote_character: str = "0x00",
        quote_escape_character: str = "0x00",
        record_delimiter: str = "0x0d0a",
        encoding: str = "utf-8",
        quoting_method: str = "PRESERVE_BLANK",
        target_system=SourceSystems.BAAN,
    ):
        program_name = (
            cls.fetch_program_name(table_name, target_system) if table_name else None
        )
        return cls(
            field_delimiter,
            quote_character,
            quote_escape_character,
            record_delimiter,
            encoding,
            quoting_method,
            program_name,
            target_system,
        )
    @staticmethod
    def fetch_program_name(table_name: str, target_system: str) -> Optional[str]:
        """Fetch MetaData for the given TableName while fetching DA2/DAT Default Delimiters
        Args:
            table_name (str): User provided File Name or Program Name
        Returns:
            Optional[str]: Table Name if it exists . None if it doesn't exist
        """
        logger.debug(
            f"Asserting ProgramName from {target_system.value} MetaData db.", table_name
        )
        base_table_name = table_name.split("/")[-1].split(".")[0].lower()
        base_table_name = base_table_name[-8:]
        mongo_client = MongoDBConnector.get_instance()
        db = mongo_client[get_config("DB_NAME_LN_METADATA")]
        if target_system == SourceSystems.LN:
            collection_name = get_config("COL_NAME_LN_TABLE_METADATA")
        else:
            collection_name = get_config("COL_NAME_BAAN_TABLE_METADATA")
        collection = db[collection_name]
        if collection.count_documents({"tableName": base_table_name}):
            logger.debug(
                f"Found matching program name in {collection_name} for {table_name}"
            )
            return base_table_name
        logger.debug(
            f"No matching program name found in BAAN or LN metadata for {table_name}"
        )
        return None
@dataclass
class DATDelimiters(DA2Delimiters):
    field_delimiter: str
    quote_character: str
    quote_escape_character: str
    record_delimiter: str
    target_system: SourceSystems = SourceSystems.BAAN
    program_name: Optional[str] = None
    @classmethod
    def get_default(
        cls,
        table_name: Optional[str] = None,
        field_delimiter: str = "0x01",
        quote_character: str = "0x00",
        quote_escape_character: str = "0x00",
        record_delimiter: str = "0x0a",
        encoding: str = "utf-8",
        quoting_method: str = "PRESERVE_BLANK",
        target_system: SourceSystems = SourceSystems.BAAN,
    ):
        program_name = (
            cls.fetch_program_name(table_name, target_system) if table_name else None
        )
        return cls(
            field_delimiter,
            quote_character,
            quote_escape_character,
            record_delimiter,
            encoding,
            quoting_method,
            program_name,
            target_system,
        )
@dataclass(kw_only=True)
class TableDetails(JSONSerializable):
    connector_id: str
    table_name: str
    database_name: str
    source_type: str
    connection_details: Optional[ConnectionConfig] = None
    alias: Optional[str] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    job_id: Optional[str] = None
    output_without_timestamp: Optional[bool] = False
    requires_incremental_read: Optional[str] = IncrementalReadOption.NO
    time_stamp_column_name: Optional[str] = None
    @property
    def requires_incremental_read(self):
        return self._requires_incremental_read
    @requires_incremental_read.setter
    def requires_incremental_read(self, requires_incremental_read: str):
        if isinstance(requires_incremental_read, str):
            self._requires_incremental_read = requires_incremental_read.upper()
        else:
            self._requires_incremental_read = IncrementalReadOption.NO
@dataclass
class DeltaLakeTable(TableDetails, JSONSerializable):
    version: Optional[int] = None
    timestamp: Optional[str] = None
    mode: Optional[str] = None
    write_source_only: Optional[bool] = False
    generate_manifest: Optional[bool] = True
    merge_keys: Optional[Dict[str, str]] = field(default_factory=dict)
    partition_keys: Optional[List[str]] = field(default_factory=list)
    create_if_not_found: Optional[bool] = False
    api_version: Optional[str] = "v2"
    def __post_init__(self):
        if self.merge_keys is not None and not isinstance(self.merge_keys, dict):
            raise TypeError(
                "merge_keys must be a dictionary with string keys and string values"
            )
        for key, value in self.merge_keys.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise TypeError(
                    "merge_keys must be a dictionary with string keys and string values"
                )
@dataclass
class IbmDb2Table(TableDetails, JSONSerializable):
    schema: Optional[str] = None
    columns: Optional[List[str]] = field(default_factory=list)
    query: Optional[str] = None
    partition_column_dict: Optional[dict] = None
@dataclass
class MSSQLTable(TableDetails, JSONSerializable):
    schema: Optional[str] = None
    columns: Optional[List[str]] = field(default_factory=list)
    query: Optional[str] = None
    partition_column_dict: Optional[dict] = None
@dataclass
class SnowflakeTable(TableDetails, JSONSerializable):
    schema: Optional[str] = None
    sf_warehouse: Optional[str] = None
    columns: Optional[List[str]] = field(default_factory=list)
    query: Optional[str] = None
    partition_column_dict: Optional[dict] = None
@dataclass
class OracleTable(TableDetails, JSONSerializable):
    schema: Optional[str] = None
    columns: Optional[List[str]] = field(default_factory=list)
    query: Optional[str] = None
    partition_column_dict: Optional[dict] = None
@dataclass(kw_only=True)
class S3Table(TableDetails):
    region: str
    target_delimiters: Optional[dict] = None
    options: Optional[dict] = None
    format: Optional[str] = None
    target_table_folder: Optional[str] = None
    output_without_timestamp: Optional[bool] = False
    mode: Optional[str] = None
@dataclass
class JobMetadata(JSONSerializable):
    id: str
    job_type: str
    job_name: str
@dataclass
class MultiTableDetails(S3Table):
    table_name: List[str]
class ProfilingResults(JSONSerializable):
    connector_id: str
    table_name: str
    database_name: str
    length_histogram: Optional[bool] = False