import dataclasses
import json
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Union

import dataconf
from dataconf.utils import NoneType
from pyhocon import ConfigTree

KEY_INCLUDE_SCHEMA_NAME = 'include_schema_name'


class ConfigurationBase:

    @staticmethod
    def _convert_private_value(value: str):
        return value.replace('"#', '"pswd_')

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith('pswd_'):
            return value.replace('pswd_', '#', 1)
        else:
            return value

    @classmethod
    def load_from_dict(cls, configuration: dict):
        """
        Initialize the configuration dataclass object from dictionary.
        Args:
            configuration: Dictionary loaded from json configuration.

        Returns:

        """
        json_conf = json.dumps(configuration)
        json_conf = ConfigurationBase._convert_private_value(json_conf)
        return dataconf.loads(json_conf, cls, ignore_unexpected=True)

    @classmethod
    def get_dataclass_required_parameters(cls) -> List[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: List[str]

        """
        return [cls._convert_private_value_inv(f.name) for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING and f.default_factory == dataclasses.MISSING]


@dataclass
class SSHConfiguration(ConfigurationBase):
    host: Optional[str] = None
    username: Optional[str] = None
    pswd_private_key: Optional[str] = None
    pswd_key_password: Optional[str] = None
    port: int = 22

    LOCAL_BIND_ADDRESS = "localhost"
    LOCAL_BIND_PORT = 9800


@dataclass
class SSLConfiguration(ConfigurationBase):
    verifyCert: bool = True


@dataclass
class ShowLogConfig(ConfigurationBase):
    method: str = 'direct'
    endpoint_url: str = ''
    authentication: bool = False
    user: str = ''
    pswd_password: str = ''


@dataclass
class DbAdvancedParameters(ConfigurationBase):
    max_execution_time: Optional[str] = ""
    show_binary_log_config: ShowLogConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))


@dataclass
class ReplicaDbOptions(ConfigurationBase):
    host: str
    port: int
    user: str
    pswd_password: str
    use_ssh: bool = False
    ssh_options: SSHConfiguration = dataclasses.field(default_factory=lambda: ConfigTree({}))
    use_ssl: bool = False
    ssl_options: SSLConfiguration = dataclasses.field(default_factory=lambda: ConfigTree({}))


@dataclass
class DbOptions(ReplicaDbOptions):
    sync_from_replica: bool = False
    replica_db_settings: Union[ReplicaDbOptions, NoneType] = None


@dataclass
class SourceSettings(ConfigurationBase):
    schemas: list[str] = dataclasses.field(default_factory=list)
    tables: list[str] = dataclasses.field(default_factory=list)


class SnapshotMode(str, Enum):
    snapshot_only = "snapshot_only"
    when_needed = "when_needed"
    initial = "initial"
    never = "never"
    schema_only_recovery = "schema_only_recovery"
    schema_only = "schema_only"


class BinaryHandler(str, Enum):
    plain = "plain"
    hex = "hex"
    base64 = "base64"


@dataclass
class SyncOptions(ConfigurationBase):
    source_signal_table: str
    snapshot_mode: SnapshotMode = SnapshotMode.initial
    max_wait_s: int = 40
    snapshot_fetch_size: int = 10240
    snapshot_threads: int = 1
    handle_binary: BinaryHandler = BinaryHandler.hex


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"
    append_incremental = "append_incremental"
    append_full = "append_full"


@dataclass
class DestinationSettings(ConfigurationBase):
    load_type: LoadType = LoadType.incremental_load
    include_schema_name: bool = True
    outputBucket: str = ''

    @property
    def is_incremental_load(self) -> bool:
        return self.load_type == LoadType.incremental_load


@dataclass
class Configuration(ConfigurationBase):
    # Connection options
    db_settings: DbOptions
    advanced_options: DbAdvancedParameters = dataclasses.field(default_factory=lambda: ConfigTree({}))
    source_settings: SourceSettings = dataclasses.field(default_factory=lambda: ConfigTree({}))
    sync_options: SyncOptions = dataclasses.field(default_factory=lambda: ConfigTree())
    destination: DestinationSettings = dataclasses.field(default_factory=lambda: ConfigTree({}))
    debug: bool = False


def get_required_parameters() -> list[str]:
    return Configuration.get_dataclass_required_parameters()
