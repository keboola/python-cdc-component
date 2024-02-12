import dataclasses
import json
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

import dataconf
from pyhocon import ConfigTree


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
                if f.default == dataclasses.MISSING
                and f.default_factory == dataclasses.MISSING]


@dataclass
class SSHKeys(ConfigurationBase):
    public: Optional[str] = None
    pswd_private: Optional[str] = None


@dataclass
class SSHConfiguration(ConfigurationBase):
    sshHost: Optional[str] = None
    user: Optional[str] = None
    sshPort: int = 22
    keys: SSHKeys = dataclasses.field(default_factory=lambda: ConfigTree({}))
    enabled: bool = False

    LOCAL_BIND_ADDRESS = "localhost"
    LOCAL_BIND_PORT = 9800


@dataclass
class DbOptions(ConfigurationBase):
    host: str
    port: int
    database: str
    user: str
    pswd_password: str
    ssh_options: SSHConfiguration = dataclasses.field(default_factory=lambda: ConfigTree({}))


@dataclass
class SourceSettings(ConfigurationBase):
    schemas: list[str] = dataclasses.field(default_factory=list)
    tables: list[str] = dataclasses.field(default_factory=list)
    primary_key: list[str] = dataclasses.field(default_factory=list)


class SnapshotMode(str, Enum):
    initial = "initial"
    never = "never"


@dataclass
class SyncOptions(ConfigurationBase):
    snapshot_mode: SnapshotMode = SnapshotMode.initial
    snapshot_fetch_size: int = 10240
    snapshot_threads: int = 1


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"


@dataclass
class DestinationSettings(ConfigurationBase):
    load_type: LoadType = LoadType.incremental_load

    @property
    def is_incremental_load(self) -> bool:
        return self.load_type == LoadType.incremental_load


@dataclass
class Configuration(ConfigurationBase):
    # Connection options
    db_settings: DbOptions
    source_settings: SourceSettings = dataclasses.field(default_factory=lambda: ConfigTree({}))
    sync_options: SyncOptions = dataclasses.field(default_factory=lambda: ConfigTree())
    destination: DestinationSettings = dataclasses.field(default_factory=lambda: ConfigTree({}))
    debug: bool = False
    max_workers: int = 10