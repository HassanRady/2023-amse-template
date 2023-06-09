from pathlib import Path
from typing import Dict, List, Sequence

from pydantic import BaseModel
from strictyaml import YAML, load

import project

# Project Directories
PACKAGE_ROOT = Path(project.__file__).resolve().parent.parent
CONFIG_FILE_PATH = PACKAGE_ROOT/ "project" / "config.yml"




class DataBaseConfig(BaseModel):
    DB_PATH: str
    TRAIN_TABLE: str
    PLAN_CHANGE_TABLE: str

class WeatherDataConfig(BaseModel):
    WEATHER_DATA_URL: str
    WEATHER_DATA_PATH: str

class Config(BaseModel):
    """Master config object."""

    database: DataBaseConfig
    weather_data: WeatherDataConfig


def find_config_file() -> Path:
    """Locate the configuration file."""
    if CONFIG_FILE_PATH.is_file():
        return CONFIG_FILE_PATH
    raise Exception(f"Config not found at {CONFIG_FILE_PATH!r}")


def fetch_config_from_yaml(cfg_path: Path = None) -> YAML:
    """Parse YAML containing the package configuration."""

    if not cfg_path:
        cfg_path = find_config_file()

    if cfg_path:
        with open(cfg_path, "r") as conf_file:
            parsed_config = load(conf_file.read())
            return parsed_config
    raise OSError(f"Did not find config file at path: {cfg_path}")


def create_and_validate_config(parsed_config: YAML = None) -> Config:
    """Run validation on config values."""
    if parsed_config is None:
        parsed_config = fetch_config_from_yaml()

    # specify the data attribute from the strictyaml YAML type.
    _config = Config(
        database=DataBaseConfig(**parsed_config.data),
        weather_data=WeatherDataConfig(**parsed_config.data),
    )

    return _config


config = create_and_validate_config()
