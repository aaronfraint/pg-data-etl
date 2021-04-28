import configparser
from pathlib import Path

from ..settings import DB_CONFIG_FILEPATH
from .make_config_file import make_config_file


def read_config_file(filepath: Path = DB_CONFIG_FILEPATH) -> dict:

    config = configparser.ConfigParser()
    config.read(filepath)

    all_hosts = {}

    for host in config.sections():
        all_hosts[host] = {}

        for key in config[host]:
            value = config[host][key]

            all_hosts[host][key] = value

    return all_hosts


def configurations(filepath: Path = DB_CONFIG_FILEPATH, verbose: bool = False) -> dict:

    if not filepath.exists():
        make_config_file(filepath)

    if verbose:
        print(f"Loading db configurations from {filepath}")

    return read_config_file(filepath)
