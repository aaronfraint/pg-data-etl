import configparser
from pathlib import Path

from .make_config_file import make_config_file, DB_CONFIG_FILEPATH


def read_config_file(filepath: Path = DB_CONFIG_FILEPATH) -> dict:
    """
    TODO: docstring
    """

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
    """
    TODO: docstring
    """
    if not filepath.exists():
        make_config_file(filepath)

    if verbose:
        print(f"Loading db configurations from {filepath}")

    return read_config_file(filepath)
