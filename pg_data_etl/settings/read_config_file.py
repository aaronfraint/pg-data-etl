from __future__ import annotations
import configparser
from pathlib import Path

from .make_config_file import make_config_file, DB_CONFIG_FILEPATH


def read_config_file(filepath: Path | str = DB_CONFIG_FILEPATH) -> dict:
    """
    - Read the configuration file with database info, and return as a dictionary
    keyed on each entry in the file (could be one or multiple entries in the .cfg file)

    Arguments:
        filepath (Path | str): a Path or filepath as string to the configuration file.
                               Defaults to `DB_CONFIG_FILEPATH`

    Returns:
        dict: with all of the info keyed by database cluster
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


def configurations(filepath: Path | str = DB_CONFIG_FILEPATH, verbose: bool = False) -> dict:
    """
    - Load the configuration data from file
    - If the file does not exist yet, generate the template into the filepath provided by the user

    Arguments:
        filepath (Path | str): a Path or filepath as string to the configuration file
        verbose (bool): flag that will print out the path of the configuration file used

    Returns:
        dict: with all connection info keyed by cluster
    """
    if not filepath.exists():
        make_config_file(filepath)

    if verbose:
        print(f"Loading db configurations from {filepath}")

    return read_config_file(filepath)
