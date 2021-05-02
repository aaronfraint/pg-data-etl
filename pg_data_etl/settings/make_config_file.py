from pathlib import Path
from typing import Union

from ..settings import STARTER_CONFIG_FILE, DB_CONFIG_FILEPATH


def make_config_file(
    filepath: Union[Path, str] = DB_CONFIG_FILEPATH, overwrite: bool = False
) -> bool:
    """
    TODO: docstring
    """

    filepath = Path(filepath)

    # Make sure the parent fiolder exists
    if not filepath.parent.exists():
        filepath.parent.mkdir(parents=True)

    if not overwrite and filepath.exists():
        print("Config file already exists and overwrite=False. Will not overwrite.")
        return False

    else:
        if filepath.exists():
            print(f"Overwriting config file at {filepath}")
        else:
            print(f"Creating a new config file at {filepath}")

        with open(filepath, "w") as open_file:
            open_file.write(STARTER_CONFIG_FILE)

        return True
