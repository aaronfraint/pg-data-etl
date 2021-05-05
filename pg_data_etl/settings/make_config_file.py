from pathlib import Path
from typing import Union

from pathlib import Path

DB_CONFIG_FILEPATH = Path.home() / ".pg-data-etl" / "database_connections.cfg"

STARTER_CONFIG_FILE = """
[DEFAULT]
pw = this-is-a-placeholder-password
port = 5432
super_db = postgres
super_un = postgres
super_pw = this-is-another-placeholder-password

[localhost]
host = localhost
un = postgres
pw = your-password-here

[digitalocean]
un = your-username-here
host = your-host-here.db.ondigitalocean.com
pw = your-password-here
port = 98765
sslmode = require
super_db = your_default_db
super_un = your_super_admin
super_pw = some_super_password12354
"""


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
