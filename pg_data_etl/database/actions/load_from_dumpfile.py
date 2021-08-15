from __future__ import annotations
from pathlib import Path

from pg_data_etl import helpers


def load_from_dumpfile(self, filepath: str | Path) -> None:
    """
    - Load a `.sql` file into a new database
    - The database must not already exist

    Arguments:
        filepath (str): full path to the SQL file

    Returns:
        loads the `.sql` file into an empty database
    """

    db_name = self.connection_params["db_name"]

    if self.exists():
        print(f"Database {db_name} already exists. Use a different name.")
    else:
        command = f'"{self.cmd.psql}" -f "{filepath}" {self.uri}'
        helpers.run_command_in_shell(command)

    return None
