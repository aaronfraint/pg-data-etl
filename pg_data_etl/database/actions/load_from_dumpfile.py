from pathlib import Path

from pg_data_etl import helpers


def load_from_dumpfile(self, filepath: str) -> None:
    """
    - Load a `.sql` file into a new database

    Args:
        filepath (str): full path to the SQL file
    """

    db_name = self.connection_params["db_name"]

    if self.exists():
        print(f"Database {db_name} already exists. Use a different name.")
    else:
        command = f'"{self.cmd.psql}" -f "{filepath}" {self.uri}'
        helpers.run_command_in_shell(command)