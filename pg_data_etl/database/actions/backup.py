from pathlib import Path

from pg_data_etl import helpers


def dump(self, output_folder: str = ".") -> Path:
    """
    - Create a standalone text file backup of the entire database.
    - Returns the full filepath to the newly created file.

    Arguments:
        output_folder (str): folder where output file should go. Defaults to active directory.

    Returns:
        Path: to the newly created output file
    """

    db_name = self.connection_params["db_name"]
    timestamp = helpers.timestamp_for_filepath()

    filename = f"{db_name}_{timestamp}.sql"
    output_filepath = Path(output_folder) / filename

    command = f'"{self.cmd.pg_dump}" --no-owner --no-acl {self.uri} > "{output_filepath}"'

    print(command)

    helpers.run_command_in_shell(command)

    return output_filepath
