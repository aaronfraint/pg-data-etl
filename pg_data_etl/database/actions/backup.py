from pathlib import Path
import pg_data_etl.database.helpers as helpers


def backup_to_sql_file(self, output_folder: Path) -> Path:
    """
    Create a standalone text file backup of the entire database.
    Returns the full filepath to the newly created file.
    """

    filename = f"{self.params['db_name']}_{helpers.timestamp_for_filepath()}.sql"
    output_filepath = output_folder / filename

    command = f'{self.pg_dump} --no-owner --no-acl {self.uri()} > "{output_filepath}"'

    print(command)

    helpers.run_command_in_shell(command)

    return output_filepath
