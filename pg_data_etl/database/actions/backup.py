from pathlib import Path
import pg_data_etl.database.helpers as helpers


def backup_to_sql_file(db, output_folder: Path) -> Path:
    """
    - Create a standalone text file backup of the entire database.
    - Returns the full filepath to the newly created file.
    """

    db_name = db.connection_params["db_name"]
    timestamp = helpers.timestamp_for_filepath()

    filename = f"{db_name}_{timestamp}.sql"
    output_filepath = output_folder / filename

    command = f'{db.pg_dump} --no-owner --no-acl {db.uri} > "{output_filepath}"'

    print(command)

    helpers.run_command_in_shell(command)

    return output_filepath
