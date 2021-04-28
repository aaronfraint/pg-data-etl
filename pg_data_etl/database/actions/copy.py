from pathlib import Path
from pg_data_etl import Database
import pg_data_etl.database.helpers as helpers


def copy_table_to_another_db(db, table_to_copy: str, target_db: Database) -> None:
    """
    Pipe data directly from a pg_dump of one DB into another using psql
    """

    # If the table_to_copy has a schema, ensure that the schema also exists in the target db
    if "." in table_to_copy:
        schema = table_to_copy.split(".")[0]
        target_db.add_schema(schema)

    command = f"{db.pg_dump} --no-owner --no-acl -t {table_to_copy} {db.uri()} | psql {target_db.uri()}"

    print(command)
    helpers.run_command_in_shell(command)

    target_db.ensure_geometry_is_named_geom(table_to_copy)

    return None


def copy_entire_db_to_another_db(db, target_db: Database) -> None:
    """
    Copy an entire database to a new database.

    To get around memory error limitations, this is done in two steps as opposed to a single command with a pipe:
        Step 1) Backup the source db to .sql file with pg_dump
        Step 2) Load the .sql file into the target db with psql
    """

    if target_db.exists():
        print(
            f"A database named '{target_db.params['db_name']}' already exists. Use a different name or drop this database before copying into it."
        )
        return None

    target_db.create_db()

    sql_filepath = db.backup_to_sql_file(Path.cwd())

    command = f'psql -f  "{sql_filepath}" {target_db.uri()}'
    print(command)
    helpers.run_command_in_shell(command)

    # Ensure that spatial tables have 'geom' instead of 'shape' columns
    for table in target_db.spatial_table_list():
        target_db.ensure_geometry_is_named_geom(table)

    # Delete the .sql file from disk
    sql_filepath.unlink()

    return None
