from pg_data_etl import helpers


def create_database(db) -> None:
    """
    - Create the database if it doesn't exist yet, via `psql`
    """

    if not db.exists():

        db_name = db.connection_params["db_name"]

        # Create the database
        command = f'psql -c "CREATE DATABASE {db_name};" {db.uri_superuser}'
        helpers.run_command_in_shell(command)

        # Enable PostGIS
        command = f'psql -c "CREATE EXTENSION postgis;" {db.uri}'
        helpers.run_command_in_shell(command)


def drop_database(db) -> None:
    """
    - Drop the database if it exists, via `psql`
    """

    if db.exists():
        db_name = db.connection_params["db_name"]

        command = f'psql -c "DROP DATABASE {db_name};" {db.uri_superuser}'
        helpers.run_command_in_shell(command)


def add_schema(db, schema: str) -> None:
    """
    - Create a schema if it does not yet exist
    """

    query = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    db.execute(query)
