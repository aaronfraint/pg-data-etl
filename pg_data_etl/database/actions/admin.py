import pg_data_etl.database.helpers as helpers


def create_db(db) -> None:
    """
    Create the database if it doesn't exist yet
    """

    if not db.exists():

        # Create the database
        command = f'psql -c "CREATE DATABASE {db.params["db_name"]};" {db.uri(super_uri=True)}'
        helpers.run_command_in_shell(command)

        # Enable PostGIS
        command = f'psql -c "CREATE EXTENSION postgis;" {db.uri()}'
        helpers.run_command_in_shell(command)


def drop_db(db) -> None:
    """
    Drop the database if it exists.
    """

    if db.exists():
        command = (
            f'psql -c "DROP DATABASE {db.params["db_name"]};" {db.uri(super_uri=True)}'
        )
        helpers.run_command_in_shell(command)


def add_schema(db, schema: str) -> None:
    """Add a schema if it does not yet exist """

    if schema not in db.schema_list():
        db.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")


def table_list(db, schema: str = None) -> list:
    """
    Get a list of all tables in the db.
    Omit the behind-the-scenes tables.
    """
    query = """
        SELECT concat(table_schema, '.', table_name )
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    """

    if schema:
        query += f" AND table_schema = '{schema}'"

    tables = db.query_via_psycopg2(query)

    return [x[0] for x in tables]


def spatial_table_list(db, schema: str = None) -> list:
    """
    Get a list of all SPATIAL tables in the db
    """

    query = """
        SELECT concat(f_table_schema, '.', f_table_name )
        FROM geometry_columns
    """

    if schema:
        query += f" WHERE f_table_schema = '{schema}'"

    geotables = db.query_via_psycopg2(query)

    return [x[0] for x in geotables]


def schema_list(db) -> list:
    """
    Get a list of all schemas in the db
    """

    query = """
        SELECT schema_name
        FROM information_schema.schemata;
    """

    schemas = db.query_via_psycopg2(query)

    return [x[0] for x in schemas]


def columns_in_table(db, tablename: str) -> list:
    """ Get a list of all column names in a given table. """

    schema, tbl = helpers.convert_full_tablename_to_parts(tablename)

    query = f"""
        SELECT DISTINCT column_name
        FROM information_schema.columns
        WHERE
            table_name = '{tbl}'
            AND
            table_schema = '{schema}';
    """

    return [x[0] for x in db.query_via_psycopg2(query)]
