from pg_data_etl import helpers


def list_of_all_tables(db, schema: str = None) -> list:
    """
    - Get a list of all tables in the db.
    - Omit the behind-the-scenes tables within `pg_catalog` and `information_schema`
    """
    query = """
        SELECT concat(table_schema, '.', table_name )
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    """

    if schema:
        query += f" AND table_schema = '{schema}'"

    return helpers.get_list_of_singletons_from_query(db, query)


def list_of_spatial_tables(db, schema: str = None) -> list:
    """
    - Get a list of all SPATIAL tables in the db
    """

    query = """
        SELECT concat(f_table_schema, '.', f_table_name )
        FROM geometry_columns
    """

    if schema:
        query += f" WHERE f_table_schema = '{schema}'"

    return helpers.get_list_of_singletons_from_query(db, query)


def list_of_schemas(db) -> list:
    """
    - Get a list of all schemas in the db
    """

    query = """
        SELECT schema_name
        FROM information_schema.schemata;
    """

    return helpers.get_list_of_singletons_from_query(db, query)


def list_of_columns_in_table(db, tablename: str) -> list:
    """
    - Get a list of all column names in a given table
    """

    schema, tbl = helpers.convert_full_tablename_to_parts(tablename)

    query = f"""
        SELECT DISTINCT column_name
        FROM information_schema.columns
        WHERE
            table_name = '{tbl}'
            AND
            table_schema = '{schema}';
    """

    return helpers.get_list_of_singletons_from_query(db, query)
