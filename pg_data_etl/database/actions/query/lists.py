from typing import Union
from pg_data_etl import helpers
from .simple import get_list_of_singletons_from_query


def all_tables(self, schema: str = None) -> list:
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

    return get_list_of_singletons_from_query(self, query)


def spatial_tables(self, schema: str = None) -> list:
    """
    - Get a list of all SPATIAL tables in the db
    """

    query = """
        SELECT concat(f_table_schema, '.', f_table_name )
        FROM geometry_columns
    """

    if schema:
        query += f" WHERE f_table_schema = '{schema}'"

    return get_list_of_singletons_from_query(self, query)


def tables(self, spatial_only: bool = False, schema: Union[str, None] = None) -> list:
    """
    - Return a list of tables in the database
    - Set `spatial_only=True` if you only want a list of geotables
    """
    if spatial_only:
        return spatial_tables(self, schema=schema)
    else:
        return all_tables(self, schema=schema)


def schemas(self) -> list:
    """
    - Get a list of all schemas in the db
    """

    query = """
        SELECT schema_name
        FROM information_schema.schemata;
    """

    return get_list_of_singletons_from_query(self, query)


def columns(self, tablename: str) -> list:
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

    return get_list_of_singletons_from_query(self, query)
