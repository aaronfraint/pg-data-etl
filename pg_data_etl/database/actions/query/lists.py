from __future__ import annotations
from pg_data_etl import helpers


def tables(self, spatial_only: bool = False, schema: str | None = None) -> list:
    """
    - Return a list of tables in the database
    - Set `spatial_only=True` if you only want a list of geotables

    Arguments:
        spatial_only (bool): flag that will filter to spatial tables only if `True`
        schema (str | None): optional name of schema to filter results to

    Returns:
        list: with all tablenames, with each entry formatted as `schema.tablename`
    """
    if spatial_only:
        query = """
            SELECT concat(f_table_schema, '.', f_table_name )
            FROM geometry_columns
        """

        if schema:
            query += f" WHERE f_table_schema = '{schema}'"

    else:
        query = """
            SELECT concat(table_schema, '.', table_name )
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """

        if schema:
            query += f" AND table_schema = '{schema}'"

    return self.query_as_list_of_singletons(query)


def schemas(self) -> list:
    """
    - Get a list of all schemas in the db

    Returns:
        list: with all schema names within the database
    """

    query = """
        SELECT schema_name
        FROM information_schema.schemata;
    """

    return self.query_as_list_of_singletons(query)


def views(self) -> list:
    """
    - Get a list of all views in the db

    Returns:
        list: with the names of all views inside the database
    """

    query = """
        select concat(table_schema, '.', table_name)
        from information_schema.views
        where table_schema not in ('information_schema', 'pg_catalog')
    """

    return self.query_as_list_of_singletons(query)


def columns(self, tablename: str) -> list:
    """
    - Get a list of all column names in a given table

    Arguments:
        tablename (str): name of the table you're interested in

    Returns:
        list: with each entry being a column name within the table
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

    return self.query_as_list_of_singletons(query)
