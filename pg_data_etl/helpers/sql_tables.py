from __future__ import annotations

from pandas import DataFrame
from geopandas import GeoDataFrame


def sanitize_df_for_sql(df: DataFrame | GeoDataFrame) -> DataFrame | GeoDataFrame:
    """
    Clean up a dataframe column names so it imports into SQL properly.

    This includes:
        - spaces in column names replaced with underscore
        - all column names are 100% lowercase
        - funky characters are stripped out of column names

    TODO: docstring
    """

    bad_characters = [".", "-", "(", ")", "+", ":", "$"]

    # Replace "Column Name" with "column_name"
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = [x.lower() for x in df.columns]

    # Remove '.' and '-' from column names.
    # i.e. 'geo.display-label' becomes 'geodisplaylabel'
    for s in bad_characters:
        df.columns = df.columns.str.replace(s, "", regex=False)

    return df


def convert_full_tablename_to_parts(tablename: str) -> tuple:
    """
    Take in a table name and return a tuple with (schema, name)

    e.g.  'my_schema.my_table'  -> ('my_schema', 'my_table')
          'my_table'            -> ('public', 'my_table')

    Arguments:
        tablename (str): name of the table, with or without a schema

    Returns:
        tuple: with two values - (schema, table)
    """
    if "." not in tablename:
        schema = "public"
        tbl = tablename
    else:
        schema, tbl = tablename.split(".")

    return (schema, tbl)


def this_is_raw_sql(table_or_sql: str) -> bool:
    """
    - Determine if a text string is a query or a table name
    - This involves converting the text to lower case and seeing
    if 'select' and 'from' both appear in the text

    Arguments:
        table_or_sql (str): text to test

    Returns:
        bool: True or False, depending on if it's a query
    """
    text = table_or_sql.lower()

    return "select" in text and "from" in text
