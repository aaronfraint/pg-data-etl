from typing import Union
from pandas import DataFrame
from geopandas import GeoDataFrame


def _sanitize_df_for_sql(df: Union[DataFrame, GeoDataFrame]) -> Union[DataFrame, GeoDataFrame]:
    """
    Clean up a dataframe column names so it imports into SQL properly.

    This includes:
        - spaces in column names replaced with underscore
        - all column names are 100% lowercase
        - funky characters are stripped out of column names
    """

    # Replace "Column Name" with "column_name"
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = [x.lower() for x in df.columns]

    # Remove '.' and '-' from column names.
    # i.e. 'geo.display-label' becomes 'geodisplaylabel'
    for s in [".", "-", "(", ")", "+"]:
        df.columns = df.columns.str.replace(s, "", regex=False)

    return df


def _convert_full_tablename_to_parts(tablename: str) -> tuple:
    """
    Take in a table name and return a tuple with (schema, name)

    e.g.  'my_schema.my_table'  -> ('my_schema', 'my_table')
          'my_table'            -> ('public', 'my_table')
    """
    if "." not in tablename:
        schema = "public"
        tbl = tablename
    else:
        schema, tbl = tablename.split(".")

    return (schema, tbl)