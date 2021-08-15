from __future__ import annotations
from pathlib import Path
import pandas as pd
import sqlalchemy

from pg_data_etl import helpers


def import_file_with_pandas(
    self,
    filepath: Path | str,
    tablename: str,
    pd_read_kwargs: dict = {},
    df_import_kwargs: dict = {"index": False},
) -> None:
    """
    - Import a tabular CSV or XLSX file to postgres
    - Custom arguments can be provided for the reading of the file via `pd_read_kwargs`
    - Custom import arguments can be provided via `df_import_kwargs`

    Arguments:
        filepath (Path | str): Path or string of filepath to the source CSV or XLSX
        tablename (str): name the new table should be given in the database
        pd_read_kwargs (dict): a key/value dict with any special arguments needed to read the file
        df_import_kwargs (dict): a key/value dict with any special arguments needed to write the data to SQL

    Returns:
        creates a new SQL table from the specified file
    """

    # Determine if this is a CSV, XLS, or XLSX and use the appropriate pandas loader
    filepath = Path(filepath)
    suffix = filepath.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(filepath, **pd_read_kwargs)
    elif suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(filepath, **pd_read_kwargs)
    else:
        print(
            f"File type: '{suffix}' is not supported. Check the official pandas documentation to see if this is a valid filetype."
        )
        return None

    self.import_dataframe(df, tablename, df_import_kwargs)


def import_dataframe(self, df: pd.DataFrame, tablename: str, df_import_kwargs: dict = {}) -> None:
    """
    - Import an in-memort `pandas.DataFrame` into postgres

    Arguments:
        df (pd.DataFrame): data to load into postgres, as an in-memory dataframe
        tablename (str): name of the new table in SQL
        df_import_kwargs (dict): a key/value dict with any special arguments needed to write the data to SQL

    Returns:
        creates a new SQL table from the provided dataframe
    """

    # Clean up column names
    df = helpers.sanitize_df_for_sql(df)

    # Make sure the schema exists
    schema, tbl = helpers.convert_full_tablename_to_parts(tablename)
    self.schema_add(schema)

    # Write to database
    engine = sqlalchemy.create_engine(self.uri)

    df.to_sql(tbl, engine, schema=schema, **df_import_kwargs)

    engine.dispose()
