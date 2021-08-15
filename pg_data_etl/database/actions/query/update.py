def table_rename_column(self, old_colname: str, new_colname: str, tablename: str) -> None:
    """
    - Change a column name for a table in SQL

    Arguments:
        old_colname (str): name of the original column
        new_colname (str): desired name of the column
        tablename (str): name of the table to update

    Returns:
        None: but updates the column name in-place
    """

    query = f"ALTER TABLE {tablename} RENAME {old_colname} TO {new_colname};"
    self.execute(query)


def table_add_uid_column(self, tablename: str, uid_col: str = "uid") -> None:
    """
    - Add a unique ID column to a table as a SERIAL PRIMARY KEY type
    - If the `uid_col` already exists, it's dropped before being recreated

    Arguments:
        tablename (str): name of the table to add a primary key column to
        uid_col (str): name of the new column, defaults to 'uid'

    Returns:
        None: but adds a primary key column to the table
    """

    query = f"""
        ALTER TABLE {tablename}
        DROP COLUMN IF EXISTS {uid_col};

        ALTER TABLE {tablename}
        ADD {uid_col} serial PRIMARY KEY;
    """
    self.execute(query)
