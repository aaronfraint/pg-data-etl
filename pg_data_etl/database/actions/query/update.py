def table_rename_column(self, old_colname: str, new_colname: str, tablename: str) -> None:
    """
    - Change a column name for a table in SQL
    """

    query = f"ALTER TABLE {tablename} RENAME {old_colname} TO {new_colname};"
    self.execute(query)


def table_add_uid_column(self, tablename: str, uid_col: str = "uid") -> None:
    """
    - Add a unique ID column to a table as a SERIAL PRIMARY KEY type
    """

    query = f"""
        ALTER TABLE {tablename}
        DROP COLUMN IF EXISTS {uid_col};
        
        ALTER TABLE {tablename}
        ADD {uid_col} serial PRIMARY KEY;
    """
    self.execute(query)
