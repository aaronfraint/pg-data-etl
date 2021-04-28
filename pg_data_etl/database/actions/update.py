def rename_column(db, old_colname: str, new_colname: str, tablename: str) -> None:
    """ Change a column name for a table in SQL """

    query = f"ALTER TABLE {tablename} RENAME {old_colname} TO {new_colname};"

    db.execute(query)


def add_uid_column_to_table(db, sql_tablename: str, uid_col: str = "uid"):
    """
    Add a unique ID column to a table as a SERIAL PRIMARY KEY type
    """

    sql_unique_id_column = f"""
        ALTER TABLE {sql_tablename} DROP COLUMN IF EXISTS {uid_col};
        ALTER TABLE {sql_tablename} ADD {uid_col} serial PRIMARY KEY;
    """
    db.execute(sql_unique_id_column)
