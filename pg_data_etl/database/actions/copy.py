from pg_data_etl import helpers


def export_table_to_another_db(self, table_to_copy: str, target_db) -> None:
    """
    - Pipe data directly from a pg_dump of one DB into another using psql

    Arguments:
        table_to_copy (str): name of table that you want to copy
        target_db (Database): database where you want the data copied to

    Returns:
        None: although it makes a copy of the table inside `target_db`
    """

    # If the table_to_copy has a schema, ensure that the schema also exists in the target db
    if "." in table_to_copy:
        schema = table_to_copy.split(".")[0]
        target_db.schema_add(schema)

    pg_dump = self.cmd.pg_dump
    command = f"{pg_dump} --no-owner --no-acl -t {table_to_copy} {self.uri} | psql {target_db.uri}"

    print(command)
    helpers.run_command_in_shell(command)

    self.gis_table_lint_geom_colname(target_db, table_to_copy)

    return None


def export_entire_db_to_another_db(self, target_db) -> None:
    """
    - Copy an entire database to a new database.

    - To get around memory error limitations, this is done in two steps as opposed to a single
    command with a pipe:
        Step 1) Backup the source db to .sql file with pg_dump
        Step 2) Load the .sql file into the target db with psql

    Arguments:
        target_db (Database): new database (that doesn't exist yet) where you want the data

    Returns:
        None: although it makes a full copy the source database in `target_db`
    """

    if target_db.exists():
        target_db_name = target_db.connection_params["db_name"]
        print(f"A database named '{target_db_name}' already exists.")
        print("Use a different name or drop this database first before copying into it.")
        return None

    else:
        sql_filepath = self.dump()

        target_db.admin("CREATE")

        command = f'{target_db.cmd.psql} -f  "{sql_filepath}" {target_db.uri}'
        helpers.run_command_in_shell(command)

        # Ensure that spatial tables have 'geom' instead of 'shape' columns
        for table in target_db.tables(spatial_only=True):
            target_db.gis_table_lint_geom_colname(table)

        # Delete the .sql file from disk
        sql_filepath.unlink()

        return None
