from pg_data_etl import helpers


def create_database(self) -> None:
    """
    - Create the database if it doesn't exist yet, via `psql`
    """

    if not self.exists():

        db_name = self.connection_params["db_name"]

        # Create the database
        commands = [
            f'{self.cmd.psql} -c "CREATE DATABASE {db_name};" {self.uri_superuser}',
            f'{self.cmd.psql} -c "CREATE EXTENSION postgis;" {self.uri}',
        ]

        for cmd in commands:
            helpers.run_command_in_shell(cmd)


def drop_database(self) -> None:
    """
    - Drop the database if it exists, via `psql`
    """

    if self.exists():
        db_name = self.connection_params["db_name"]

        command = f'{self.cmd.psql} -c "DROP DATABASE {db_name};" {self.uri_superuser}'
        helpers.run_command_in_shell(command)


def admin(self, admin_action: str) -> None:
    """
    - Allow user to `"CREATE"` or `"DROP"` the database
    """
    admin_action = admin_action.upper()

    # Check that admin_action is allowed
    options = ["CREATE", "DROP"]
    if admin_action not in options:
        print(
            f"{admin_action=} is not supported\nAvailable administration options include: {options}"
        )
        return None

    if admin_action == "CREATE":
        create_database(self)

    if admin_action == "DROP":
        drop_database(self)


def schema_add(self, schema: str) -> None:
    """
    - Create a schema if it does not yet exist
    """

    query = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    self.execute(query)
