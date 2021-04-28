from __future__ import annotations

from pg_data_etl import helpers, actions


class Database:
    """
    The `Database` class encapsulates the PostgreSQL connection and
    all necessary functionality. It can be created in two ways:

    - Option 1: pass a database connection string (i.e. 'uri')

            >>> uri = 'postgresql://postgres:password@localhost:5432/name_of_db'
            >>> db = Database.from_uri(uri)

    - Option 2: pass a keyword-argument dictionary with the connection parameters

            >>> creds = {'db_name': 'my_db', 'un': 'my_username', pw='my_password'}
            >>> db = Database.from_parameters(**creds)


    No matter how it was created, you can access the connection parameters and URI:

    - Get the database URI with `db.uri`

            >>> db.uri
            'postgresql://postgres:password@localhost:5432/name_of_db'

    - Get the database connection parameters with `db.connection_params`

            >>> db.connection_params
            {'db_name': 'name_of_db', 'host': 'localhost', 'un': 'postgres',
            'pw': 'password', 'port': '5432', 'extras': None}

    ---

    """

    def __init__(self, psql_path: str | None = None, **kwargs):
        """
        - Save all initial keyword arguments as private variables
            - i.e. "host" argument becomes accessible as `Database._host`

        - Accept a flexible set of kwargs, depending on whether the instance
        is created `from_parameters()` or `from_uri()`

        """

        self._psql_path = psql_path
        self._init_kwargs = kwargs

        # Save all kwargs as private variables
        for key, value in kwargs.items():
            setattr(self, f"_{key}", value)

        # Record how the instance was created
        if self._uri:
            self.CREATED_BY_URI = True
        else:
            self.CREATED_BY_URI = False

    @property
    def connection_params(self) -> dict:
        """
        - Return a `dict` of the connection parameters
        """
        if self.CREATED_BY_URI:
            return helpers.decode_uri(self._uri)

        else:
            return {
                "db_name": self._db_name,
                "host": self._host,
                "un": self._un,
                "pw": self._pw,
                "port": self._port,
                "extras": self._extras,
            }

    @property
    def uri(self) -> str:
        """
        - Return a connection string
        """
        if self.CREATED_BY_URI:
            return self._uri
        else:
            return helpers.generate_uri(
                db_name=self._db_name,
                host=self._host,
                un=self._un,
                pw=self._pw,
                port=self._port,
                extras=self._extras,
            )

    @property
    def uri_superuser(self, super_db: str = "postgres"):
        """
        - Return a connection string for the super-user / super-database
        - This is only needed to create the database from within Python
        """

        if self.CREATED_BY_URI:
            params = helpers.decode_uri(self._uri)
            params["db_name"] = super_db
            return helpers.generate_uri(**params)

        else:
            return helpers.generate_uri(
                db_name=self._super_db,
                host=self._host,
                un=self._super_un,
                pw=self._super_pw,
                port=self._port,
            )

    @classmethod
    def from_parameters(
        cls,
        db_name: str,
        host: str = "localhost",
        un: str = "postgres",
        pw: str = "",
        port: int = 5432,
        super_un: str | None = None,
        super_pw: str | None = None,
        super_db: str = "postgres",
        extras: str | None = None,
    ) -> Database:
        """
        - Build a `Database` from keyword arguments

        - Super username/password are necessary to create a database. Unless
        explicitly declared, these values are assumed to be the same as the
        provided username/password

        """

        if not super_un:
            super_un = un
        if not super_pw:
            super_pw = pw

        return cls(
            db_name=db_name,
            host=host,
            un=un,
            pw=pw,
            port=port,
            super_un=super_un,
            super_pw=super_pw,
            super_db=super_db,
            extras=extras,
        )

    @classmethod
    def from_uri(cls, uri: str) -> Database:
        """
        - Build a `Database` through its URI
        """
        return cls(uri=uri)

    def exists(self) -> bool:
        """
        - Return `True` or `False` depending on whether the database exists or not
        """
        return actions.does_database_exist(self)

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
            actions.create_database(self)

        if admin_action == "DROP":
            actions.drop_database(self)

    def list_of_tables(self, spatial_only: bool = False, schema: str | None = None) -> list:
        """
        - Return a list of tables in the database
        - Set `spatial_only=True` if you only want a list of geotables
        """
        if spatial_only:
            return actions.list_of_spatial_tables(self, schema=schema)
        else:
            return actions.list_of_all_tables(self, schema=schema)

    def list_of_schema(self) -> list:
        """
        - Return a list of all schemas in the database
        """
        return actions.list_of_schemas(self)

    def list_of_columns_in(self, tablename: str) -> list:
        """
        - Return a list of all columns in a table
        - Tablename can by `'my_table' or `'my_schema.my_table'`
        - Tables without a schema are assumed to be within the `public` schema
        """
        return actions.list_of_columns_in_table(tablename)