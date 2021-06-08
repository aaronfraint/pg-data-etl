"""
`pg_data_etl.database`
----------------------
"""

from __future__ import annotations
from pathlib import Path

from pg_data_etl import helpers
from pg_data_etl.settings import configurations


class Database:
    """
    The `Database` class encapsulates the PostgreSQL connection and
    all necessary functionality. It can be created in two ways:

    - Option 1: pass a database connection string (i.e. 'uri')

        ```python
        >>> uri = 'postgresql://postgres:password@localhost:5432/name_of_db'
        >>> db = Database.from_uri(uri)
        ```

    - Option 2: pass a keyword-argument dictionary with the connection parameters

        ```python
        >>> creds = {'db_name': 'my_db', 'un': 'my_username', pw='my_password'}
        >>> db = Database.from_parameters(**creds)
        ```

    - Option 3: use the built-in configuration manager

        ```python
        >>> db = Database.from_config('my_db', 'localhost')
        ```

    ---

    No matter how it was created, you can access the connection parameters and URI:

    - Get the database URI with `db.uri`

        ```python
        >>> db.uri
        'postgresql://postgres:password@localhost:5432/name_of_db'
        ```

    - Get the database connection parameters with `db.connection_params`

        ```python
        >>> db.connection_params
        {'db_name': 'name_of_db', 'host': 'localhost', 'un': 'postgres',
        'pw': 'password', 'port': '5432', 'extras': None}
        ```

    ---

    """

    def __init__(self, **kwargs):
        """
        - Save all initial keyword arguments as private variables
            - i.e. "host" argument becomes accessible as `Database._host`

        - Accept a flexible set of kwargs, depending on whether the instance
        is created `from_parameters()` or `from_uri()`

        """

        self._init_kwargs = kwargs

        # Save all kwargs as private variables
        for key, value in kwargs.items():
            setattr(self, f"_{key}", value)

        # Set up the CommandPath subclass
        if self._bin_paths:
            self.cmd = helpers.CommandPathManager(**self._bin_paths)
        else:
            self.cmd = helpers.CommandPathManager()

        # Record how the instance was created
        if hasattr(self, "_uri"):
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

    # Creation methods
    # ----------------

    @classmethod
    def from_parameters(
        cls,
        db_name: str,
        host: str = "localhost",
        un: str = "postgres",
        pw: str = "",
        port: int = 5432,
        super_db: str = "postgres",
        super_un: str | None = None,
        super_pw: str | None = None,
        extras: str | None = None,
        bin_paths: dict | None = None,
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
            bin_paths=bin_paths,
        )

    @classmethod
    def from_uri(
        cls,
        uri: str,
        bin_paths: dict | None = None,
    ) -> Database:
        """
        - Build a `Database` through its URI
        """
        return cls(uri=uri, bin_paths=bin_paths)

    @classmethod
    def from_config(
        cls,
        db_name: str,
        config_key: str,
        config_filepath: str | None = None,
        bin_paths: dict | None = None,
    ) -> Database:
        """
        - Build a `Database` with the configuration file support
        """
        if config_filepath:
            config = configurations(filepath=Path(config_filepath))
        else:
            config = configurations()

        return cls.from_parameters(db_name=db_name, bin_paths=bin_paths, **config[config_key])

    # Administration
    # --------------

    from .actions import exists, admin, schema_add, load_from_dumpfile

    # Change Things Within Database
    # -----------------------------

    from .actions import (
        execute,
        table_add_uid_column,
        table_rename_column,
        gis_make_geotable_from_query,
        gis_table_update_spatial_data_projection,
        gis_table_lint_geom_colname,
        gis_table_add_spatial_index,
    )

    # Lists of Content
    # ----------------

    from .actions import tables, schemas, columns, views

    # Get Data Out of Database To Memory
    # ----------------------------------

    from .actions import gdf, df
    from .actions import query_as_list_of_lists as query
    from .actions import (
        query_as_singleton,
        query_as_list_of_singletons,
        query_as_list_of_lists,
    )

    # Get Data Out of Database To File
    # --------------------------------

    from .actions import (
        dump,
        export_gis,
        export_entire_db_to_another_db,
        export_table_to_another_db,
    )

    # Put Files Into Database
    # -----------------------

    from .actions import import_gis, import_file_with_pandas

    # Put In-Memory Data Into Database
    # --------------------------------

    from .actions import import_dataframe, import_geodataframe
