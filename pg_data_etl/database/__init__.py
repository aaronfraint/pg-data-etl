"""
`pg_data_etl.database`
----------------------



"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
import geopandas as gpd

from pg_data_etl import helpers

from . import actions


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

    def __init__(self, psql_bin: str | None = None, **kwargs):
        """
        - Save all initial keyword arguments as private variables
            - i.e. "host" argument becomes accessible as `Database._host`

        - Accept a flexible set of kwargs, depending on whether the instance
        is created `from_parameters()` or `from_uri()`

        """

        self._psql_bin = psql_bin
        self._init_kwargs = kwargs

        # Save all kwargs as private variables
        for key, value in kwargs.items():
            setattr(self, f"_{key}", value)

        # Record how the instance was created
        if hasattr(self, "_uri"):
            self.CREATED_BY_URI = True
        else:
            self.CREATED_BY_URI = False

    # Properties
    # ----------

    def _add_bin_path_if_exists(self, cmd):
        if not self._psql_bin:
            return cmd
        else:
            return Path(self._psql_bin) / cmd

    @property
    def psql(self):
        return self._add_bin_path_if_exists("psql")

    @property
    def pg_dump(self):
        return self._add_bin_path_if_exists("pg_dump")

    @property
    def shp2pgsql(self):
        return self._add_bin_path_if_exists("shp2pgsql")

    @property
    def pgsql2shp(self):
        return self._add_bin_path_if_exists("pgsql2shp")

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

    # Administration
    # --------------

    from .actions.query.simple import exists

    from .actions.admin import admin, add_schema

    # Change Things Within Database
    # -----------------------------

    from .actions.query.execute import execute

    from .actions.query.update import add_uid_column_to_table, rename_column

    from .actions.query.update_geo import (
        make_geotable_from_query,
        update_spatial_data_projection,
        lint_geom_colname,
        add_spatial_index_to_table,
    )

    # Lists of Content
    # ----------------

    from .actions.query.lists import tables, schemas, columns

    # Get Data Out of Database To Memory
    # ----------------------------------

    from .actions.query.data_spatial import gdf
    from .actions.query.data_nonspatial import df

    # Get Data Out of Database To File
    # --------------------------------

    from .actions.backup import dump

    from .actions.copy import copy_entire_db_to_another_db, copy_table_to_another_db

    from .actions.data_export import export_gis

    # Put Files Into Database
    # -----------------------

    from .actions.import_tabular_data import import_file_with_pandas
    from .actions.import_geo_data import import_gis

    # Put In-Memory Data Into Database
    # --------------------------------

    from .actions.import_tabular_data import import_dataframe
    from .actions.import_geo_data import import_geodataframe