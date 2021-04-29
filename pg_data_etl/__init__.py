"""
`pg_data_etl`
-------------


"""
from typing import Union

import pg_data_etl.helpers as helpers

# import pg_data_etl.database.actions as actions

from .database import Database

from .settings.read_config_file import configurations as _configurations

config = _configurations()


def from_uri(uri: str) -> Database:
    """
    - Build a `Database` through its URI

    Example:

        >>> uri = 'postgresql://postgres:password@localhost:5432/name_of_db'
        >>> db = pg.from_uri(uri)
    """
    return Database.from_uri(uri=uri)


def from_config(db_name: str, config_key: str) -> Database:
    """
    - Build a `Database` with its name and a configuration key

    Examples:

        >>> db = pg.from_config('my_database', 'localhost')

        >>> db = pg.from_config('my_database', 'remote')

    Returns:
        database
    """
    return Database.from_parameters(db_name=db_name, **config[config_key])


def from_parameters(
    db_name: str,
    host: str = "localhost",
    un: str = "postgres",
    pw: str = "",
    port: int = 5432,
    super_un: Union[str, None] = None,
    super_pw: Union[str, None] = None,
    super_db: str = "postgres",
    extras: Union[str, None] = None,
) -> Database:
    """
    - Build a `Database` from keyword arguments

    - Super username/password are necessary to create a database. Unless
    explicitly declared, these values are assumed to be the same as the
    provided username/password
    """
    return Database.from_parameters(
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
