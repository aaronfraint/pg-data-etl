from __future__ import annotations


def decode_uri(uri: str) -> dict:
    """
    - Turn a URI into a dictionary of parameters
    - e.g. `"postgresql://postgres:password@localhost:5432/name_of_db"`
    becomes:
        {'db_name': 'name_of_db',
        'host': 'localhost',
        'un': 'postgres',
        'pw': 'password',
        'port': '5432',
        'extras': None}

    Args:
        uri (str): database connection string

    Returns:
        dictionary with individual parameters
    """

    psql_un_pw, host_port_db = uri.split("@")

    # Get username and password
    _, un_pw = psql_un_pw.split(r"//")
    un, pw = un_pw.split(":")

    # Get host, port, database name
    host, port_db = host_port_db.split(":")
    port, db_plus = port_db.split(r"/")

    if "?" in db_plus:
        db, extras = db_plus.split("?")
    else:
        db = db_plus
        extras = None

    return {
        "db_name": db,
        "host": host,
        "un": un,
        "pw": pw,
        "port": port,
        "extras": extras,
    }


def generate_uri(
    db_name: str,
    host: str = "localhost",
    un: str = "postgres",
    pw: str = "",
    port: int = 5432,
    extras: str | None = None,
) -> str:
    """
    - Turn individual connection parameters into a URI

    Args:
        db_name (str): name of database
        host (str): name of host
        un (str): username
        pw (str): password
        port (int): port
        extras (str): optional arguments at the end of the connection string

    Returns:
        database connection string
    """
    uri = f"postgresql://{un}:{pw}@{host}:{port}/{db_name}"

    if extras:
        uri += f"?{extras}"

    return uri
