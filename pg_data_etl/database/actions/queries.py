import psycopg2


def query_via_psycopg2(db, query: str, super_uri: bool = False) -> list:
    """ Use psycopg2 to run a query and return the result as a list of lists """

    connection = psycopg2.connect(db.uri(super_uri=super_uri))
    cursor = connection.cursor()

    cursor.execute(query)

    result = cursor.fetchall()

    cursor.close()
    connection.close()

    return [list(x) for x in result]


def exists(db) -> bool:
    """
    True or False: does this database exist already?
    """
    query = f"""
        SELECT EXISTS(
            SELECT datname FROM pg_catalog.pg_database
            WHERE lower(datname) = lower('{db.params["db_name"]}')
        );
    """

    x = self.query_via_psycopg2(query, super_uri=True)[0][0]

    return x
