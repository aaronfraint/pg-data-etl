import psycopg2


def query_as_list_of_lists(self, query: str, super_uri: bool = False) -> list:
    """
    - Use `psycopg2` to run a query and return the result as a list of lists
    - This will NOT commit any changes to the database
    """

    if super_uri:
        uri = self.uri_superuser
    else:
        uri = self.uri

    connection = psycopg2.connect(uri)
    cursor = connection.cursor()

    cursor.execute(query)

    result = cursor.fetchall()

    cursor.close()
    connection.close()

    return [list(x) for x in result]


def query_as_list_of_singletons(self, query: str, super_uri: bool = False):
    """
    - Run a query where the expected output is a list of values
    """

    result = self.query_as_list_of_lists(query, super_uri=super_uri)

    return [x[0] for x in result]


def query_as_singleton(self, query: str, super_uri: bool = False):
    """
    - Run a query where the expected output is a single value
    """

    result = self.query_as_list_of_singletons(query, super_uri=super_uri)

    return result[0]


def exists(self) -> bool:
    """
    - True or False: does this database exist already?
    """

    db_name = self.connection_params["db_name"]

    query = f"""
        SELECT EXISTS(
            SELECT datname FROM pg_catalog.pg_database
            WHERE lower(datname) = lower('{db_name}')
        );
    """

    return self.query_as_singleton(query, super_uri=True)
