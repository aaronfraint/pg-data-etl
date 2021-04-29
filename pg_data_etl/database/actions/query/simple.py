import psycopg2


def get_query_via_psycopg2(self, query: str, super_uri: bool = False) -> list:
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


def get_list_of_singletons_from_query(self, query: str, super_uri: bool = False):
    """
    - Run a query where the expected output is a list of values
    """

    result = get_query_via_psycopg2(self, query, super_uri=super_uri)

    return [x[0] for x in result]


def get_single_output_from_query(self, query: str, super_uri: bool = False):
    """
    - Run a query where the expected output is a single value
    """

    result = get_list_of_singletons_from_query(self, query, super_uri=super_uri)

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

    return get_single_output_from_query(self, query, super_uri=True)
