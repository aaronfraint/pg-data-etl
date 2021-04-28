import psycopg2


def execute_via_psycopg2(db, query: str, super_uri: bool = False) -> None:
    """ Use psycopg2 to execute a query & commit it to the database """

    connection = psycopg2.connect(db.uri(super_uri=super_uri))
    cursor = connection.cursor()

    cursor.execute(query)

    cursor.close()
    connection.commit()
    connection.close()

    return None
