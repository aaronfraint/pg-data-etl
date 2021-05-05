import psycopg2


def execute(self, query: str) -> None:
    """ Use psycopg2 to execute a query & commit it to the database """

    connection = psycopg2.connect(self.uri)
    cursor = connection.cursor()

    cursor.execute(query)

    cursor.close()
    connection.commit()
    connection.close()

    return None
