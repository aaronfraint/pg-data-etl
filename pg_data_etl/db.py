from __future__ import annotations
import psycopg2
import subprocess


def run_command_in_shell(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

    output = process.communicate()
    print(output[0])


def get_output_via_psycopg2(uri: str, query: str):
    connection = psycopg2.connect(uri)
    cursor = connection.cursor()

    cursor.execute(query)

    result = cursor.fetchall()
    cursor.close()
    connection.close()

    return result


class Database:
    def __init__(
        self,
        db_name: str,
        host: str,
        un: str,
        pw: str,
        super_un: str,
        super_pw: str,
        super_db: str = "postgres",
        port: int = 5423,
    ):
        """
        Create a URI and super URI that get used across the class.
        """

        self.uri = f"postgresql://{un}:{pw}@{host}:{port}/{db_name}"

        self.super_uri = f"postgresql://{super_un}:{super_pw}@{host}:{port}/{super_db}"

        self.params = {
            "un": un,
            "pw": pw,
            "host": host,
            "db_name": db_name,
            "port": port,
            "super_un": super_un,
            "super_pw": super_pw,
            "super_db": super_db,
        }

    def execute_query(self, query: str, super_uri: bool = False) -> list:
        """
        Use psycopg2 to execute a query and return the output as a list of lists
        """

        if super_uri:
            uri = self.super_uri
        else:
            uri = self.uri

        result = get_output_via_psycopg2(uri, query)

        return [list(x) for x in result]

    def exists(self) -> bool:
        """
        True or False: does this database exist already?
        """
        query = f"""
           SELECT EXISTS(
                SELECT datname FROM pg_catalog.pg_database
                WHERE lower(datname) = lower('{self.params["db_name"]}')
            );
        """

        x = self.execute_query(query, super_uri=True)[0][0]
        # print(x)

        return x

    def create_db(self) -> None:
        """
        Create the database if it doesn't exist yet
        """

        if not self.exists():
            command = f'psql -c "CREATE DATABASE {self.params["db_name"]};" {self.super_uri}'

            run_command_in_shell(command)

    def drop_db(self) -> None:
        """
        Drop the database if it exists.
        """

        if self.exists():
            command = f'psql -c "DROP DATABASE {self.params["db_name"]};" {self.super_uri}'
            run_command_in_shell(command)

        print(self.exists())

    def all_tables_in_db(self):
        query = """
            SELECT table_name
            FROM information_schema.tables
        """

        tables = self.execute_query(query)
        print([x[0] for x in tables])
        return [x[0] for x in tables]

    def copy_table_to_another_db(self, table_to_copy: str, other_db: Database):
        """
        Pipe data directly from one DB to another using:
            pg_dump | psql
        """
        command = f"pg_dump -t {table_to_copy} {self.uri} | psql {other_db.uri}"
        run_command_in_shell(command)