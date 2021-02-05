from __future__ import annotations
import psycopg2
import subprocess


def run_command_in_shell(command: str) -> str:
    """Use subprocess to execute a command in a shell"""

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

    output = process.communicate()
    print(output[0])

    process.kill()

    return output


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

        self._uri = f"postgresql://{un}:{pw}@{host}:{port}/{db_name}"

        self._super_uri = f"postgresql://{super_un}:{super_pw}@{host}:{port}/{super_db}"

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

    def uri(self, super_uri: bool = False) -> str:
        """
        Return the normal URI by default.
        Return the super database URI if super_uri = True
        """

        if super_uri:
            return self._super_uri
        else:
            return self._uri

    def execute_via_psycopg2(self, query: str, super_uri: bool = False) -> None:
        """ Use psycopg2 to execute a query & commit it to the database """

        connection = psycopg2.connect(self.uri(super_uri=super_uri))
        cursor = connection.cursor()

        cursor.execute(query)

        cursor.close()
        connection.commit()
        connection.close()

        return None

    def query_via_psycopg2(self, query: str, super_uri: bool = False) -> list:
        """ Use psycopg2 to run a query and return the result as a list of lists """

        connection = psycopg2.connect(self.uri(super_uri=super_uri))
        cursor = connection.cursor()

        cursor.execute(query)

        result = cursor.fetchall()

        cursor.close()
        connection.close()

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

        x = self.query_via_psycopg2(query, super_uri=True)[0][0]

        return x

    def create_db(self) -> None:
        """
        Create the database if it doesn't exist yet
        """

        if not self.exists():

            # Create the database
            command = (
                f'psql -c "CREATE DATABASE {self.params["db_name"]};" {self.uri(super_uri=True)}'
            )
            run_command_in_shell(command)

            # Enable PostGIS
            command = f'psql -c "CREATE EXTENSION postgis;" {self.uri()}'
            run_command_in_shell(command)

    def drop_db(self) -> None:
        """
        Drop the database if it exists.
        """

        if self.exists():
            command = (
                f'psql -c "DROP DATABASE {self.params["db_name"]};" {self.uri(super_uri=True)}'
            )
            run_command_in_shell(command)

    def all_tables_in_db(self, schema: str = None) -> list:
        """
        Get a list of all tables in the db.
        Omit the behind-the-scenes tables.
        """
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """

        if schema:
            query += f" AND table_schema = '{schema}'"

        tables = self.query_via_psycopg2(query)

        return [x[0] for x in tables]

    def all_spatial_tables_in_db(self, schema: str = None) -> list:
        """
        Get a list of all SPATIAL tables in the db
        """

        query = """
            SELECT f_table_name
            FROM geometry_columns
        """

        if schema:
            query += f" WHERE f_table_schema = '{schema}'"

        geotables = self.query_via_psycopg2(query)

        return [x[0] for x in geotables]

    def all_schemas_in_db(self) -> list:
        """
        Get a list of all schemas in the db
        """

        query = """
            SELECT schema_name
            FROM information_schema.schemata;
        """

        schemas = self.query_via_psycopg2(query)

        return [x[0] for x in schemas]

    def copy_table_to_another_db(self, table_to_copy: str, other_db: Database) -> None:
        """
        Pipe data directly from a pg_dump of one DB into another using psql
        """

        # If the table_to_copy has a schema, ensure that the schema also exists in the target db
        if "." in table_to_copy:

            schema = table_to_copy.split(".")[0]

            if schema not in other_db.all_schemas_in_db():
                other_db.execute_via_psycopg2(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        command = f"pg_dump -t {table_to_copy} {self.uri()} | psql {other_db.uri()}"
        print(command)
        run_command_in_shell(command)

    def pgsql2shp(self, table_or_sql: str, output_filepath: str) -> None:
        """
        Use pgsql2shp to export a shapefile from the database.

        Valid arguments for `table_or_sql` are the name of a table or a full query.
        e.g. "pa.centerlines"
             "SELECT * FROM pa.centerlines WHERE some_column = 'some value'"
        """

        if "select" in table_or_sql.lower():
            query = table_or_sql
        else:
            query = f"SELECT * FROM {table_or_sql}"

        command = f'pgsql2shp -f "{output_filepath}" -h {self.params["host"]} -u {self.params["un"]} -P {self.params["pw"]} {self.params["db_name"]} "{query}" '
        print(command)

        run_command_in_shell(command)
