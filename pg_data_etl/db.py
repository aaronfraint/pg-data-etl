from __future__ import annotations
import psycopg2
import subprocess

import pandas as pd
import geopandas as gpd
import sqlalchemy


def _convert_full_tablename_to_parts(tablename: str) -> tuple:
    """
    Take in a table name and return a tuple with (schema, name)

    e.g.  'my_schema.my_table'  -> ('my_schema', 'my_table')
          'my_table'            -> ('public', 'my_table')
    """
    if "." not in tablename:
        schema = "public"
        tbl = tablename
    else:
        schema, tbl = tablename.split(".")

    return (schema, tbl)


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

    # ACCESS

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

    # MANAGEMENT

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

    def add_schema(self, schema: str) -> None:
        """Add a schema if it does not yet exist """

        if schema not in self.schema_list():
            self.execute_via_psycopg2(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # LISTS OF THINGS

    def table_list(self, schema: str = None) -> list:
        """
        Get a list of all tables in the db.
        Omit the behind-the-scenes tables.
        """
        query = """
            SELECT concat(table_schema, '.', table_name )
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """

        if schema:
            query += f" AND table_schema = '{schema}'"

        tables = self.query_via_psycopg2(query)

        return [x[0] for x in tables]

    def spatial_table_list(self, schema: str = None) -> list:
        """
        Get a list of all SPATIAL tables in the db
        """

        query = """
            SELECT concat(f_table_schema, '.', f_table_name )
            FROM geometry_columns
        """

        if schema:
            query += f" WHERE f_table_schema = '{schema}'"

        geotables = self.query_via_psycopg2(query)

        return [x[0] for x in geotables]

    def schema_list(self) -> list:
        """
        Get a list of all schemas in the db
        """

        query = """
            SELECT schema_name
            FROM information_schema.schemata;
        """

        schemas = self.query_via_psycopg2(query)

        return [x[0] for x in schemas]

    def columns_in_table(self, tablename: str) -> list:
        """ Get a list of all column names in a given table. """

        schema, tbl = _convert_full_tablename_to_parts(tablename)

        query = f"""
            SELECT DISTINCT column_name
            FROM information_schema.columns
            WHERE
                table_name = '{tbl}'
              AND
                table_schema = '{schema}';
        """

        return [x[0] for x in self.query_via_psycopg2(query)]

    # COPY BETWEEN DATABASES

    def copy_table_to_another_db(self, table_to_copy: str, other_db: Database) -> None:
        """
        Pipe data directly from a pg_dump of one DB into another using psql
        """

        # If the table_to_copy has a schema, ensure that the schema also exists in the target db
        if "." in table_to_copy:
            schema = table_to_copy.split(".")[0]
            other_db.add_schema(schema)

        command = (
            f"pg_dump --no-owner --no-acl -t {table_to_copy} {self.uri()} | psql {other_db.uri()}"
        )
        print(command)
        run_command_in_shell(command)

        other_db.lint_geom_colname(table_to_copy)

    def copy_entire_db_to_another_db(self, other_db: Database) -> None:
        """
        Pipe an entire database from one location to another using pg_dump and psql
        """

        command = (
            f"pg_dump --no-owner --no-acl -t {table_to_copy} {self.uri()} | psql {other_db.uri()}"
        )
        print(command)
        run_command_in_shell(command)

        other_db.lint_geom_colname(table_to_copy)

    # SHAPEFILE I/O

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

    def shp2pgsql(self, shp_path: str, srid: int, sql_tablename: str, new_srid: int = None):
        """
        Use the shp2pgsql command to import a shapefile into the database
        """

        # Ensure that the schema provided in the 'sql_tablename' exists
        if "." in sql_tablename:
            schema = sql_tablename.split(".")[0]
            self.add_schema(schema)

        # If 'new_srid' is provided, use 'old:new' to project on the fly
        srid_arg = f"{srid}:{new_srid}" if new_srid else srid

        command = f'shp2pgsql -I -s {srid_arg} "{shp_path}" {sql_tablename} | psql {self.uri()}'

        print(command)

        run_command_in_shell(command)

        self.lint_geom_colname(sql_tablename)

    # UPDATE DATA IN-PLACE

    def rename_column(self, old_colname: str, new_colname: str, tablename: str) -> None:
        """ Change a column name for a table in SQL """

        query = f"ALTER TABLE {tablename} RENAME {old_colname} TO {new_colname};"

        self.execute_via_psycopg2(query)

    def lint_geom_colname(self, tablename: str):
        """
        Rename the geometry column to 'geom' if it comes through as 'shape'
        """

        if "shape" in self.columns_in_table(tablename):
            self.rename_column("shape", "geom", tablename)


class Query:
    def __init__(self, db: Database, q: str):
        self.db = db
        self.q = q
        self.df = None
        self.gdf = None

    def get_df(self) -> pd.Dataframe:
        """ Get a pandas Dataframe from the query """

        engine = sqlalchemy.create_engine(self.db.uri())

        self.df = pd.read_sql(self.q, engine)

        engine.dispose()

        return self.df

    def get_gdf(self, geom_col: str = "geom"):
        """ Get a geopandas GeoDataFrame from the query """

        connection = psycopg2.connect(self.db.uri())

        self.gdf = gpd.GeoDataFrame.from_postgis(self.q, connection, geom_col=geom_col)

        connection.close()

        return self.gdf
