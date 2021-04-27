from __future__ import annotations
import psycopg2
import subprocess
from typing import Union
from pathlib import Path

import pandas as pd
import geopandas as gpd
import sqlalchemy
from geoalchemy2 import Geometry, WKTElement

from datetime import datetime


def _sanitize_df_for_sql(
    df: Union[pd.DataFrame, gpd.GeoDataFrame]
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Clean up a dataframe column names so it imports into SQL properly.

    This includes:
        - spaces in column names replaced with underscore
        - all column names are 100% lowercase
        - funky characters are stripped out of column names
    """

    # Replace "Column Name" with "column_name"
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = [x.lower() for x in df.columns]

    # Remove '.' and '-' from column names.
    # i.e. 'geo.display-label' becomes 'geodisplaylabel'
    for s in [".", "-", "(", ")", "+"]:
        df.columns = df.columns.str.replace(s, "", regex=False)

    return df


def _timestamp_for_filepath(dt: datetime = None) -> str:
    """
    Make a datetime string formatted like: 'on_2021_02_09_at_09_29_58'
    """
    if not dt:
        dt = datetime.now()

    return dt.strftime("on_%Y_%m_%d_at_%H_%M_%S")


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
        port: int = 5432,
        pg_dump_path: str = None,
    ):
        """
        Create a URI and super URI that get used across the class.
        """

        self._uri = f"postgresql://{un}:{pw}@{host}:{port}/{db_name}"

        self._super_uri = f"postgresql://{super_un}:{super_pw}@{host}:{port}/{super_db}"

        self.pg_dump = "pg_dump" if not pg_dump_path else pg_dump_path

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

    # BACKUP

    def backup_to_sql_file(self, output_folder: Path) -> Path:
        """
        Create a standalone text file backup of the entire database.
        Returns the full filepath to the newly created file.
        """

        filename = f"{self.params['db_name']}_{_timestamp_for_filepath()}.sql"
        output_filepath = output_folder / filename

        command = f'{self.pg_dump} --no-owner --no-acl {self.uri()} > "{output_filepath}"'

        print(command)

        run_command_in_shell(command)

        return output_filepath

    # COPY BETWEEN DATABASES

    def copy_table_to_another_db(self, table_to_copy: str, target_db: Database) -> None:
        """
        Pipe data directly from a pg_dump of one DB into another using psql
        """

        # If the table_to_copy has a schema, ensure that the schema also exists in the target db
        if "." in table_to_copy:
            schema = table_to_copy.split(".")[0]
            target_db.add_schema(schema)

        command = f"{self.pg_dump} --no-owner --no-acl -t {table_to_copy} {self.uri()} | psql {target_db.uri()}"

        print(command)
        run_command_in_shell(command)

        target_db.ensure_geometry_is_named_geom(table_to_copy)

        return None

    def copy_entire_db_to_another_db(self, target_db: Database) -> None:
        """
        Copy an entire database to a new database.

        To get around memory error limitations, this is done in two steps as opposed to a single command with a pipe:
            Step 1) Backup the source db to .sql file with pg_dump
            Step 2) Load the .sql file into the target db with psql
        """

        if target_db.exists():
            print(
                f"A database named '{target_db.params['db_name']}' already exists. Use a different name or drop this database before copying into it."
            )
            return None

        target_db.create_db()

        sql_filepath = self.backup_to_sql_file(Path.cwd())

        command = f'psql -f  "{sql_filepath}" {target_db.uri()}'
        print(command)
        run_command_in_shell(command)

        # Ensure that spatial tables have 'geom' instead of 'shape' columns
        for table in target_db.spatial_table_list():
            target_db.ensure_geometry_is_named_geom(table)

        # Delete the .sql file from disk
        sql_filepath.unlink()

        return None

    # SHAPEFILE I/O

    def pgsql2shp(self, table_or_sql: str, output_filepath: Path) -> None:
        """
        Use pgsql2shp to export a shapefile from the database.

        Valid arguments for `table_or_sql` are the name of a table or a full query.
        e.g. "pa.centerlines"
             "SELECT * FROM pa.centerlines WHERE some_column = 'some value'"
        """

        print("WARNING! pgsql2shp creates shapefiles that do not contain EPSG values")
        print("As an alternative that preserves EPSG values, use Database.ogr2ogr_export() instead")

        if "select" in table_or_sql.lower():
            query = table_or_sql
        else:
            query = f"SELECT * FROM {table_or_sql}"

        command = f'pgsql2shp -f "{output_filepath}" -h {self.params["host"]} -u {self.params["un"]} -P {self.params["pw"]} -p {self.params["port"]} {self.params["db_name"]} "{query}" '
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

        self.ensure_geometry_is_named_geom(sql_tablename)

    def ogr2ogr_export(self, table_or_sql: str, filepath: Path, filetype: str = "ESRI Shapefile"):
        """
        Use ogr2ogr to export a shapefile from the database.

        Valid arguments for `table_or_sql` are the name of a table or a full query.
        e.g. "pa.centerlines"
             "SELECT * FROM pa.centerlines WHERE some_column = 'some value'"
        """

        cmd = f'ogr2ogr -f "{filetype}" "{filepath}" PG:"host={self.params["host"]} user={self.params["un"]} password={self.params["pw"]} port={self.params["port"]} dbname={self.params["db_name"]}" '

        if "select" in table_or_sql.lower():
            sql = table_or_sql
            cmd += f' -sql "{sql}"'
        else:
            tablename = table_or_sql
            cmd += f" {tablename}"

        print(cmd)
        run_command_in_shell(cmd)

    # UPDATE DATA IN-PLACE

    def rename_column(self, old_colname: str, new_colname: str, tablename: str) -> None:
        """ Change a column name for a table in SQL """

        query = f"ALTER TABLE {tablename} RENAME {old_colname} TO {new_colname};"

        self.execute_via_psycopg2(query)

    def ensure_geometry_is_named_geom(self, tablename: str):
        """
        Rename the geometry column to 'geom' if it comes through as 'shape'
        """

        if "shape" in self.columns_in_table(tablename):
            self.rename_column("shape", "geom", tablename)

    # GET DATA OUT VIA QUERY

    def query(self, q: str, geo: bool = None, query_kwargs: dict = None) -> Query:
        """
        Run a query in the database and return a Query() object.

        Leave 'geo' as None if you want the query to figure out
        whether to return a geodataframe or regular dataframe.

        If you specifically want a dataframe, set geo=False
        Likewise, set geo=True if you definitely want a geodataframe

        If your spatial query uses a non-standard geom column (i.e. 'shape')
        you'll need to pass a dictionary to 'query_kwargs'. For example:
            >>> db = Database('my_db')
            >>> db.query('select geom as shape from circuittrails', query_kwargs={'geom_col': 'shape'})

        A more traditional query with 'geom' requires less typing:
            >>> db.query('select geom from circuittrails')
        """

        if query_kwargs:
            query = Query(self, q, **query_kwargs)
        else:
            query = Query(self, q)

        # Use the user's geo flag, if they provided one
        if geo is not None:
            geo_flag = geo

        # If none provided, try to guess if it's spatial
        else:
            geo_flag = query.is_spatial()

        # If spatial, return a geodataframe
        if geo_flag:
            query.get_gdf()
        # If not spatial, return a pandas dataframe
        else:
            query.get_df()

        return query

    # IMPORT PANDAS & GEOPANDAS

    def import_tabular_file(
        self,
        filepath: Path,
        sql_tablename: str,
        pd_read_kwargs: dict = {},
        df_import_kwargs: dict = {"index": False},
    ) -> None:

        # Determine if this is a CSV, XLS, or XLSX and use the appropriate pandas loader
        suffix = filepath.suffix.lower()

        if suffix == ".csv":
            df = pd.read_csv(filepath, **pd_read_kwargs)
        elif suffix in [".xlsx", ".xls"]:
            df = pd.read_excel(filepath, **pd_read_kwargs)
        else:
            print(
                f"File type: '{suffix}' is not supported. Check the official pandas documentation to see if this is a valid filetype."
            )
            return None

        self.import_dataframe(df, sql_tablename, df_import_kwargs)

    def import_dataframe(
        self, df: pd.DataFrame, sql_tablename: str, df_import_kwargs: dict = {}
    ) -> None:

        # Clean up column names
        df = _sanitize_df_for_sql(df)

        # Make sure the schema exists
        schema, table_name = _convert_full_tablename_to_parts(sql_tablename)
        self.add_schema(schema)

        # Write to database
        engine = sqlalchemy.create_engine(self.uri())

        df.to_sql(table_name, engine, schema=schema, **df_import_kwargs)

        engine.dispose()

    def import_geo_file(self, filepath: Path, sql_tablename: str, gpd_kwargs: dict = {}) -> None:

        # Read the data into a geodataframe
        gdf = gpd.read_file(filepath)

        # Drop null geometries
        gdf = gdf[gdf["geometry"].notnull()]

        self.import_geodataframe(gdf, sql_tablename, gpd_kwargs)

    def import_geodataframe(
        self, gdf: gpd.GeoDataFrame, sql_tablename: str, gpd_kwargs: dict = {}, uid_col: str = "uid"
    ) -> None:

        schema, table_name = _convert_full_tablename_to_parts(sql_tablename)

        gdf = gdf.copy()

        gdf = _sanitize_df_for_sql(gdf)

        epsg_code = int(str(gdf.crs).split(":")[1])

        # Get a list of all geometry types in the dataframe
        geom_types = list(gdf.geometry.geom_type.unique())

        # If there are multi- and single-part features, explode to singlepart
        if len(geom_types) > 1:
            # Explode multipart to singlepart and reset the index
            gdf = gdf.explode()
            gdf["explode"] = gdf.index
            gdf = gdf.reset_index()

        # Use the non-multi version of the geometry
        geom_type_to_use = min(geom_types, key=len).upper()

        # Replace the 'geom' column with 'geometry'
        if "geom" in gdf.columns:
            gdf["geometry"] = gdf["geom"]
            gdf.drop("geom", 1, inplace=True)

        # Drop the 'gid' column
        if "gid" in gdf.columns:
            gdf.drop("gid", 1, inplace=True)

        # Rename 'uid' to 'old_uid'
        if uid_col in gdf.columns:
            gdf[f"old_{uid_col}"] = gdf[uid_col]
            gdf.drop(uid_col, 1, inplace=True)

        # Build a 'geom' column using geoalchemy2
        # and drop the source 'geometry' column
        gdf["geom"] = gdf["geometry"].apply(lambda x: WKTElement(x.wkt, srid=epsg_code))
        gdf.drop("geometry", 1, inplace=True)

        # Write geodataframe to SQL database
        self.add_schema(schema)

        engine = sqlalchemy.create_engine(self.uri())
        gdf.to_sql(
            table_name,
            engine,
            schema=schema,
            dtype={"geom": Geometry(geom_type_to_use, srid=epsg_code)},
            **gpd_kwargs,
        )
        engine.dispose()

        self.add_uid_column_to_table(sql_tablename)
        self.add_spatial_index_to_table(sql_tablename)

    # CANNED QUERIES THAT GET USED A LOT

    def add_uid_column_to_table(self, sql_tablename: str, uid_col: str = "uid"):
        """
        Add a unique ID column to a table as a SERIAL PRIMARY KEY type
        """

        sql_unique_id_column = f"""
            ALTER TABLE {sql_tablename} DROP COLUMN IF EXISTS {uid_col};
            ALTER TABLE {sql_tablename} ADD {uid_col} serial PRIMARY KEY;
        """
        self.execute_via_psycopg2(sql_unique_id_column)

    def add_spatial_index_to_table(self, sql_tablename: str):
        """
        Add a spatial index on a table's geom column
        """
        sql_make_spatial_index = f"""
            CREATE INDEX ON {sql_tablename}
            USING GIST (geom);
        """
        self.execute_via_psycopg2(sql_make_spatial_index)

    def project_spatial_table(
        self,
        sql_tablename: str,
        old_epsg: Union[int, str],
        new_epsg: Union[int, str],
        geom_type: str,
    ) -> None:
        """
        Transform a table in-place from old_epsg to new_epsg.

        You can use this with identical old and new epsgs to force an
        entry into the geometry_columns table.
        (Helpful for making geotables directly in the DB via query)
        """

        sql_transform_geom = f"""
            ALTER TABLE {sql_tablename}
            ALTER COLUMN geom TYPE geometry({geom_type}, {new_epsg})
            USING ST_Transform(ST_SetSRID(geom, {old_epsg}), {new_epsg});
        """
        self.execute_via_psycopg2(sql_transform_geom)

    # EXECUTE A QUERY THAT CREATES A NEW GEOTABLE IN THE DB

    def make_geotable_from_query(
        self,
        query: str,
        new_table_name: str,
        geom_type: str,
        epsg: int,
        uid_col: str = "uid",
    ) -> None:
        """
        Allow the creation of a new table in the db directly via query.

        This is especially helpful when you're working with a large dataset
        and you want to limit the I/O processing time.
        """

        schema, tbl = _convert_full_tablename_to_parts(new_table_name)

        valid_geom_types = [
            "POINT",
            "MULTIPOINT",
            "POLYGON",
            "MULTIPOLYGON",
            "LINESTRING",
            "MULTILINESTRING",
        ]

        if geom_type.upper() not in valid_geom_types:
            for msg in [
                f"Geometry type of {geom_type} is not valid.",
                f"Please use one of the following: {valid_geom_types}",
                "Aborting",
            ]:
                print("\t", msg)
            return

        sql_make_table_from_query = f"""
            DROP TABLE IF EXISTS {new_table_name};
            CREATE TABLE {new_table_name} AS
            {query}
        """

        self.add_schema(schema)

        self.execute_via_psycopg2(sql_make_table_from_query)

        self.add_uid_column_to_table(new_table_name, uid_col=uid_col)
        self.add_spatial_index_to_table(new_table_name)

        # We're not reprojecting here, but rather forcing an entry for
        # the new geo table into the geometry_columns table
        self.project_spatial_table(new_table_name, epsg, epsg, geom_type=geom_type.upper())

    # REPORTS

    def report_spatial(self, print_output: bool = False) -> dict:
        query = "select concat(f_table_schema, '.', f_table_name), srid, type from geometry_columns"

        results = self.query_via_psycopg2(query)

        output = {}

        for row in results:
            tbl, epsg, geom_type = row

            if epsg not in output:
                output[epsg] = {}

            if geom_type not in output[epsg]:
                output[epsg][geom_type] = []

            output[epsg][geom_type].append(tbl)

        print("-" * 80)
        print(f"Spatial Data Report for DB: {self.uri()}")

        epsg_list = [x for x in output.keys()]
        if len(epsg_list) < 2:
            print(f"\t-> All data is stored in {epsg_list[0]}")
        else:
            print(f"\t-> Data is stored in {len(epsg_list)} projections: {epsg_list}")

        for k in output.keys():
            print(f"\t-> EPSG: {k}")
            for geom_type in output[k]:
                print(f"\t\t->{geom_type}")
                for tbl in output[k][geom_type]:
                    print(f"\t\t\t-> {tbl}")

        return output

    def get_projection(self, tablename: str) -> int:
        """
        - Get the projection of a spatial table

        Args:
            tablename (str): name of table to check, optionally with schema prefix

        Returns:
            EPSG of table
        """
        schema, tbl = _convert_full_tablename_to_parts(tablename)

        query = f"""
            select srid
            from geometry_columns
            where f_table_schema = '{schema}'
            and f_table_name = '{tbl}'
        """

        result = self.query_via_psycopg2(query)
        data_epsg = result[0][0]

        return data_epsg

    def check_projection(self, tablename: str, epsg: int) -> bool:
        """
        - Check a table to see if the projection matches an expected value

        Args:
            tablename (str): name of table to check, optionally with schema prefix

        Returns:
            `True` or `False`, depending on whether the `epsg` matches the table
        """

        return self.get_projection(tablename) == epsg


class Query:
    def __init__(self, db: Database, q: str, geom_col: str = "geom"):
        self.db = db
        self.q = q
        self.geom_col = geom_col
        self.df = None
        self.gdf = None
        self.runtime = None

    def is_spatial(self) -> bool:
        """
        Guess if the query is spatial by looking at the 'FROM tablename' portion of the query.

        If the tablename is in the list of spatial tables, it will fetch a geodataframe of the query.

        If the initial 'FROM tablename' is not spatial, it will fetch a pandas dataframe of the query.
        """

        # Replace all occuranges of '\n' and '\t' in the query with a space
        q = self.q.replace("\n", " ").replace("\t", " ")

        # Turn query into a list and remove all empty values
        # i.e. from this:
        #           ['', 'select', '', '', '', '', '*', 'from', '', '', '', '', 'my_table']
        #      to this:
        #           ['select', '*', 'from', 'my_table']

        query_as_list = q.lower().split(" ")
        query_as_list = [x for x in query_as_list if x != ""]

        # Isolate the text after "from" in the query
        from_idx = query_as_list.index("from")
        selected_table = query_as_list[from_idx + 1]

        # Make sure that schema-less tables are prefixed with "public"
        if "." not in selected_table:
            selected_table = f"public.{selected_table}"

        # Return True if the FROM table is spatial
        if selected_table in self.db.spatial_table_list():
            return True
        else:
            return False

    def get_df(self) -> pd.Dataframe:
        """ Get a pandas Dataframe from the query """

        start_time = datetime.now()

        engine = sqlalchemy.create_engine(self.db.uri())

        self.df = pd.read_sql(self.q, engine)

        engine.dispose()

        end_time = datetime.now()
        self.runtime = str(end_time - start_time)

        return self.df

    def get_gdf(self):
        """ Get a geopandas GeoDataFrame from the query """

        start_time = datetime.now()

        connection = psycopg2.connect(self.db.uri())

        self.gdf = gpd.GeoDataFrame.from_postgis(self.q, connection, geom_col=self.geom_col)

        connection.close()

        end_time = datetime.now()
        self.runtime = str(end_time - start_time)

        return self.gdf
