import sqlalchemy
from datetime import datetime
import pandas as pd
import psycopg2
import geopandas as gpd

from pg_data_etl import Database


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

        self.gdf = gpd.GeoDataFrame.from_postgis(
            self.q, connection, geom_col=self.geom_col
        )

        connection.close()

        end_time = datetime.now()
        self.runtime = str(end_time - start_time)

        return self.gdf
