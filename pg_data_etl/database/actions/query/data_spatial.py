import psycopg2
import geopandas as gpd


def gdf(self, query: str, geom_col: str = "geom") -> gpd.GeoDataFrame:
    """
    - Get a `geopandas.GeoDataFrame` from a SQL query

    Arguments:
        query (str): `PostGIS` query as a string
        geom_col (str): geometry column name in the query. Usually `'geom'` or `'shape'`

    Returns:
        gpd.GeoDataFrame: query output as GIS data
    """

    connection = psycopg2.connect(self.uri)

    gdf = gpd.GeoDataFrame.from_postgis(query, connection, geom_col=geom_col)

    connection.close()

    return gdf
