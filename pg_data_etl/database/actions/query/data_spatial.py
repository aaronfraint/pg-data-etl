import psycopg2
import geopandas as gpd


def gdf(self, query: str, geom_col: str = "geom") -> gpd.GeoDataFrame:
    """
    - Get a `geopandas.GeoDataFrame` from a `PostGIS` query
    """

    connection = psycopg2.connect(self.uri)

    gdf = gpd.GeoDataFrame.from_postgis(query, connection, geom_col=geom_col)

    connection.close()

    return gdf