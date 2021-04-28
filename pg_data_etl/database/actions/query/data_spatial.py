import psycopg2
import geopandas as gpd


def get_gdf(db, query: str, geom_col: str = "geom") -> gpd.GeoDataFrame:
    """
    - Get a `geopandas.GeoDataFrame` from a `PostGIS` query
    """

    connection = psycopg2.connect(db.uri)

    gdf = gpd.GeoDataFrame.from_postgis(query, connection, geom_col=geom_col)

    connection.close()

    return gdf