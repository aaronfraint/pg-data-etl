from pathlib import Path
import pandas as pd
import sqlalchemy
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement

from pg_data_etl import helpers


def shp2pgsql(self, filepath: str, srid: int, sql_tablename: str, new_srid: int = None):
    """
    Use the shp2pgsql command to import a shapefile into the database
    """

    # Ensure that the schema provided in the 'tablename' exists
    schema, _ = helpers.convert_full_tablename_to_parts(sql_tablename)
    self.schema_add(schema)

    # If 'new_srid' is provided, use 'old:new' to project on the fly
    srid_arg = f"{srid}:{new_srid}" if new_srid else srid

    command = (
        f'{self.cmd.shp2pgsql} -I -s {srid_arg} "{filepath}" {sql_tablename} | psql {self.uri}'
    )

    print(command)

    helpers.run_command_in_shell(command)

    self.gis_table_lint_geom_colname(sql_tablename)


def import_geofile_with_geopandas(
    self,
    filepath: Path,
    sql_tablename: str,
    gpd_kwargs: dict = {},
    explode: bool = False,
) -> None:

    # Read the data into a geodataframe
    gdf = gpd.read_file(filepath)

    # Drop null geometries
    gdf = gdf[gdf["geometry"].notnull()]

    self.import_geodataframe(gdf, sql_tablename, gpd_kwargs, explode=explode)


def import_geodataframe(
    self,
    gdf: gpd.GeoDataFrame,
    tablename: str,
    gpd_kwargs: dict = {},
    uid_col: str = "uid",
    explode: bool = False,
) -> None:
    """
    TODO: option to use multipart features instead of exploding to singlepart
    """

    gdf = gdf.copy()

    gdf = helpers.sanitize_df_for_sql(gdf)

    epsg_code = int(str(gdf.crs).split(":")[1])

    # Get a list of all geometry types in the dataframe
    geom_types = list(gdf.geometry.geom_type.unique())

    # If there are multi- and single-part features, explode to singlepart
    if explode:
        # Explode multipart to singlepart and reset the index
        gdf = gdf.explode()
        gdf["explode"] = gdf.index.to_numpy()
        gdf = gdf.reset_index()

    else:
        if len(geom_types) > 1:
            print(f"Warning! This dataset has {geom_types=}")
            print("Run with explode=True")
            return None

    # Use the non-multi version of the geometry
    geom_type_to_use = min(geom_types, key=len).upper()

    # Replace the 'geom' column with 'geometry'
    if "geom" in gdf.columns:
        gdf["geometry"] = gdf["geom"]
        gdf.drop(labels="geom", axis=1, inplace=True)

    # Drop the 'gid' column
    if "gid" in gdf.columns:
        gdf.drop(labels="gid", axis=1, inplace=True)

    # Rename 'uid' to 'old_uid'
    if uid_col in gdf.columns:
        gdf[f"old_{uid_col}"] = gdf[uid_col]
        gdf.drop(labels=uid_col, axis=1, inplace=True)

    # Build a 'geom' column using geoalchemy2
    # and drop the source 'geometry' column
    gdf["geom"] = gdf["geometry"].apply(lambda x: WKTElement(x.wkt, srid=epsg_code))
    gdf.drop(labels="geometry", axis=1, inplace=True)

    # Ensure that the target schema exists
    schema, tbl = helpers.convert_full_tablename_to_parts(tablename)
    self.schema_add(schema)

    # Write geodataframe to SQL database
    engine = sqlalchemy.create_engine(self.uri)
    gdf.to_sql(
        tbl,
        engine,
        schema=schema,
        dtype={"geom": Geometry(geom_type_to_use, srid=epsg_code)},
        **gpd_kwargs,
    )
    engine.dispose()

    self.table_add_uid_column(tablename)
    self.gis_table_add_spatial_index(tablename)


def import_gis(self, method="geopandas", **kwargs):
    """
    - Import GIS data using `geopandas` or `shp2pgsql`
    - Both methods take `filepath` and `sql_tablename` keyword arguments
    - The geopandas method accepts optional `gpd_kwargs=dict` and `explode=bool` arguments
    - The `shp2pgsql` method requires `srid=int` and accepts an optional `new_srid=int` to convert projections during the import process
    """
    method_mapper = {
        "geopandas": import_geofile_with_geopandas,
        "shp2pgsql": shp2pgsql,
    }

    if method not in method_mapper:
        print(f"{method=} does not exist. Valid options include: {method_mapper.keys()}")

    func = method_mapper[method]

    func(self, **kwargs)
