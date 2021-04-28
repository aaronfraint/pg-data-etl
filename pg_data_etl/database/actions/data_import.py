from pathlib import Path
import pandas as pd
import sqlalchemy
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement

import pg_data_etl.database.helpers as helpers


def shp2pgsql(db, shp_path: str, srid: int, sql_tablename: str, new_srid: int = None):
    """
    Use the shp2pgsql command to import a shapefile into the database
    """

    # Ensure that the schema provided in the 'sql_tablename' exists
    if "." in sql_tablename:
        schema = sql_tablename.split(".")[0]
        db.add_schema(schema)

    # If 'new_srid' is provided, use 'old:new' to project on the fly
    srid_arg = f"{srid}:{new_srid}" if new_srid else srid

    command = (
        f'shp2pgsql -I -s {srid_arg} "{shp_path}" {sql_tablename} | psql {db.uri()}'
    )

    print(command)

    helpers.run_command_in_shell(command)

    db.ensure_geometry_is_named_geom(sql_tablename)


def import_tabular_file(
    db,
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

    db.import_dataframe(df, sql_tablename, df_import_kwargs)


def import_dataframe(
    db, df: pd.DataFrame, sql_tablename: str, df_import_kwargs: dict = {}
) -> None:

    # Clean up column names
    df = helpers.sanitize_df_for_sql(df)

    # Make sure the schema exists
    schema, table_name = helpers.convert_full_tablename_to_parts(sql_tablename)
    db.add_schema(schema)

    # Write to database
    engine = sqlalchemy.create_engine(db.uri())

    df.to_sql(table_name, engine, schema=schema, **df_import_kwargs)

    engine.dispose()


def import_geo_file(
    db, filepath: Path, sql_tablename: str, gpd_kwargs: dict = {}
) -> None:

    # Read the data into a geodataframe
    gdf = gpd.read_file(filepath)

    # Drop null geometries
    gdf = gdf[gdf["geometry"].notnull()]

    db.import_geodataframe(gdf, sql_tablename, gpd_kwargs)


def import_geodataframe(
    db,
    gdf: gpd.GeoDataFrame,
    sql_tablename: str,
    gpd_kwargs: dict = {},
    uid_col: str = "uid",
) -> None:

    schema, table_name = helpers.convert_full_tablename_to_parts(sql_tablename)

    gdf = gdf.copy()

    gdf = helpers.sanitize_df_for_sql(gdf)

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
    db.add_schema(schema)

    engine = sqlalchemy.create_engine(db.uri())
    gdf.to_sql(
        table_name,
        engine,
        schema=schema,
        dtype={"geom": Geometry(geom_type_to_use, srid=epsg_code)},
        **gpd_kwargs,
    )
    engine.dispose()

    db.add_uid_column_to_table(sql_tablename)
    db.add_spatial_index_to_table(sql_tablename)
