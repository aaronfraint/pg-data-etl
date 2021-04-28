from typing import Union
from pg_data_etl import helpers


def ensure_geometry_is_named_geom(db, tablename: str):
    """
    Rename the geometry column to 'geom' if it comes through as 'shape'
    """

    if "shape" in db.columns_in_table(tablename):
        db.rename_column("shape", "geom", tablename)


def add_spatial_index_to_table(db, sql_tablename: str):
    """
    Add a spatial index on a table's geom column
    """
    sql_make_spatial_index = f"""
        CREATE INDEX ON {sql_tablename}
        USING GIST (geom);
    """
    db.execute(sql_make_spatial_index)


def project_spatial_table(
    db,
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
    db.execute(sql_transform_geom)


# EXECUTE A QUERY THAT CREATES A NEW GEOTABLE IN THE DB


def make_geotable_from_query(
    db,
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

    schema, tbl = helpers.convert_full_tablename_to_parts(new_table_name)

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

    db.add_schema(schema)

    db.execute(sql_make_table_from_query)

    db.add_uid_column_to_table(new_table_name, uid_col=uid_col)
    db.add_spatial_index_to_table(new_table_name)

    # We're not reprojecting here, but rather forcing an entry for
    # the new geo table into the geometry_columns table
    db.project_spatial_table(new_table_name, epsg, epsg, geom_type=geom_type.upper())
