from pg_data_etl.configuration_manager import DB_CONFIG_FILEPATH
from typing import Union
from pg_data_etl import helpers


def lint_geom_colname(db, tablename: str) -> None:
    """
    - Rename the geometry column to 'geom' if it comes through as 'shape'
    """

    if "shape" in db.list_of_columns_in(tablename):
        db.rename_column(db, "shape", "geom", tablename)


def add_spatial_index_to_table(db, tablename: str) -> None:
    """
    - Add a spatial index on a table's geom column
    """
    query = f"""
        CREATE INDEX ON {tablename}
        USING GIST (geom);
    """
    db.execute(query)


def update_spatial_data_projection(
    db,
    tablename: str,
    old_epsg: Union[int, str],
    new_epsg: Union[int, str],
    geom_type: str,
) -> None:
    """
    - Transform a table in-place from `old_epsg` to `new_epsg`.
    - You can use this with identical old and new epsgs to force an
    entry into the `geometry_columns` table. (Helpful for making geotables directly in the DB via query)
    """

    query = f"""
        ALTER TABLE {tablename}
        ALTER COLUMN geom TYPE geometry({geom_type}, {new_epsg})
        USING ST_Transform(ST_SetSRID(geom, {old_epsg}), {new_epsg});
    """
    db.execute(query)


def make_geotable_from_query(
    db,
    query: str,
    new_table_name: str,
    geom_type: str,
    epsg: int,
    uid_col: str = "uid",
) -> None:
    """
    - Allow the creation of a new table in the db directly via query.

    - This is especially helpful when you're working with a large dataset
    and you want to limit the I/O processing time.
    """

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
        return None

    else:

        # Make sure the schema exists
        schema, _ = helpers.convert_full_tablename_to_parts(new_table_name)
        db.add_schema(schema)

        query_to_make_table = f"""
            DROP TABLE IF EXISTS {new_table_name};
            CREATE TABLE {new_table_name} AS
            {query}
        """

        db.execute(query_to_make_table)

        db.add_uid_column_to_table(new_table_name)
        db.add_spatial_index_to_table(db, new_table_name)

        # We're not reprojecting here, but rather forcing an entry for
        # the new geo table into the geometry_columns table
        db.update_spatial_data_projection(db, new_table_name, epsg, epsg, geom_type.upper())
