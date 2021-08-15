from __future__ import annotations

from pg_data_etl import helpers


def gis_table_lint_geom_colname(self, tablename: str) -> None:
    """
    - Rename the geometry column to 'geom' if it comes through as 'shape'
    - This is necessary for any data coming from an ESRI/PostGIS database,
    where the `geom` convention is replaced by ESRI's preference for `shape`

    Arguments:
        tablename (str): name of the spatial table to lint

    Returns:
        None: but updates the geometry column name if it came through as `shape`
    """

    if "shape" in self.columns(tablename):
        self.table_rename_column("shape", "geom", tablename)


def gis_table_add_spatial_index(self, tablename: str) -> None:
    """
    - Add a spatial index on a table's geom column

    Arguments:
        tablename (str): name of the table to perform a spatial index on

    Returns:
        None: although it updates the database to have a spatial index on the `geom` column
    """
    query = f"""
        CREATE INDEX ON {tablename}
        USING GIST (geom);
    """
    self.execute(query)


def gis_table_update_spatial_data_projection(
    self,
    tablename: str,
    old_epsg: int | str,
    new_epsg: int | str,
    geom_type: str,
) -> None:
    """
    - Transform a table in-place from `old_epsg` to `new_epsg`.
    - You can use this with identical old and new epsgs to force an entry into the
    `geometry_columns` table. (Helpful for making geotables directly in the DB via query)

    Arguments:
        tablename (str): name of the spatial table to re-project
        old_epsg (int | str): EPSG code of the original projection
        new_epsg (int | str): EPSG code of the desired new projection
        geom_type (str): PostGIS geometry data type (e.g 'LineString', 'Point', etc.)

    Returns:
        None: but updates the table in-place to the new_epsg
    """

    query = f"""
        ALTER TABLE {tablename}
        ALTER COLUMN geom TYPE geometry({geom_type}, {new_epsg})
        USING ST_Transform(ST_SetSRID(geom, {old_epsg}), {new_epsg});
    """
    self.execute(query)


def gis_make_geotable_from_query(
    self,
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

    Arguments:
        query (str): any valid SQL query that returns a spatial table
        new_table_name (str): name of the new table to hold the query output
        geom_type (str): PostGIS geometry data type returned by the query
        epsg (int): EPSG code of the geometry data returned by the query
        uid_col (str): name of the new unique ID column that will be auto-generated (defaults to 'uid')

    Returns:
        None: but generates a new spatial table from the query
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
        self.schema_add(schema)

        query_to_make_table = f"""
            DROP TABLE IF EXISTS {new_table_name};
            CREATE TABLE {new_table_name} AS
            {query}
        """

        self.execute(query_to_make_table)

        self.table_add_uid_column(new_table_name, uid_col=uid_col)
        self.gis_table_add_spatial_index(new_table_name)

        # We're not reprojecting here, but rather forcing an entry for
        # the new geo table into the geometry_columns table
        self.gis_table_update_spatial_data_projection(new_table_name, epsg, epsg, geom_type.upper())
