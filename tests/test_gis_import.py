from pg_data_etl import Database


def test_shp2pgsql_imports_spatial_data_from_disk(
    local_db: Database, downloaded_shapefile
):
    """
    Using shp2pgsql:
        Confirm that we have a spatial table with proper projection in the DB
        after importing a local shapefile
    """

    unzipped_shp_file_name = downloaded_shapefile

    sql_tablename = "test.neighborhoods"

    local_db.import_gis(
        method="shp2pgsql",
        srid=2272,
        filepath=unzipped_shp_file_name,
        sql_tablename=sql_tablename,
    )

    # Confirm the spatial table exists
    assert sql_tablename in local_db.tables(spatial_only=True)

    # Confirm that the EPSG is correct
    assert 2272 == local_db.projection(sql_tablename)


def test_geopandas_imports_spatial_data_from_disk(
    local_db: Database, downloaded_shapefile
):
    """
    Using geopandas:
        Confirm that we have a spatial table with proper projection in the DB
        after importing a local shapefile
    """

    unzipped_shp_file_name = downloaded_shapefile

    sql_tablename = "test.neighborhoods_gpd"

    local_db.import_gis(
        method="geopandas",
        filepath=str(unzipped_shp_file_name) + ".shp",
        sql_tablename=sql_tablename,
    )

    # Confirm the spatial table exists
    assert sql_tablename in local_db.tables(spatial_only=True)

    # Confirm that the EPSG is correct
    assert 2272 == local_db.projection(sql_tablename)
