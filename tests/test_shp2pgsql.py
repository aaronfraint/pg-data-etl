from pg_data_etl import Database


def test_shp2pgsql_imports_spatial_data(local_db: Database, downloaded_shapefile):
    """
    Confirm that we have a spatial table in the DB after importing a local shapefile
    """

    unzipped_shp_file_name = downloaded_shapefile

    local_db.import_gis(
        method="shp2pgsql",
        srid=2272,
        filepath=unzipped_shp_file_name,
        sql_tablename="test.neighborhoods",
    )

    assert "test.neighborhoods" in local_db.tables(spatial_only=True)
