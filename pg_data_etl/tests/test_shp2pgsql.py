def test_shp2pgsql_imports_spatial_data(local_db, downloaded_shapefile):

    unzipped_shp_file_name = downloaded_shapefile

    local_db.shp2pgsql(unzipped_shp_file_name, 2272, "test.neighborhoods")

    assert "neighborhoods" in local_db.spatial_table_list(schema="test")
