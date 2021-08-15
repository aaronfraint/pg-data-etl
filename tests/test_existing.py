from pg_data_etl import Database


def test_that_existing_database_exists(localhost_postgres: Database):
    """ This database definitely exists already """

    assert localhost_postgres.exists() is True


def test_that_spatial_table_is_inside_database(local_db_with_spatial_data: Database):
    assert "test.neighborhoods_gpd" in local_db_with_spatial_data.tables(spatial_only=True)
