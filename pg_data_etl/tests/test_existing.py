from pg_data_etl.tests.fixtures import network_db_that_exists, production_gis_db


def test_that_existing_database_exists():
    """ This database definitely exists already """

    assert network_db_that_exists.exists() is True


def test_that_existing_table_exists():
    """ 'countyboundaries' is a table that exists in the production GIS database """

    assert "countyboundaries" in production_gis_db.all_spatial_tables_in_db()
