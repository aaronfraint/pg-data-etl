def test_that_existing_database_exists(network_db_that_exists):
    """ This database definitely exists already """

    assert network_db_that_exists.exists() is True


def test_that_existing_table_exists(production_gis_db):
    """ 'countyboundaries' is a table that exists in the production GIS database """

    assert "boundaries.countyboundaries" in production_gis_db.spatial_table_list()
