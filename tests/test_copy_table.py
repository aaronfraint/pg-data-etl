def test_copy_of_table(local_db, network_db_that_exists):
    """Copy a known non-spatial table from an existing DB to a new DB"""

    network_db_that_exists.copy_table_to_another_db("lineroutes", local_db)

    assert "public.lineroutes" in local_db.table_list()


def test_copy_of_local_spatial_table(local_db, local_sw_db):
    """Copy a spatial table from a local db to another local db """

    local_sw_db.copy_table_to_another_db("circuittrails", local_db)

    assert "public.circuittrails" in local_db.spatial_table_list()


def test_copy_of_network_spatial_table(local_db, production_gis_db):
    """Copy a spatial table from a NETWORK db to a local db """

    production_gis_db.copy_table_to_another_db("boundaries.countyboundaries", local_db)

    assert "boundaries.countyboundaries" in local_db.spatial_table_list()


def test_list_of_columns_in_table(local_db, production_gis_db):
    """
    Confirm that a 'shape' column gets properly renamed to 'geom' after copying

    All tables in the production GIS db have 'shape' as the name of their geometry column.
    """

    production_gis_db.copy_table_to_another_db("boundaries.countyboundaries", local_db)

    columns = local_db.columns_in_table("boundaries.countyboundaries")

    assert "geom" in columns
