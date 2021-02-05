from pg_data_etl.tests.fixtures import (
    local_db,
    network_db_that_exists,
    production_gis_db,
    take_a_break,
    local_sw_db,
)


def test_copy_of_table():
    """Copy a known non-spatial table from an existing DB to a new DB"""

    local_db.create_db()

    take_a_break()

    network_db_that_exists.copy_table_to_another_db("lineroutes", local_db)

    assert "lineroutes" in local_db.all_tables_in_db()


def test_copy_of_local_spatial_table():
    """Copy a spatial table from a local db to another local db """
    local_sw_db.copy_table_to_another_db("circuittrails", local_db)

    assert "circuittrails" in local_db.all_spatial_tables_in_db()


def test_copy_of_network_spatial_table():
    """Copy a spatial table from a NETWORK db to a local db """
    production_gis_db.copy_table_to_another_db("boundaries.countyboundaries", local_db)

    assert "countyboundaries" in local_db.all_spatial_tables_in_db()
