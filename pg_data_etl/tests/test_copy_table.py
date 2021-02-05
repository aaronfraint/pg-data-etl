from pg_data_etl.tests.fixtures import local_db, network_db_that_exists, take_a_break


def test_copy_of_table():
    """Copy a known table from an existing DB to a new DB"""

    local_db.create_db()

    take_a_break()

    network_db_that_exists.copy_table_to_another_db("lineroutes", local_db)

    assert "lineroutes" in local_db.all_tables_in_db()
