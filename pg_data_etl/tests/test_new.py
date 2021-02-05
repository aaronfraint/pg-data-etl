from pg_data_etl.tests.fixtures import local_db, take_a_break


def test_creation_of_database():
    """ Make a new database locally and confirm it exists """

    local_db.create_db()
    take_a_break()

    assert local_db.exists() is True


def test_dropping_of_database():
    """Create a local DB, drop it, then confirm it does not exist """

    local_db.create_db()
    take_a_break()

    local_db.drop_db()
    take_a_break()

    assert local_db.exists() is False
