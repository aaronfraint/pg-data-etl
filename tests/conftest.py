from pg_data_etl import Database, connections
import pytest
import os
import shutil
import zipfile
import urllib.request

# This is a temp folder where we'll store inputs/ouputs from the testing suite
test_data_path = r"C:\Users\afraint\Documents\pg_data_etl_tests"


# This is a shapefile we'll download from the web and import with shp2pgsql
shp_zip_url = "https://github.com/azavea/geo-data/raw/master/Neighborhoods_Philadelphia/Neighborhoods_Philadelphia.zip"
zipped_shp_file_name = os.path.join(test_data_path, "Neighborhoods_Philadelphia.zip")


@pytest.fixture(scope="function")
def test_folder():
    if not os.path.exists(test_data_path):
        os.makedirs(test_data_path)

    yield test_data_path


@pytest.fixture(scope="function")
def local_db():
    """ Spin up a local db, use it in the test, then drop it """

    db = Database("test_from_pytest_lastone", **connections["localhost"])

    db.create_db()

    yield db

    db.drop_db()


@pytest.fixture(scope="function")
def production_gis_db():
    yield Database("gis", **connections["dvrpc_gis"])


@pytest.fixture(scope="function")
def network_db_that_exists():
    yield Database("GTFS", **connections["daisy"])


@pytest.fixture(scope="function")
def local_sw_db():
    yield Database("sidewalk_gap_analysis", **connections["localhost"])


@pytest.fixture(scope="function")
def downloaded_shapefile():
    with urllib.request.urlopen(shp_zip_url) as response, open(
        zipped_shp_file_name, "wb"
    ) as out_file:
        shutil.copyfileobj(response, out_file)

    with zipfile.ZipFile(zipped_shp_file_name) as zf:
        zf.extractall(test_data_path)

    unzipped_file_name = zipped_shp_file_name.replace(".zip", "")

    yield unzipped_file_name


@pytest.fixture(scope="session", autouse=True)
def teardown_test_data_dir(request):
    """
    TEARDOWN:
    ---------
        1) Remove the folder (+ all contents) at the end of the test suite.
    """

    # Teardown
    def remove_test_dir_and_db():
        shutil.rmtree(test_data_path)

    request.addfinalizer(remove_test_dir_and_db)
