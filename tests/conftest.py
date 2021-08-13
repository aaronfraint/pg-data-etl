from pg_data_etl import Database
import pytest
import shutil
import zipfile
import urllib.request
from pathlib import Path

# This is a temp folder where we'll store inputs/ouputs from the testing suite
TEST_DATA_PATH = Path("./test_data")


# This is a shapefile we'll download from the web and import with shp2pgsql
ZIPPED_SHAPEFILE_URL = "https://github.com/azavea/geo-data/raw/master/Neighborhoods_Philadelphia/Neighborhoods_Philadelphia.zip"
LOCAL_ZIPPED_FILEPATH = TEST_DATA_PATH / "Neighborhoods_Philadelphia.zip"


@pytest.fixture(scope="function")
def local_db():
    """ Spin up a local db, use it in the test, then drop it """

    db = Database.from_config("pytest", "localhost")

    db.admin("CREATE")

    yield db

    db.admin("DROP")


@pytest.fixture(scope="function")
def local_db_with_spatial_data(downloaded_shapefile):
    """ Spin up a local db, use it in the test, then drop it """

    db = Database.from_config("pytest", "localhost")

    db.admin("CREATE")

    sql_tablename = "test.neighborhoods_gpd"

    db.import_gis(
        method="geopandas",
        filepath=str(downloaded_shapefile) + ".shp",
        sql_tablename=sql_tablename,
    )

    yield db

    db.admin("DROP")


@pytest.fixture(scope="function")
def localhost_postgres():
    yield Database.from_config("postgres", "localhost")


# @pytest.fixture(scope="function")
# def production_gis_db():
#     yield Database.from_parameters("gis", **config["dvrpc_gis"])


# @pytest.fixture(scope="function")
# def network_db_that_exists():
#     yield Database.from_parameters("GTFS", **config["daisy"])


# @pytest.fixture(scope="function")
# def local_sw_db():
#     yield Database.from_parameters("sidewalk_gap_analysis", **config["localhost"])


@pytest.fixture(scope="session")
def downloaded_shapefile():

    if not TEST_DATA_PATH.exists():
        TEST_DATA_PATH.mkdir()

    # Copy zipped shapefile to disk
    with urllib.request.urlopen(ZIPPED_SHAPEFILE_URL) as response, open(
        LOCAL_ZIPPED_FILEPATH, "wb"
    ) as out_file:
        shutil.copyfileobj(response, out_file)

    # Extract the zip file
    with zipfile.ZipFile(LOCAL_ZIPPED_FILEPATH) as zf:
        zf.extractall(TEST_DATA_PATH)

    # provide the unzipped filepath
    print(LOCAL_ZIPPED_FILEPATH)
    unzipped_file_name = str(LOCAL_ZIPPED_FILEPATH).replace(".zip", "")

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
        shutil.rmtree(TEST_DATA_PATH)

    request.addfinalizer(remove_test_dir_and_db)
