import time
from pg_data_etl import Database, configurations
import pytest
import os
import shutil
import zipfile
import urllib.request

# Define database connections for the tests
c = configurations()
local_db = Database("test_from_pytest_lastone", **c["localhost"])
local_sw_db = Database("sidewalk_gap_analysis", **c["localhost"])
network_db_that_exists = Database("GTFS", **c["daisy"])
production_gis_db = Database("gis", **c["dvrpc_gis"])

# Create a directory to hold the test files
test_data_path = r"C:\Users\afraint\Documents\pg_data_etl_tests"
if not os.path.exists(test_data_path):
    os.makedirs(test_data_path)

# This is a shapefile we're going to make vis pgsql2shp
shp_path = os.path.join(test_data_path, "test_shapefile")

# This is a shapefile we'll download from the web and import with shp2pgsql
shp_zip_url = "https://github.com/azavea/geo-data/raw/master/Neighborhoods_Philadelphia/Neighborhoods_Philadelphia.zip"
zipped_shp_file_name = os.path.join(test_data_path, "Neighborhoods_Philadelphia.zip")


def take_a_break(seconds: float = 1.0):
    time.sleep(seconds)


@pytest.fixture(scope="session", autouse=True)
def test_data(request):
    """
    TEARDOWN:
    ---------
        1) Remove the folder (+ all contents) at the end of the test suite.
    """

    # Teardown
    def remove_test_dir_and_db():
        shutil.rmtree(test_data_path)
        local_db.drop_db()

    request.addfinalizer(remove_test_dir_and_db)
