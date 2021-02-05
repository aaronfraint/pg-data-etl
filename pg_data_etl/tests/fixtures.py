import time
from pg_data_etl import Database, configurations
import pytest
import os
import shutil

# Define database connections for the tests
c = configurations()
local_db = Database("test_from_pytest_lastone", **c["localhost"])
local_sw_db = Database("sidewalk_gap_analysis", **c["localhost"])
network_db_that_exists = Database("GTFS", **c["daisy"])
production_gis_db = Database("gis", **c["dvrpc_gis"])

# Create a directory to hold the test files
test_data_path = r"C:\Users\afraint\Documents\pg_data_etl_tests"

shp_path = os.path.join(test_data_path, "test_shapefile")


def take_a_break(seconds: float = 1.0):
    time.sleep(seconds)


@pytest.fixture(scope="session", autouse=True)
def test_data(request):
    """
    Create a test data directory during the setup.
    Remove the folder (+ all contents) at the end of the test suite.
    """

    # Setup
    os.makedirs(test_data_path)

    # Teardown
    def remove_test_dir():
        shutil.rmtree(test_data_path)

    request.addfinalizer(remove_test_dir)
