import time
from pg_data_etl import Database, configurations

c = configurations()

local_db = Database("test_from_pytest_lastone", **c["localhost"])
local_sw_db = Database("sidewalk_gap_analysis", **c["localhost"])
network_db_that_exists = Database("GTFS", **c["daisy"])
production_gis_db = Database("gis", **c["dvrpc_gis"])


def take_a_break(seconds: float = 1.0):
    time.sleep(seconds)
