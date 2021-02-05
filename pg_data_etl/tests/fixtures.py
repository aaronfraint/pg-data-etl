import time
from pg_data_etl import Database, configurations

c = configurations()

local_db = Database("test_from_pytest_lastone", **c["localhost"])
network_db_that_exists = Database("GTFS", **c["daisy"])


def take_a_break(seconds: float = 0.5):
    time.sleep(seconds)
