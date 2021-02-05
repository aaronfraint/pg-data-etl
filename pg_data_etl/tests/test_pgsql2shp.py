from pathlib import Path
from pg_data_etl.tests.fixtures import production_gis_db, shp_path


def test_pgsql2shp_makes_shapefile():
    """Save a shapefile from postgres, and confirm the filepath exists """

    production_gis_db.pgsql2shp("boundaries.countyboundaries", shp_path)

    assert Path(shp_path + ".shp").exists()
