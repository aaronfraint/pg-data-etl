from pathlib import Path
import os


def test_pgsql2shp_makes_shapefile(production_gis_db, test_folder):
    """Save a shapefile from postgres, and confirm the filepath exists """

    shp_path = os.path.join(test_folder, "test_output_from_pgsql2shp")

    production_gis_db.pgsql2shp("boundaries.countyboundaries", shp_path)

    assert Path(shp_path + ".shp").exists()
