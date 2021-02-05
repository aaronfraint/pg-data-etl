import urllib
import shutil
import zipfile

from pg_data_etl.tests.fixtures import (
    local_db,
    zipped_shp_file_name,
    shp_zip_url,
    test_data_path,
)


def test_shp2pgsql_imports_spatial_data():

    with urllib.request.urlopen(shp_zip_url) as response, open(
        zipped_shp_file_name, "wb"
    ) as out_file:
        shutil.copyfileobj(response, out_file)

    with zipfile.ZipFile(zipped_shp_file_name) as zf:
        zf.extractall(test_data_path)

    unzipped_shp_file_name = zipped_shp_file_name.replace(".zip", "")

    local_db.create_db()

    local_db.shp2pgsql(unzipped_shp_file_name, 2272, "test.neighborhoods")