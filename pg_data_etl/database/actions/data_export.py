from pathlib import Path
import geopandas as gpd
from pg_data_etl import helpers
from .query.data_spatial import get_gdf


def export_shp_with_pgsql2shp(db, table_or_sql: str, filepath: Path) -> None:
    """
    Use pgsql2shp to export a shapefile from the database.

    Valid arguments for `table_or_sql` are the name of a table or a full query.
    e.g. "pa.centerlines"
            "SELECT * FROM pa.centerlines WHERE some_column = 'some value'"
    """

    print("WARNING! pgsql2shp creates shapefiles that do not contain EPSG values")
    print("As an alternative that preserves EPSG values, use Database.ogr2ogr_export() instead")

    if helpers.this_is_raw_sql(table_or_sql):
        query = table_or_sql
    else:
        query = f"SELECT * FROM {table_or_sql}"

    command = f'pgsql2shp -f "{filepath}" -h {db.params["host"]} -u {db.params["un"]} -P {db.params["pw"]} -p {db.params["port"]} {db.params["db_name"]} "{query}" '
    print(command)

    helpers.run_command_in_shell(command)


def export_shp_with_ogr2ogr(
    db, table_or_sql: str, filepath: Path, filetype: str = "ESRI Shapefile"
) -> None:
    """
    Use ogr2ogr to export a shapefile from the database.

    Valid arguments for `table_or_sql` are the name of a table or a full query.
    e.g. "pa.centerlines"
            "SELECT * FROM pa.centerlines WHERE some_column = 'some value'"
    """

    cmd = f'ogr2ogr -f "{filetype}" "{filepath}" PG:"host={db.params["host"]} user={db.params["un"]} password={db.params["pw"]} port={db.params["port"]} dbname={db.params["db_name"]}" '

    if helpers.this_is_raw_sql(table_or_sql):
        sql = table_or_sql
        cmd += f' -sql "{sql}"'
    else:
        tablename = table_or_sql
        cmd += f" {tablename}"

    print(cmd)
    helpers.run_command_in_shell(cmd)


def export_gis_with_geopandas(
    db, table_or_sql: str, filepath: Path, filetype: str = "geojson", geom_col: str = "geom"
) -> None:
    """
    - Use `geopandas` to extract data from SQL and write to `.geojson` or `.shp`
    """

    # Exit early if arguments don't match
    if filetype != filepath.suffix:
        print(f"File type and path do not match!")
        print(f"{filetype=} {filepath.suffix=}")
        return None

    if filetype not in ["geojson", "shp"]:
        print(f"Invalid filetype: {filetype=}")
        return None

    # Get a geodataframe from SQL
    if helpers.this_is_raw_sql(table_or_sql):
        query = table_or_sql
    else:
        query = f"SELECT * FROM {table_or_sql}"

    gdf = get_gdf(db, query, geom_col=geom_col)

    # Write to file
    if filetype == "geojson":
        gdf.to_file(filepath, driver="GeoJSON")

    elif filetype == "shp":
        gdf.to_file(filepath, driver="ESRI Shapefile")

    return None
