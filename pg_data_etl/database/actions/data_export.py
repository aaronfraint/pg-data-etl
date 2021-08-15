from __future__ import annotations
from pathlib import Path

from pg_data_etl import helpers


def export_shp_with_pgsql2shp(self, table_or_sql: str, filepath: Path) -> None:
    """
    Use pgsql2shp to export a shapefile from the database.

    Valid arguments for `table_or_sql` are the name of a table or a full query.
    e.g. "pa.centerlines"
            "SELECT * FROM pa.centerlines WHERE some_column = 'some value'"
    """

    print("WARNING! pgsql2shp creates shapefiles that do not contain EPSG values")
    print("As an alternative that preserves EPSG values, use either:")
    print("\t -> Database.export_gis(method='geopandas')")
    print("\t -> Database.export_gis(method='ogr2ogr')")

    if helpers.this_is_raw_sql(table_or_sql):
        query = table_or_sql
    else:
        query = f"SELECT * FROM {table_or_sql}"

    params = self.connection_params()

    command = f'pgsql2shp -f "{filepath}" -h {params["host"]} -u {params["un"]} -P {params["pw"]} -p {params["port"]} {params["db_name"]} "{query}" '
    print(command)

    helpers.run_command_in_shell(command)


def export_shp_with_ogr2ogr(
    self, table_or_sql: str, filepath: Path, filetype: str = "ESRI Shapefile"
) -> None:
    """
    Use ogr2ogr to export a shapefile from the database.

    Valid arguments for `table_or_sql` are the name of a table or a full query.
    e.g. "pa.centerlines"
            "SELECT * FROM pa.centerlines WHERE some_column = 'some value'"
    """

    params = self.connection_params()

    pg_params_for_ogr = f'PG:"host={params["host"]} user={params["un"]} password={params["pw"]} port={params["port"]} dbname={params["db_name"]}"'

    cmd = f'{self.cmd.ogr2ogr} -f "{filetype}" "{filepath}" {pg_params_for_ogr} '

    # If a query is passed, append cmd with ' -sql QUERY'
    if helpers.this_is_raw_sql(table_or_sql):
        sql = table_or_sql
        cmd += f' -sql "{sql}"'
    # Otherwise, just append the tablename to the cmd
    else:
        tablename = table_or_sql
        cmd += f" {tablename}"

    print(cmd)
    helpers.run_command_in_shell(cmd)


def export_gis_with_geopandas(
    self,
    table_or_sql: str,
    filepath: Path | str,
    filetype: str = "geojson",
    geom_col: str = "geom",
) -> None:
    """
    - Use `geopandas` to extract data from SQL and write to `.geojson` or `.shp`
    """

    filepath = Path(filepath)

    # Exit early if arguments don't match
    if filetype not in filepath.suffix:
        print("File type and path do not match!")
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

    data = self.gdf(query, geom_col=geom_col)

    # Write to file
    if filetype == "geojson":
        data.to_file(filepath, driver="GeoJSON")

    elif filetype == "shp":
        data.to_file(filepath, driver="ESRI Shapefile")

    return None


def export_gis(self, method="geopandas", **kwargs):
    """
    - All methods require kwargs `table_or_sql` and `filepath`
    - Optional kwargs include `filetype` (ogr2ogr & geopandas) and `geom_col` (geopandas only)
    """
    method_mapper = {
        "geopandas": export_gis_with_geopandas,
        "ogr2ogr": export_shp_with_ogr2ogr,
        "pgsql2shp": export_shp_with_pgsql2shp,
    }

    if method not in method_mapper:
        print(f"{method=} does not exist. Valid options include: {method_mapper.keys()}")

    func = method_mapper[method]

    func(self, **kwargs)
