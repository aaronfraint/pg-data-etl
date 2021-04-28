from pathlib import Path
import pg_data_etl.database.helpers as helpers


def pgsql2shp(db, table_or_sql: str, output_filepath: Path) -> None:
    """
    Use pgsql2shp to export a shapefile from the database.

    Valid arguments for `table_or_sql` are the name of a table or a full query.
    e.g. "pa.centerlines"
            "SELECT * FROM pa.centerlines WHERE some_column = 'some value'"
    """

    print("WARNING! pgsql2shp creates shapefiles that do not contain EPSG values")
    print(
        "As an alternative that preserves EPSG values, use Database.ogr2ogr_export() instead"
    )

    if "select" in table_or_sql.lower():
        query = table_or_sql
    else:
        query = f"SELECT * FROM {table_or_sql}"

    command = f'pgsql2shp -f "{output_filepath}" -h {db.params["host"]} -u {db.params["un"]} -P {db.params["pw"]} -p {db.params["port"]} {db.params["db_name"]} "{query}" '
    print(command)

    helpers.run_command_in_shell(command)


def ogr2ogr_export(
    db, table_or_sql: str, filepath: Path, filetype: str = "ESRI Shapefile"
):
    """
    Use ogr2ogr to export a shapefile from the database.

    Valid arguments for `table_or_sql` are the name of a table or a full query.
    e.g. "pa.centerlines"
            "SELECT * FROM pa.centerlines WHERE some_column = 'some value'"
    """

    cmd = f'ogr2ogr -f "{filetype}" "{filepath}" PG:"host={db.params["host"]} user={db.params["un"]} password={db.params["pw"]} port={db.params["port"]} dbname={db.params["db_name"]}" '

    if "select" in table_or_sql.lower():
        sql = table_or_sql
        cmd += f' -sql "{sql}"'
    else:
        tablename = table_or_sql
        cmd += f" {tablename}"

    print(cmd)
    helpers.run_command_in_shell(cmd)
