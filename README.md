# pg-data-etl

[![PyPI](https://img.shields.io/pypi/v/pg-data-etl?style=for-the-badge)](https://pypi.org/project/pg-data-etl/)

ETL tools for spatial data stored in postgres.

## About

This module exists to make life easier when working with geospatial data in a Postgres database.

You should have the following command-line tools installed, preferably on your system path:

- `psql`
- `pg_dump`
- `shp2postgis`
- `ogr2ogr`

If you want to use the optional vector tile functions you'll also need:

- `tippecanoe`

## Installation

`pip install pg_data_etl`

## Example

The following code blocks import spatial data into Postgres and runs a spatial query:

### 1) Connect to the database and create it

```python
>>> from pg_data_etl import Database
>>> credentials = {
...     "db_name": "sample_database",
...     "host": "localhost",
...     "un": "username",
...     "pw": "my-password",
...     "super_un": "postgres",
...     "super_pw": "superuser-password"
... }
>>> db = Database.from_parameters(**credentials)
>>> db.admin("CREATE")
```

### 2) Import GIS data from the web

```python
>>> data_to_import = [
...     ("philly.high_injury_network", "https://phl.carto.com/api/v2/sql?filename=high_injury_network_2020&format=geojson&skipfields=cartodb_id&q=SELECT+*+FROM+high_injury_network_2020"),
...     ("philly.playgrounds", "https://opendata.arcgis.com/datasets/899c807e205244278b3f39421be8489c_0.geojson")
... ]
>>> for sql_tablename, source_url in data_to_import:
...     kwargs = {
...         "filepath": source_url,
...         "sql_tablename": sql_tablename,
...         "gpd_kwargs": {"if_exists":"replace"}
...     }
...     db.import_gis(**kwargs)
```

### 3) Run a query and get the result as a `geopandas.GeoDataFrame`

```python
>>> # Define a SQL query as a string in Python
>>> query = """
... select * from philly.high_injury_network
... where st_dwithin(
...     st_transform(geom, 26918),
...     (select st_transform(st_collect(geom), 26918) from philly.playgrounds),
...     100
... )
... order by st_length(geom) DESC """
>>> # Get a geodataframe from the db using the query
>>> gdf = db.gdf(query)
>>> gdf.head()
   index  objectid            street_name   buffer                                               geom  uid
0    234       189          BUSTLETON AVE  75 feet  LINESTRING (-75.07081 40.03528, -75.07052 40.0...  236
1     65        38                 5TH ST  50 feet  LINESTRING (-75.14528 39.96913, -75.14502 39.9...   66
2    223       179           ARAMINGO AVE  75 feet  LINESTRING (-75.12212 39.97449, -75.12132 39.9...  224
3    148       215               KELLY DR  75 feet  LINESTRING (-75.18470 39.96934, -75.18513 39.9...  150
4    156       224  MARTIN LUTHER KING DR  75 feet  LINESTRING (-75.17713 39.96327, -75.17775 39.9...  159
```

To save time and typing, database credentials can be stored in a text file. You can place this file wherever you want,
but by default it's placed into `/USERHOME/.pg-data-etl/database_connections.cfg`.

To generate one for the first time, run the following from a terminal prompt:

```shell
> pg make-config-file
```

This file uses the following format:

```
[DEFAULT]
pw = this-is-a-placeholder-password
port = 5432
super_db = postgres
super_un = postgres
super_pw = this-is-another-placeholder-password

[localhost]
host = localhost
un = postgres
pw = your-password-here
```

Each entry in square brackets is a named connection, and any parameters not explicitly defined are inherited from `DEFAULT`.
You can have as many connections defined as you'd like, and you can use them like this:

```python
>>> from pg_data_etl import Database
>>> db = Database.from_config("sample_database", "localhost")
```

## Development

Clone or fork this repo:

```bash
git clone https://github.com/aaronfraint/pg-data-etl.git
cd pg-data-etl
```

Install an editable version with `poetry`:

```bash
poetry install
```

Windows users who prefer to use `conda` can use the included `environment.yml` file:

```bash
conda env create -f environment.yml
```
