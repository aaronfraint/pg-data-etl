# pg-data-etl

![PyPI](https://img.shields.io/pypi/v/pg-data-etl)

ETL tools for postgres data, built on top of the psql and pg_dump command line tools.

## About

This module exists to make life easier when working with geospatial data in a Postgres environment.

You should have the following command-line tools installed, preferably on your system path:

- `psql`
- `pg_dump`
- `shp2postgis`
- `ogr2ogr`

## Installation

`pip install pg_data_etl`

## Example

The following code blocks import spatial data into Postgres and runs a spatial query:

### 1) Connect to the database

```python
>>> import pg_data_etl as pg
>>> credentials = {
...     "host": "localhost",
...     "un": "username",
...     "pw": "my-password",
...     "super_un": "postgres",
...     "super_pw": "superuser-password"
... }
>>> db = pg.Database("sample_database", **credentials)
>>> db.create_db()
```

### 2) Import GIS data from the web

```python
>>> data_to_import = [
...     ("philly.high_injury_network", "https://phl.carto.com/api/v2/sql?filename=high_injury_network_2020&format=geojson&skipfields=cartodb_id&q=SELECT+*+FROM+high_injury_network_2020"),
...     ("philly.playgrounds", "https://opendata.arcgis.com/datasets/899c807e205244278b3f39421be8489c_0.geojson")
... ]
>>> for sql_tablename, source_url in data_to_import:
...     db.import_geo_file(source_url, sql_tablename)
```

### 3) Run a query and get the result as a `geopandas.GeoDataFrame`

```
>>> playground_query = """
... select * from philly.high_injury_network
... where st_dwithin(
...     st_transform(geom, 26918),
...     (select st_transform(st_collect(geom), 26918) from philly.playgrounds),
...     100
... )
... order by st_length(geom) DESC """
>>> high_injury_corridors_near_playgrounds = db.query(playground_query)
>>> high_injury_corridors_near_playgrounds.gdf.head()
   index  objectid            street_name   buffer                                               geom  uid
0    234       189          BUSTLETON AVE  75 feet  LINESTRING (-75.07081 40.03528, -75.07052 40.0...  236
1     65        38                 5TH ST  50 feet  LINESTRING (-75.14528 39.96913, -75.14502 39.9...   66
2    223       179           ARAMINGO AVE  75 feet  LINESTRING (-75.12212 39.97449, -75.12132 39.9...  224
3    148       215               KELLY DR  75 feet  LINESTRING (-75.18470 39.96934, -75.18513 39.9...  150
4    156       224  MARTIN LUTHER KING DR  75 feet  LINESTRING (-75.17713 39.96327, -75.17775 39.9...  159
```

To save time and typing, database credentials can be stored in a text file. You can place this file wherever you want,
but by default it's placed into `/USERHOME/sql_data_io/database_connections.cfg`. This file uses the following format:

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
>>> import pg_data_etl as pg
>>> credentials = pg.connections()
>>> db = pg.Database("sample_database", **credentials["localhost"])
```

## Development

Clone or fork this repo and install an editable version:

```bash
git clone https://github.com/aaronfraint/pg-data-etl.git
cd pg-data-etl
pip install --editable .
```

Windows users may find the included `environment.yml` the easiest way to install, using `conda`:

```bash
git clone https://github.com/aaronfraint/pg-data-etl.git
cd pg-data-etl
conda env create -f environment.yml
```
