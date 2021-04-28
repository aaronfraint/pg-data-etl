# """
# Create a URI and super URI that get used across the class.
# """

# self._uri = f"postgresql://{un}:{pw}@{host}:{port}/{db_name}"

# self._super_uri = f"postgresql://{super_un}:{super_pw}@{host}:{port}/{super_db}"

# self.pg_dump = "pg_dump" if not pg_dump_path else pg_dump_path

# self.params = {
#     "un": un,
#     "pw": pw,
#     "host": host,
#     "db_name": db_name,
#     "port": port,
#     "super_un": super_un,
#     "super_pw": super_pw,
#     "super_db": super_db,
# }

# # ACCESS

# def uri(self, super_uri: bool = False) -> str:
#     """
#     Return the normal URI by default.
#     Return the super database URI if super_uri = True
#     """

#     if super_uri:
#         return self._super_uri
#     else:
#         return self._uri

# def execute(self, query: str, super_uri: bool = False) -> None:
#     helpers.execute_via_psycopg2(self, query, super_uri=super_uri)

# # GET DATA OUT VIA QUERY

# def query(self, q: str, geo: bool = None, query_kwargs: dict = None) -> Query:
#     """
#     Run a query in the database and return a Query() object.

#     Leave 'geo' as None if you want the query to figure out
#     whether to return a geodataframe or regular dataframe.

#     If you specifically want a dataframe, set geo=False
#     Likewise, set geo=True if you definitely want a geodataframe

#     If your spatial query uses a non-standard geom column (i.e. 'shape')
#     you'll need to pass a dictionary to 'query_kwargs'. For example:
#         >>> db = Database('my_db')
#         >>> db.query('select geom as shape from circuittrails', query_kwargs={'geom_col': 'shape'})

#     A more traditional query with 'geom' requires less typing:
#         >>> db.query('select geom from circuittrails')
#     """

#     if query_kwargs:
#         query = Query(self, q, **query_kwargs)
#     else:
#         query = Query(self, q)

#     # Use the user's geo flag, if they provided one
#     if geo is not None:
#         geo_flag = geo

#     # If none provided, try to guess if it's spatial
#     else:
#         geo_flag = query.is_spatial()

#     # If spatial, return a geodataframe
#     if geo_flag:
#         query.get_gdf()
#     # If not spatial, return a pandas dataframe
#     else:
#         query.get_df()

#     return query

# # IMPORT PANDAS & GEOPANDAS

# # CANNED QUERIES THAT GET USED A LOT

# # REPORTS

# def report_spatial(self, print_output: bool = False) -> dict:
#     query = "select concat(f_table_schema, '.', f_table_name), srid, type from geometry_columns"

#     results = self.query_via_psycopg2(query)

#     output = {}

#     for row in results:
#         tbl, epsg, geom_type = row

#         if epsg not in output:
#             output[epsg] = {}

#         if geom_type not in output[epsg]:
#             output[epsg][geom_type] = []

#         output[epsg][geom_type].append(tbl)

#     print("-" * 80)
#     print(f"Spatial Data Report for DB: {self.uri()}")

#     epsg_list = [x for x in output.keys()]
#     if len(epsg_list) < 2:
#         print(f"\t-> All data is stored in {epsg_list[0]}")
#     else:
#         print(f"\t-> Data is stored in {len(epsg_list)} projections: {epsg_list}")

#     for k in output.keys():
#         print(f"\t-> EPSG: {k}")
#         for geom_type in output[k]:
#             print(f"\t\t->{geom_type}")
#             for tbl in output[k][geom_type]:
#                 print(f"\t\t\t-> {tbl}")

#     return output

# def get_projection(self, tablename: str) -> int:
#     """
#     - Get the projection of a spatial table

#     Args:
#         tablename (str): name of table to check, optionally with schema prefix

#     Returns:
#         EPSG of table
#     """
#     schema, tbl = _convert_full_tablename_to_parts(tablename)

#     query = f"""
#         select srid
#         from geometry_columns
#         where f_table_schema = '{schema}'
#         and f_table_name = '{tbl}'
#     """

#     result = self.query_via_psycopg2(query)
#     data_epsg = result[0][0]

#     return data_epsg

# def check_projection(self, tablename: str, epsg: int) -> bool:
#     """
#     - Check a table to see if the projection matches an expected value

#     Args:
#         tablename (str): name of table to check, optionally with schema prefix

#     Returns:
#         `True` or `False`, depending on whether the `epsg` matches the table
#     """

#     return self.get_projection(tablename) == epsg
