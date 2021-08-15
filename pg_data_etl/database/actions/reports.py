from pg_data_etl import helpers


def report_spatial(self, print_output: bool = False) -> dict:
    """
    - Get a report of all spatial table in the database
    - The report groups tables first by EPSG code, then by geometry type
    - All tables that share an EPSG and geometry type are grouped into a list

    Arguments:
        print_output (bool): flag to control if the report should also be printed out to console

    Returns:
        dict: keyed on EPSG code, with a sub-dictionary keyed on geometry type. Value is a list of tables
    """
    query = "select concat(f_table_schema, '.', f_table_name), srid, type from geometry_columns"

    results = self.query_as_list_of_lists(query)

    output = {}

    for row in results:
        tbl, epsg, geom_type = row

        if epsg not in output:
            output[epsg] = {}

        if geom_type not in output[epsg]:
            output[epsg][geom_type] = []

        output[epsg][geom_type].append(tbl)

    if print_output:

        print("-" * 80)
        print(f"Spatial Data Report for DB: {self.uri}")

        epsg_list = [x for x in output.keys()]
        if len(epsg_list) < 2:
            print(f"\t-> All data is stored in {epsg_list[0]}")
        else:
            print(f"\t-> Data is stored in {len(epsg_list)} projections: {epsg_list}")

        for k in output.keys():
            print(f"\t-> EPSG: {k}")
            for geom_type in output[k]:
                print(f"\t\t->{geom_type}")
                for tbl in output[k][geom_type]:
                    print(f"\t\t\t-> {tbl}")

    return output


def projection(self, tablename: str) -> int:
    """
    - Get the projection of a spatial table

    Arguments:
        tablename (str): name of table to check, optionally with schema prefix

    Returns:
        EPSG of table
    """
    schema, tbl = helpers.convert_full_tablename_to_parts(tablename)

    query = f"""
        select srid
        from geometry_columns
        where f_table_schema = '{schema}'
        and f_table_name = '{tbl}'
    """

    return self.query_as_singleton(query)
