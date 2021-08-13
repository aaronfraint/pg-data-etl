# def test_spatial_copy_table_to(local_db, local_db_with_spatial_data):
#     """Copy a spatial table from a local db to another local db """

#     local_db_with_spatial_data.copy_table_to("circuittrails", local_db)

#     assert "public.circuittrails" in local_db.list_of_tables(spatial_only=True)
