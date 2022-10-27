from util.connect_help import SchemaConnection, merge_and_insert, read_table

table_columns = [
    'PROD_ID',
    'PROD_NAME',
    'PROD_DESC',
    'PROD_CATEGORY',
    'PROD_CATEGORY_ID',
    'PROD_CATEGORY_DESC',
    'PROD_WEIGHT_CLASS',
    'SUPPLIER_ID',
    'PROD_STATUS',
    'PROD_LIST_PRICE',
    'PROD_MIN_PRICE'
]

def load_products(schema_con: SchemaConnection, etl_process_id: int) -> None:
    products_tra = read_table(
        table_name='PRODUCTS_TRA',
        columns=table_columns,
        con=schema_con.STG,
        with_process_id=etl_process_id
    )
    products_sor = read_table(
        table_name='PRODUCTS',
        columns=['ID', *table_columns],
        con=schema_con.SOR
    )
    merge_and_insert(
        source_df=products_tra,
        target_table='PRODUCTS',
        target_df=products_sor,
        key_columns=['PROD_ID'],
        db_con=schema_con.SOR
    )