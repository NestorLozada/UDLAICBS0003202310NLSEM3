from util.connect_help import SchemaConnection, map_relationships, merge_and_insert, read_table

table_columns = [
    'PROD_ID',
    'CUST_ID',
    'TIME_ID',
    'CHANNEL_ID',
    'PROMO_ID',
    'QUANTITY_SOLD',
    'AMOUNT_SOLD'
]

def load_sales(schema_con: SchemaConnection, etl_process_id: int) -> None:
    sales_tra = read_table(
        table_name='sales_tra',
        columns=table_columns,
        con=schema_con.STG,
        with_process_id=etl_process_id
    )
    sales_sor = read_table(
        table_name='sales',
        columns=['ID', *table_columns],
        con=schema_con.SOR
    )
    sales_with_relationships = map_relationships(
        df=sales_tra,
        con=schema_con.SOR,
        relationships=[
            ('PROD_ID', 'products', 'PROD_ID'),
            ('CUST_ID', 'customers', 'CUST_ID'),
            ('TIME_ID', 'times', 'TIME_ID'),
            ('CHANNEL_ID', 'channels', 'CHANNEL_ID'),
            ('PROMO_ID', 'promotions', 'PROMO_ID')
        ]
    )
    merge_and_insert(
        source_df=sales_with_relationships,
        target_table='sales',
        target_df=sales_sor,
        key_columns=['PROD_ID', 'CUST_ID', 'TIME_ID', 'CHANNEL_ID', 'PROMO_ID'],
        db_con=schema_con.SOR
    )