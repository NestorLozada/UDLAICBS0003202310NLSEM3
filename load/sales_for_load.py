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
        table_name='SALES_TRA',
        columns=table_columns,
        con=schema_con.STG,
        with_process_id=etl_process_id
    )
    sales_sor = read_table(
        table_name='SALES',
        columns=['ID', *table_columns],
        con=schema_con.SOR
    )
    sales_with_relationships = map_relationships(
        df=sales_tra,
        con=schema_con.SOR,
        relationships=[
            ('PROD_ID', 'PRODUCTS', 'PROD_ID'),
            ('CUST_ID', 'CUSTOMERS', 'CUST_ID'),
            ('TIME_ID', 'TIMES', 'TIME_ID'),
            ('CHANNEL_ID', 'CHANNELS', 'CHANNEL_ID'),
            ('PROMO_ID', 'PROMOTIONS', 'PROMO_ID')
        ]
    )
    merge_and_insert(
        source_df=sales_with_relationships,
        target_table='SALES',
        target_df=sales_sor,
        key_columns=['PROD_ID', 'CUST_ID', 'TIME_ID', 'CHANNEL_ID', 'PROMO_ID'],
        db_con=schema_con.SOR
    )