from util.connect_help import SchemaConnection, map_relationships, merge_and_insert, read_table

table_columns = [
    'CUST_ID',
    'CUST_FIRST_NAME',
    'CUST_LAST_NAME',
    'CUST_GENDER',
    'CUST_YEAR_OF_BIRTH',
    'CUST_MARITAL_STATUS',
    'CUST_STREET_ADDRESS',
    'CUST_POSTAL_CODE',
    'CUST_CITY',
    'CUST_STATE_PROVINCE',
    'COUNTRY_ID',
    'CUST_MAIN_PHONE_INTEGER',
    'CUST_INCOME_LEVEL',
    'CUST_CREDIT_LIMIT',
    'CUST_EMAIL',
]

def load_customers(schema_con: SchemaConnection, etl_process_id: int) -> None:
    customers_tra = read_table(
        table_name='customers_tra',
        columns=table_columns,
        con=schema_con.STG,
        with_process_id=etl_process_id
    )
    customers_sor = read_table(
        table_name='customers',
        columns=['ID', *table_columns],
        con=schema_con.SOR
    )
    customers_with_relationships = map_relationships(
        df=customers_tra,
        con=schema_con.SOR,
        relationships=[('COUNTRY_ID', 'countries', 'COUNTRY_ID')]
    )
    merge_and_insert(
        source_df=customers_with_relationships,
        target_table='customers',
        target_df=customers_sor,
        key_columns=['CUST_ID'],
        db_con=schema_con.SOR
    )