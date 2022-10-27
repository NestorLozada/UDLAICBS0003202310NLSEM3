from sqlalchemy.engine import Engine
from util.connect_help import read_table

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

def transform_customers(db_con: Engine, etl_process_id: int) -> None:
    # Read from extract table
    customers_ext = read_table(
        table_name='CUSTOMERS_EXT',
        columns=table_columns,
        con=db_con
    )

    df_customers = customers_ext.copy(deep=True)
    if not customers_ext.empty:
        # Tranform columns
        df_customers['CUST_ID'] = df_customers['CUST_ID'].astype(int)
        df_customers['CUST_YEAR_OF_BIRTH'] = df_customers['CUST_YEAR_OF_BIRTH'].astype(int)
        df_customers['COUNTRY_ID'] = df_customers['COUNTRY_ID'].astype(int)
        df_customers['CUST_CREDIT_LIMIT'] = df_customers['CUST_CREDIT_LIMIT'].astype(int)
        # Add ETL process ID
        df_customers['ETL_PROC_ID'] = etl_process_id
        # Write to transform table
        df_customers.to_sql('customers_tra', con=db_con, if_exists='append',index=False)
