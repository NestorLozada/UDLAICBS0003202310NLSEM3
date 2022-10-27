from sqlalchemy.engine import Engine
from util.connect_help import read_table
from transform.capitalization import format_date

table_columns = [
    'PROD_ID',
    'CUST_ID',
    'TIME_ID',
    'CHANNEL_ID',
    'PROMO_ID',
    'QUANTITY_SOLD',
    'AMOUNT_SOLD'
]

def transform_sales(db_con: Engine, etl_process_id: int) -> None:
    # Read from extract table
    sales_ext = read_table(
        table_name='SALES_EXT',
        columns=table_columns,
        con=db_con
    )

    df_sales = sales_ext.copy(deep=True)
    if not sales_ext.empty:
        # Tranform columns
        df_sales['PROD_ID'] = df_sales['PROD_ID'].astype(int)
        df_sales['CUST_ID'] = df_sales['CUST_ID'].astype(int)
        df_sales['TIME_ID'] = df_sales['TIME_ID'].apply(format_date)
        df_sales['CHANNEL_ID'] = df_sales['CHANNEL_ID'].astype(int)
        df_sales['PROMO_ID'] = df_sales['PROMO_ID'].astype(int)
        df_sales['QUANTITY_SOLD'] = df_sales['QUANTITY_SOLD'].astype(float)
        df_sales['AMOUNT_SOLD'] = df_sales['AMOUNT_SOLD'].astype(float)
        # Add ETL process ID
        df_sales['ETL_PROC_ID'] = etl_process_id
        # Write to transform table
        df_sales.to_sql('sales_tra', con=db_con, if_exists='append',index=False)
