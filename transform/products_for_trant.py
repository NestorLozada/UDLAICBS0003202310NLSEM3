from sqlalchemy.engine import Engine
from util.connect_help import read_table

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

def transform_products(db_con: Engine, etl_process_id: int) -> None:
    # Read from extract table
    products_ext = read_table(
        table_name='PRODUCTS_EXT',
        columns=table_columns,
        con=db_con
    )

    df_products = products_ext.copy(deep=True)
    if not products_ext.empty:
        # Tranform columns
        df_products['PROD_ID'] = df_products['PROD_ID'].astype(int)
        df_products['PROD_CATEGORY_ID'] = df_products['PROD_CATEGORY_ID'].astype(int)
        df_products['PROD_WEIGHT_CLASS'] = df_products['PROD_WEIGHT_CLASS'].astype(int)
        df_products['SUPPLIER_ID'] = df_products['SUPPLIER_ID'].astype(int)
        df_products['PROD_LIST_PRICE'] = df_products['PROD_LIST_PRICE'].astype(float)
        df_products['PROD_MIN_PRICE'] = df_products['PROD_MIN_PRICE'].astype(float)
        # Add ETL process ID
        df_products['ETL_PROC_ID'] = etl_process_id
        # Write to transform table
        df_products.to_sql('products_tra', con=db_con, if_exists='append',index=False)
