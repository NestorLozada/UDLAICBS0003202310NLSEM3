from sqlalchemy.engine import Engine
from util.connect_help import read_table

table_columns = [
    'COUNTRY_ID',
    'COUNTRY_NAME',
    'COUNTRY_REGION',
    'COUNTRY_REGION_ID',
]

def transform_countries(db_con: Engine, etl_process_id: int) -> None:
    # Read from extract table
    countries_ext = read_table(
        table_name='COUNTRIES_EXT',
        columns=table_columns,
        con=db_con
    )

    df_countries = countries_ext.copy(deep=True)
    if not countries_ext.empty:
        # Tranform columns
        df_countries['COUNTRY_ID'] = df_countries['COUNTRY_ID'].astype(int)
        df_countries['COUNTRY_REGION_ID'] = df_countries['COUNTRY_REGION_ID'].astype(int)
        # Add ETL process ID
        df_countries['ETL_PROC_ID'] = etl_process_id
        # Write to transform table
        df_countries.to_sql('COUNTRIES_TRA', con=db_con, if_exists='append',index=False)