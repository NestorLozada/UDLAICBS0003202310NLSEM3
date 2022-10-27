from sqlalchemy.engine import Engine
from util.connect_help import read_table
from transform.capitalization import get_month, format_date

table_columns = [
    'TIME_ID',
    'DAY_NAME',
    'DAY_INTEGER_IN_WEEK',
    'DAY_INTEGER_IN_MONTH',
    'CALENDAR_WEEK_INTEGER',
    'CALENDAR_MONTH_INTEGER',
    'CALENDAR_MONTH_DESC',
    'END_OF_CAL_MONTH',
    'CALENDAR_MONTH_NAME',
    'CALENDAR_QUARTER_DESC',
    'CALENDAR_YEAR'
]

def transform_times(db_con: Engine, etl_process_id: int) -> None:
    # Read from extract table
    times_ext = read_table(
        table_name='TIMES_EXT',
        columns=table_columns,
        con=db_con
    )

    df_times = times_ext.copy(deep=True)
    if not times_ext.empty:
        # Tranform columns
        df_times['TIME_ID'] = df_times['TIME_ID'].apply(format_date)
        df_times['DAY_INTEGER_IN_WEEK'] = df_times['DAY_INTEGER_IN_WEEK'].astype(int)
        df_times['DAY_INTEGER_IN_MONTH'] = df_times['DAY_INTEGER_IN_MONTH'].astype(int)
        df_times['CALENDAR_WEEK_INTEGER'] = df_times['CALENDAR_WEEK_INTEGER'].astype(int)
        df_times['CALENDAR_MONTH_INTEGER'] = df_times['CALENDAR_MONTH_INTEGER'].astype(int)
        df_times['END_OF_CAL_MONTH'] = df_times['END_OF_CAL_MONTH'].apply(format_date)
        df_times['CALENDAR_YEAR'] = df_times['CALENDAR_YEAR'].astype(int)
        df_times['CALENDAR_MONTH_NAME'] = df_times['CALENDAR_MONTH_INTEGER'].apply(get_month)
        # Add ETL process ID
        df_times['ETL_PROC_ID'] = etl_process_id
        # Write to transform table
        df_times.to_sql('times_tra', con=db_con, if_exists='append',index=False)