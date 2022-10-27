from util.connect_help import SchemaConnection, merge_and_insert, read_table

table_columns =  [
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

def load_times(schema_con: SchemaConnection, etl_process_id: int) -> None:
    times_tra = read_table(
        table_name='times_tra',
        columns=table_columns,
        con=schema_con.STG,
        with_process_id=etl_process_id
    )
    times_sor = read_table(
        table_name='times',
        columns=['ID', *table_columns],
        con=schema_con.SOR
    )
    merge_and_insert(
        source_df=times_tra,
        target_table='times',
        target_df=times_sor,
        key_columns=['TIME_ID'],
        db_con=schema_con.SOR
    )