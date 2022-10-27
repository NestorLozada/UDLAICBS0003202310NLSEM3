from util.connect_help import SchemaConnection, merge_and_insert, read_table

table_columns = [
    'CHANNEL_ID',
    'CHANNEL_DESC',
    'CHANNEL_CLASS',
    'CHANNEL_CLASS_ID',
]

def load_channels(schema_con: SchemaConnection, etl_process_id: int) -> None:
    channels_tra = read_table(
        table_name='CHANNELS_TRA',
        columns=table_columns,
        con=schema_con.STG,
        with_process_id=etl_process_id
    )
    channels_sor = read_table(
        table_name='CHANNELS',
        columns=['ID', *table_columns],
        con=schema_con.SOR
    )
    merge_and_insert(
        source_df=channels_tra,
        target_table='CHANNELS',
        target_df=channels_sor,
        key_columns=['CHANNEL_ID'],
        db_con=schema_con.SOR
    )