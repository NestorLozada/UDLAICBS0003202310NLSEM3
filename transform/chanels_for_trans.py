from sqlalchemy.engine import Engine
from util.connect_help import read_table

table_columns = [
    'CHANNEL_ID',
    'CHANNEL_DESC',
    'CHANNEL_CLASS',
    'CHANNEL_CLASS_ID',
]

def transform_channels(db_con: Engine, etl_process_id: int) -> None:
    # Read from extract table
    channels_ext = read_table(
        table_name='CHANNELS_EXT',
        columns=table_columns,
        con=db_con
    )

    df_channels = channels_ext.copy(deep=True)
    if not channels_ext.empty:
        # Tranform columns
        df_channels['CHANNEL_ID'] = df_channels['CHANNEL_ID'].astype(int)
        df_channels['CHANNEL_CLASS_ID'] = df_channels['CHANNEL_CLASS_ID'].astype(int)
        # Add ETL process ID
        df_channels['ETL_PROC_ID'] = etl_process_id
        # Write to transform table
        df_channels.to_sql('channels_tra', con=db_con, if_exists='append',index=False)