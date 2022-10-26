import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig

def extract_channels(db_con: Engine):
    #Dictionary for values
    channels_dict = {
        "CHANNEL_ID": [],
        "CHANNEL_DESC": [],
        "CHANNEL_CLASS": [],
        "CHANNEL_CLASS_ID": []
    }
    channels_csv = pd.read_csv(DataConfig.get_csv_path('channels.csv'))
    
    #Process CSV Content
    if not channels_csv.empty:
        for id,desc,cls,cls_id \
            in zip(
                channels_csv['CHANNEL_ID'],
                channels_csv['CHANNEL_DESC'],
                channels_csv['CHANNEL_CLASS'],
                channels_csv['CHANNEL_CLASS_ID']
            ):
            channels_dict["CHANNEL_ID"].append(id)
            channels_dict["CHANNEL_DESC"].append(desc)
            channels_dict["CHANNEL_CLASS"].append(cls)
            channels_dict["CHANNEL_CLASS_ID"].append(cls_id)
    if channels_dict["CHANNEL_ID"]:
        db_con.connect().execute(f'TRUNCATE TABLE CHANNELS_EXT')
        df_channels = pd.DataFrame(channels_dict)
        df_channels.to_sql('CHANNELS_EXT', db_con, if_exists="append",index=False)