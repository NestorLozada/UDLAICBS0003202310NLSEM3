import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig

def extract_channels(db_con: Engine):
    #Dictionary for values
    channels_dict = {
        "channel_id": [],
        "channel_desc": [],
        "channel_class": [],
        "channel_class_id": []
    }
    channels_csv = pd.read_csv(DataConfig.get_csv_path('channels.csv'))
    
    #recolecta la informcion de los csv
    if not channels_csv.empty:
        for id,desc,cls,cls_id \
            in zip(
                channels_csv['CHANNEL_ID'],
                channels_csv['CHANNEL_DESC'],
                channels_csv['CHANNEL_CLASS'],
                channels_csv['CHANNEL_CLASS_ID']
            ):
            channels_dict["channel_id"].append(id)
            channels_dict["channel_desc"].append(desc)
            channels_dict["channel_class"].append(cls)
            channels_dict["channel_class_id"].append(cls_id)
    if channels_dict["channel_id"]:
        db_con.connect().execute(f'TRUNCATE TABLE channels')
        df_countries = pd.DataFrame(channels_dict)
        df_countries.to_sql('channels', db_con, if_exists="append",index=False)