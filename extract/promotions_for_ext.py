import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig


def extract_promotions(db_con: Engine):
    #Dictionary for values
    promotions_dict = {
        "PROMO_ID": [],
        "PROMO_NAME": [],
        "PROMO_COST": [],
        "PROMO_BEGIN_DATE": [],
        "PROMO_END_DATE": [],
    }
    promotions_csv = pd.read_csv(DataConfig.get_csv_path('promotions.csv'))
    
    #Process CSV Content
    if not promotions_csv.empty:
        for id,name,cost, \
            begin_date,end_date \
            in zip(
                promotions_csv['PROMO_ID'],
                promotions_csv['PROMO_NAME'],
                promotions_csv['PROMO_COST'],
                promotions_csv['PROMO_BEGIN_DATE'],
                promotions_csv['PROMO_END_DATE']
            ):
            promotions_dict["PROMO_ID"].append(id)
            promotions_dict["PROMO_NAME"].append(name)
            promotions_dict["PROMO_COST"].append(cost)
            promotions_dict["PROMO_BEGIN_DATE"].append(begin_date)
            promotions_dict["PROMO_END_DATE"].append(end_date)
    if promotions_dict["PROMO_ID"]:
        db_con.connect().execute(f'TRUNCATE TABLE PROMOTIONS_EXT')
        df_promotions = pd.DataFrame(promotions_dict)
        df_promotions.to_sql('PROMOTIONS_EXT', db_con, if_exists="append",index=False)