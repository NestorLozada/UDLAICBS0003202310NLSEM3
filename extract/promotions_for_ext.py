import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig

def extract_promotions(db_con: Engine):
    #Dictionary for values
    promotions_dict = {
        "promo_id": [],
        "promo_name": [],
        "promo_cost": [],
        "promo_begin_date": [],
        "promo_end_date": [],
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
            promotions_dict["promo_id"].append(id)
            promotions_dict["promo_name"].append(name)
            promotions_dict["promo_cost"].append(cost)
            promotions_dict["promo_begin_date"].append(begin_date)
            promotions_dict["promo_end_date"].append(end_date)
    if promotions_dict["promo_id"]:
        db_con.connect().execute(f'TRUNCATE TABLE promotions')
        df_promotions = pd.DataFrame(promotions_dict)
        df_promotions.to_sql('promotions', db_con, if_exists="append",index=False)