import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig

def extract_countries(db_con: Engine):
    #Dictionary for values
    countries_dict = {
        "COUNTRY_ID": [],
        "COUNTRY_NAME": [],
        "COUNTRY_REGION": [],
        "COUNTRY_REGION_ID": []
    }
    countries_csv = pd.read_csv(DataConfig.get_csv_path('countries.csv'))
    
    #Process CSV Content
    if not countries_csv.empty:
        for id,name,reg,reg_id \
            in zip(
                countries_csv['COUNTRY_ID'],
                countries_csv['COUNTRY_NAME'],
                countries_csv['COUNTRY_REGION'],
                countries_csv['COUNTRY_REGION_ID']
            ):
            countries_dict["COUNTRY_ID"].append(id)
            countries_dict["COUNTRY_NAME"].append(name)
            countries_dict["COUNTRY_REGION"].append(reg)
            countries_dict["COUNTRY_REGION_ID"].append(reg_id)
    if countries_dict["COUNTRY_ID"]:
        db_con.connect().execute(f'TRUNCATE TABLE COUNTRIES_EXT')
        df_countries = pd.DataFrame(countries_dict)
        df_countries.to_sql('COUNTRIES_EXT', db_con, if_exists="append",index=False)