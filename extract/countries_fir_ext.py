import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig

def extract_countries(db_con: Engine):
    #Dictionary for values
    countries_dict = {
        "country_id": [],
        "country_name": [],
        "country_region": [],
        "country_region_id": []
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
            countries_dict["country_id"].append(id)
            countries_dict["country_name"].append(name)
            countries_dict["country_region"].append(reg)
            countries_dict["country_region_id"].append(reg_id)
    if countries_dict["country_id"]:
        db_con.connect().execute(f'TRUNCATE TABLE countries_ext')
        df_countries = pd.DataFrame(countries_dict)
        df_countries.to_sql('countries_ext', db_con, if_exists="append",index=False)