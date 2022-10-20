import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig

def extract_customers(db_con: Engine):
    #Dictionary for values
    customers_dict = {
        "cust_id": [],
        "cust_first_name": [],
        "cust_last_name": [],
        "cust_gender": [],
        "cust_year_of_birth": [],
        "cust_marital_status": [],
        "cust_street_address": [],
        "cust_postal_code": [],
        "cust_city": [],
        "cust_state_province": [],
        "country_id": [],
        "cust_main_phone_integer": [],
        "cust_income_level": [],
        "cust_credit_limit": [],
        "cust_email": [],
    }
    customers_csv = pd.read_csv(DataConfig.get_csv_path('customers.csv'))
    
    #Process CSV Content
    if not customers_csv.empty:
        for id,first_name,last_name, \
            gender,birth,marital_status, \
            street_address,postal_code, \
            city,state_province,country_id, \
            main_phone,income_level, \
            credit_limit,email \
            in zip(
                customers_csv['CUST_ID'],
                customers_csv['CUST_FIRST_NAME'],
                customers_csv['CUST_LAST_NAME'],
                customers_csv['CUST_GENDER'],
                customers_csv['CUST_YEAR_OF_BIRTH'],
                customers_csv['CUST_MARITAL_STATUS'],
                customers_csv['CUST_STREET_ADDRESS'],
                customers_csv['CUST_POSTAL_CODE'],
                customers_csv['CUST_CITY'],
                customers_csv['CUST_STATE_PROVINCE'],
                customers_csv['COUNTRY_ID'],
                customers_csv['CUST_MAIN_PHONE_NUMBER'],
                customers_csv['CUST_INCOME_LEVEL'],
                customers_csv['CUST_CREDIT_LIMIT'],
                customers_csv['CUST_EMAIL']
            ):
            customers_dict["cust_id"].append(id)
            customers_dict["cust_first_name"].append(first_name)
            customers_dict["cust_last_name"].append(last_name)
            customers_dict["cust_gender"].append(gender)
            customers_dict["cust_year_of_birth"].append(birth)
            customers_dict["cust_marital_status"].append(marital_status)
            customers_dict["cust_street_address"].append(street_address)
            customers_dict["cust_postal_code"].append(postal_code)
            customers_dict["cust_city"].append(city)
            customers_dict["cust_state_province"].append(state_province)
            customers_dict["country_id"].append(country_id)
            customers_dict["cust_main_phone_integer"].append(main_phone)
            customers_dict["cust_income_level"].append(income_level)
            customers_dict["cust_credit_limit"].append(credit_limit)
            customers_dict["cust_email"].append(email)
    if customers_dict["cust_id"]:
        db_con.connect().execute(f'TRUNCATE TABLE customers')
        df_customers = pd.DataFrame(customers_dict)
        df_customers.to_sql('customers', db_con, if_exists="append",index=False)