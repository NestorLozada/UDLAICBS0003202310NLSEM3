import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig

def extract_customers(db_con: Engine):
    #Dictionary for values
    customers_dict = {
        "CUST_ID": [],
        "CUST_FIRST_NAME": [],
        "CUST_LAST_NAME": [],
        "CUST_GENDER": [],
        "CUST_YEAR_OF_BIRTH": [],
        "CUST_MARITAL_STATUS": [],
        "CUST_STREET_ADDRESS": [],
        "CUST_POSTAL_CODE": [],
        "CUST_CITY": [],
        "CUST_STATE_PROVINCE": [],
        "COUNTRY_ID": [],
        "CUST_MAIN_PHONE_INTEGER": [],
        "CUST_INCOME_LEVEL": [],
        "CUST_CREDIT_LIMIT": [],
        "CUST_EMAIL": [],
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
            customers_dict["CUST_ID"].append(id)
            customers_dict["CUST_FIRST_NAME"].append(first_name)
            customers_dict["CUST_LAST_NAME"].append(last_name)
            customers_dict["CUST_GENDER"].append(gender)
            customers_dict["CUST_YEAR_OF_BIRTH"].append(birth)
            customers_dict["CUST_MARITAL_STATUS"].append(marital_status)
            customers_dict["CUST_STREET_ADDRESS"].append(street_address)
            customers_dict["CUST_POSTAL_CODE"].append(postal_code)
            customers_dict["CUST_CITY"].append(city)
            customers_dict["CUST_STATE_PROVINCE"].append(state_province)
            customers_dict["COUNTRY_ID"].append(country_id)
            customers_dict["CUST_MAIN_PHONE_INTEGER"].append(main_phone)
            customers_dict["CUST_INCOME_LEVEL"].append(income_level)
            customers_dict["CUST_CREDIT_LIMIT"].append(credit_limit)
            customers_dict["CUST_EMAIL"].append(email)
    if customers_dict["CUST_ID"]:
        db_con.connect().execute(f'TRUNCATE TABLE CUSTOMERS_EXT')
        df_customers = pd.DataFrame(customers_dict)
        df_customers.to_sql('CUSTOMERS_EXT', db_con, if_exists="append",index=False)