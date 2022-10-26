import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig

def extract_sales(db_con: Engine):
    #Dictionary for values
    sales_dict = {
        "PROD_ID": [],
        "CUST_ID": [],
        "TIME_ID": [],
        "CHANNEL_ID": [],
        "PROMO_ID": [],
        "QUANTITY_SOLD": [],
        "AMOUNT_SOLD": [],
    }
    sales_csv = pd.read_csv(DataConfig.get_csv_path('sales.csv'))
    
    #Process CSV Content
    if not sales_csv.empty:
        for prod_id,cust_id,time_id, \
            channel_id,promo_id, \
            quantity_sold,amount_sold \
            in zip(
                sales_csv['PROD_ID'],
                sales_csv['CUST_ID'],
                sales_csv['TIME_ID'],
                sales_csv['CHANNEL_ID'],
                sales_csv['PROMO_ID'],
                sales_csv['QUANTITY_SOLD'],
                sales_csv['AMOUNT_SOLD']
            ):
            sales_dict['PROD_ID'].append(prod_id)
            sales_dict['CUST_ID'].append(cust_id)
            sales_dict['TIME_ID'].append(time_id)
            sales_dict['CHANNEL_ID'].append(channel_id)
            sales_dict['PROMO_ID'].append(promo_id)
            sales_dict['QUANTITY_SOLD'].append(quantity_sold)
            sales_dict['AMOUNT_SOLD'].append(amount_sold)
    if sales_dict["PROD_ID"]:
        db_con.connect().execute(f'TRUNCATE TABLE SALES_EXT')
        df_sales = pd.DataFrame(sales_dict)
        df_sales.to_sql('SALES_EXT', db_con, if_exists="append",index=False)