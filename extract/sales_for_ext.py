import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig

def extract_sales(db_con: Engine):
    #Dictionary for values
    sales_dict = {
        "prod_id": [],
        "cust_id": [],
        "time_id": [],
        "channel_id": [],
        "promo_id": [],
        "quantity_sold": [],
        "amount_sold": [],
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
            sales_dict['prod_id'].append(prod_id)
            sales_dict['cust_id'].append(cust_id)
            sales_dict['time_id'].append(time_id)
            sales_dict['channel_id'].append(channel_id)
            sales_dict['promo_id'].append(promo_id)
            sales_dict['quantity_sold'].append(quantity_sold)
            sales_dict['amount_sold'].append(amount_sold)
    if sales_dict["prod_id"]:
        db_con.connect().execute(f'TRUNCATE TABLE sales')
        df_sales = pd.DataFrame(sales_dict)
        df_sales.to_sql('sales', db_con, if_exists="append",index=False)