import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig


def extract_products(db_con: Engine):
    #Dictionary for values
    products_dict = {
        "prod_id": [],
        "prod_name": [],
        "prod_desc": [],
        "prod_category": [],
        "prod_category_id": [],
        "prod_category_desc": [],
        "prod_weight_class": [],
        "supplier_id": [],
        "prod_status": [],
        "prod_list_price": [],
        "prod_min_price": [],
    }
    products_csv = pd.read_csv(DataConfig.get_csv_path('products.csv'))
    
    #Process CSV Content
    if not products_csv.empty:
        for id,name,desc, \
            category,category_id,category_desc, \
            weight_class,supplier_id, \
            status,list_price,min_price \
            in zip(
                products_csv['PROD_ID'],
                products_csv['PROD_NAME'],
                products_csv['PROD_DESC'],
                products_csv['PROD_CATEGORY'],
                products_csv['PROD_CATEGORY_ID'],
                products_csv['PROD_CATEGORY_DESC'],
                products_csv['PROD_WEIGHT_CLASS'],
                products_csv['SUPPLIER_ID'],
                products_csv['PROD_STATUS'],
                products_csv['PROD_LIST_PRICE'],
                products_csv['PROD_MIN_PRICE']
            ):
            products_dict["prod_id"].append(id)
            products_dict["prod_name"].append(name)
            products_dict["prod_desc"].append(desc)
            products_dict["prod_category"].append(category)
            products_dict["prod_category_id"].append(category_id)
            products_dict["prod_category_desc"].append(category_desc)
            products_dict["prod_weight_class"].append(weight_class)
            products_dict["supplier_id"].append(supplier_id)
            products_dict["prod_status"].append(status)
            products_dict["prod_list_price"].append(list_price)
            products_dict["prod_min_price"].append(min_price)
    if products_dict["prod_id"]:
        db_con.connect().execute(f'TRUNCATE TABLE products')
        df_products = pd.DataFrame(products_dict)
        df_products.to_sql('products', db_con, if_exists="append",index=False)