import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig


def extract_products(db_con: Engine):
    #Dictionary for values
    products_dict = {
        "PROD_ID": [],
        "PROD_NAME": [],
        "PROD_DESC": [],
        "PROD_CATEGORY": [],
        "PROD_CATEGORY_ID": [],
        "PROD_CATEGORY_DESC": [],
        "PROD_WEIGHT_CLASS": [],
        "SUPPLIER_ID": [],
        "PROD_STATUS": [],
        "PROD_LIST_PRICE": [],
        "PROD_MIN_PRICE": [],
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
            products_dict["PROD_ID"].append(id)
            products_dict["PROD_NAME"].append(name)
            products_dict["PROD_DESC"].append(desc)
            products_dict["PROD_CATEGORY"].append(category)
            products_dict["PROD_CATEGORY_ID"].append(category_id)
            products_dict["PROD_CATEGORY_DESC"].append(category_desc)
            products_dict["PROD_WEIGHT_CLASS"].append(weight_class)
            products_dict["SUPPLIER_ID"].append(supplier_id)
            products_dict["PROD_STATUS"].append(status)
            products_dict["PROD_LIST_PRICE"].append(list_price)
            products_dict["PROD_MIN_PRICE"].append(min_price)
    if products_dict["PROD_ID"]:
        db_con.connect().execute(f'TRUNCATE TABLE PRODUCTS_EXT')
        df_products = pd.DataFrame(products_dict)
        df_products.to_sql('PRODUCTS_EXT', db_con, if_exists="append",index=False)