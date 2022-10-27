from sqlalchemy.engine import Engine
from transform.chanels_for_trans import transform_channels
from transform.countries_for_trans import transform_countries
from transform.customers_for_trans import transform_customers
from transform.products_for_trant import transform_products
from transform.promotions_for_trans import transform_promotions
from transform.sales_for_trans import transform_sales
from transform.times_for_trans import transform_times

def transform(db_con: Engine, etl_process_id: int):
    transform_times(db_con, etl_process_id)
    transform_channels(db_con, etl_process_id)
    transform_countries(db_con, etl_process_id)
    transform_promotions(db_con, etl_process_id)
    transform_customers(db_con, etl_process_id)
    transform_products(db_con, etl_process_id)
    transform_sales(db_con, etl_process_id)