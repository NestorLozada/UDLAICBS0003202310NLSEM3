
from config import DbConfig
from sqlalchemy.engine import Engine
from util.db_simpleConnection import simple_try
from extract.chanels_for_ext import extract_channels
from extract.countries_fir_ext import extract_countries
from extract.customers_for_ext import extract_customers
from extract.products_for_ext import extract_products
from extract.promotions_for_ext import extract_promotions
from extract.sales_for_ext import extract_sales
from extract.times_for_ext import extract_times

@simple_try(DbConfig.Schema.STG)
def extract(db_con: Engine):
    extract_times(db_con)
    extract_channels(db_con)
    extract_countries(db_con)
    extract_promotions(db_con)
    extract_customers(db_con)
    extract_products(db_con)
    extract_sales(db_con)