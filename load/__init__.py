
from util.connect_help import SchemaConnection
from load.channels_for_load import load_channels
from load.countries_for_load import load_countries
from load.customers_for_load import load_customers
from load.products_for_load import load_products
from load.promotions_for_load import load_promotions
from load.sales_for_load import load_sales
from load.times_for_load import load_times


def load(schema_con: SchemaConnection, etl_process_id: int):
    load_times(schema_con, etl_process_id)
    load_channels(schema_con, etl_process_id)
    load_countries(schema_con, etl_process_id)
    load_promotions(schema_con, etl_process_id)
    load_customers(schema_con, etl_process_id)
    load_products(schema_con, etl_process_id)
    load_sales(schema_con, etl_process_id)