import time
import traceback
import extract
import transform
import load
from util.connect_help import SchemaConnection,  connection_handler, create_etl_process

@connection_handler
def main(schema_con: SchemaConnection):
    start = time.time()
    process_id = create_etl_process(schema_con.STG)
    print(f'ETL process N°{process_id}')
    print('Extracting data...')
    extract.extract(schema_con.STG)
    print('Transforming data...')
    transform.transform(schema_con.STG, process_id)
    print('Loading data...')
    load.load(schema_con, process_id)
    end = time.time()
    print(f'ETL process finished in {end - start:.4f} seconds')

try:
    main()
except:
    print("An error occurred while running the ETL process:")
    traceback.print_exc()