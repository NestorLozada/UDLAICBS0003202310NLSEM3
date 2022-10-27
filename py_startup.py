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
    print('EXTRAYENDO INFO...')
    extract.extract(schema_con.STG)
    print('TRANFORMANDO INFO...')
    transform.transform(schema_con.STG, process_id)
    print('CARGANDO INFO...')
    load.load(schema_con, process_id)
    end = time.time()
    print(f'ETL FINALIZÓ EN {end - start:.4f} SEGUNDOS')

try:
    main()
except:
    print("HUBO UN ERROR MIENTRAS SE EJECTUABA EL PROCESO ETL:")
    traceback.print_exc()