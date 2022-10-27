import functools
import traceback
from typing import List, Tuple
from sqlalchemy.engine import Engine
import pandas as pd
from util.db_connection import DbConnection
from config import DbConfig

class SchemaConnection:
    STG: Engine
    SOR: Engine

    def __init__(self):
        self.STG = self.__configure_connection(DbConfig.Schema.STG)
        self.SOR = self.__configure_connection(DbConfig.Schema.SOR)

    def begin(self):
        self.STG.begin()
        self.SOR.begin()

    def dispose(self):
        self.STG.dispose()
        self.SOR.dispose()

    def __configure_connection(self, schema: str) -> Engine:
        db_type = 'mysql'
        con_db = DbConnection(
            type = db_type,
            host = DbConfig.HOST,
            port = DbConfig.PORT,
            user = DbConfig.USER,
            password = DbConfig.PASSWORD,
            database = schema
        )
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception(f"The give database type '{db_type}' is not valid")
        elif ses_db == -2:
            raise Exception(f"Error trying connect to the database '{schema}'")
        return ses_db

def connection_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            schema_con = SchemaConnection()
            schema_con.begin()
            func(schema_con, *args, **kwargs)
            schema_con.dispose()
        except:
            traceback.print_exc()
    return wrapper

def create_etl_process(db_con: Engine) -> int:
    """Creates an ETL process record in the database and returns its ID."""
    etl_process_id = db_con.execute('INSERT INTO etl_processes VALUES ()').lastrowid
    return int(etl_process_id)

def read_table(
    table_name: str, 
    columns: List[str],
    con: Engine,
    with_process_id: int = None,
    etl_process_column: str = 'ETL_PROC_ID',
):
    """
    Reads a table from the database and returns a dataframe with the specified columns.
    If an ETL process ID is specified, it will filter the data by it.
    """
    columns_str = ', '.join(columns)
    if with_process_id is None:
        df = pd.read_sql_query(
            sql=f'SELECT {columns_str} FROM {table_name}',
            con=con
        )
    else:
        df = pd.read_sql_query(
            sql=f'SELECT {columns_str} FROM {table_name} WHERE {etl_process_column} = {with_process_id}',
            con=con
        )
    return df

def map_relationships(
    df: pd.DataFrame,
    con: Engine,
    relationships: List[Tuple[str, str, str]], # destination_column, source_table, source_column
    id_column: str = "ID",
) -> None:
    """Creates a new dataframe including the relations between the tables using their IDs."""
    mapped_df = df.copy()
    for relation in relationships:
        destination_column, source_table, source_column = relation
        source_data = read_table(
            columns=[id_column, source_column],
            table_name=source_table,
            con=con,
        )
        mapped_ids = source_data.set_index(source_column)[id_column].to_dict()
        mapped_df[destination_column] = mapped_df[source_column].apply(lambda x: mapped_ids[x])
    return mapped_df
            
def merge_and_insert(
    source_df: pd.DataFrame,
    target_table: str,
    target_df: pd.DataFrame,
    key_columns: List[str], # The columns that will be used to merge the dataframes
    db_con: Engine,
    id_column: str = "ID",
) -> None:
    """
    Merges two dataframes and inserts the new records into the target table.
    If the data already exists, it will be updated.
    Otherwise, it will be inserted.
    """
    # Remove the ID column from the target dataframe
    target_df_without_id = target_df.drop(columns=[id_column])
    
    # Merge the dataframes including
    df_result = source_df.merge(target_df_without_id, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
    
    if not df_result.empty:
        # Insert the new records
        result_indexes = df_result.set_index(key_columns).index
        target_indexes = target_df.set_index(key_columns).index
        df_to_insert = df_result[~result_indexes.isin(target_indexes)]
        if not df_to_insert.empty:
            df_to_insert.to_sql(name=target_table, con=db_con, if_exists='append', index=False)
        
        # Update the existing records
        df_to_update = df_result[result_indexes.isin(target_indexes)]
        if not df_to_update.empty:
            df_to_update = df_to_update.merge(target_df[[id_column, *key_columns]], how='left', on=key_columns).set_index(id_column) # Add ID column
            for result_indexes, row in df_to_update.iterrows():
                query_fields = []
                query_data = []
                for column, value in row.items():
                    query_fields.append(f'{column} = %s')
                    query_data.append(value)
                query_str = str(f'UPDATE {target_table} SET {", ".join(query_fields)} WHERE {id_column} = {result_indexes}')
                db_con.execute(query_str, query_data)