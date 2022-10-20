import functools
import traceback
from utilsconnection.db_connection import DbConnection
from config import DbConfig

def simple_try(schema: str):
    db_type = "mysql"
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                con_db = DbConnection(
                    type=db_type,
                    host=DbConfig.HOST,
                    port=DbConfig.PORT,
                    user=DbConfig.USER,
                    password=DbConfig.PASSWORD,
                    database=schema
                )
                ses_db = con_db.start()
                if ses_db == -1:
                    raise Exception(f"The give database type {db_type} is not valid")
                elif ses_db == -2:
                    raise Exception("Error trying connect to the database")
                ses_db.begin()
                func(ses_db, *args, **kwargs)
                ses_db.dispose()
            except:
                traceback.print_exc()
        return wrapper
    return decorator