import os
from jproperties import Properties

from utilsconnection.db_connection import Db_Connection

def __read_config(file_path: str):
    data_configs = Properties()
    with open(file_path, 'rb') as config_file:
        data_configs.load(config_file)
    data_dict = { key : str(data_configs.get(key).data) for key in data_configs }
    return data_dict

db_config = __read_config('./config/databaseconfig.properties')
data_config = __read_config('./config/route.properties')

class DbConfig:
    HOST = db_config['DataBase_HOST']
    PORT = db_config['DataBase_PORT']
    USER = db_config['DataBase_USER']
    PASSWORD = db_config['DataBase_PASSWORD']
    
    class Schema:
        SOR = db_config['DataBase_SOR']
        STG = db_config['DataBase_STG']

class DataConfig:
    csv_path = os.path.abspath(data_config['DATA_ROUTE'])
    def get_csv_path(file_name: str):
        return os.path.join(DataConfig.csv_path, file_name)