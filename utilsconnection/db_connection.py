from sqlalchemy import create_engine
class Db_Connection():
    def __init__(self, type, host, port, user, password, database):        

        self.connection = None
        self.type = type
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def start(self):
      
        try:
            if self.type == 'mysql':
                db_connection_str = 'mysql+pymysql://'+self.user+':'+self.password+'@'+self.host+':'+self.port+'/'+self.database
                self.connection = create_engine(db_connection_str)
                return self.connection
            else:
                return -1
        except Exception as e:
            print('Error in connection\n'+str(e))
            return -2

    def stop(self):
        """Dispose the database connection pool.
        Parameters
        ----------
        None     
        
        Returns
        -------
        None
        Raises
        ------
        None
        """  
        self.connection.dispose()