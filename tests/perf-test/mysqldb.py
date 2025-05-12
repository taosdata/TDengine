import mysql.connector

class MySQLDatabase:
    def __init__(self, host = '192.168.1.116', port = 3306, user = 'root', password = 'taosdata', database = 'perf_data'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
        except mysql.connector.Error as error:
            print("Failed to connect to database: {}".format(error))

    def execute(self, query, params=None):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
        except mysql.connector.Error as error:
            print("Failed to execute query: {}".format(error))
        finally:
            cursor.close()    

    def query(self, query, params=None):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            result = cursor.fetchall()
            return result
        except mysql.connector.Error as error:
            print("Failed to execute query: {}".format(error))
        finally:
            cursor.close()
    
    def get_id(self, query, params = None):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            cursor.execute("select last_insert_id()")
            id = cursor.fetchone()[0]
            self.connection.commit()
            
            return id
        except mysql.connector.Error as error:
            print("Failed to execute query: {}".format(error))
        finally:
            cursor.close()

    def disconnect(self):
        self.connection.close()