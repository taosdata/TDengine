"""
This is the sample code for TDengine python2 client.
"""
import taos
import sys
import datetime
import random

def exitProgram(conn):
    conn.close()
    sys.exit()

if __name__ == '__main__':
    start_time = datetime.datetime(2019, 7, 1)
    time_interval = datetime.timedelta(seconds=60)

    # Connect to TDengine server.
    # 
    # parameters:
    # @host     : TDengine server IP address 
    # @user     : Username used to connect to TDengine server
    # @password : Password 
    # @database : Database to use when connecting to TDengine server
    # @config   : Configuration directory
    if len(sys.argv)>1:
        hostname=sys.argv[1]
        conn = taos.connect(host=hostname, user="root", password="taosdata", config="/etc/taos")
    else:
        conn = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos")
   
    # Generate a cursor object to run SQL commands
    c1 = conn.cursor()
    # Create a database named db
    try:
        c1.execute('create database if not exists db ')
    except Exception as err:
        conn.close()
        raise(err)
        
    # use database
    try:
        c1.execute('use db')
    except Exception as err:
        conn.close()
        raise(err)


    # create table
    try:
        c1.execute('create table if not exists t (ts timestamp, a int, b float, c binary(20))')
    except Exception as err:
        conn.close()
        raise(err)

    # insert data 
    for i in range(10):
        try:
           value = c1.execute("insert into t values ('%s', %d, %f, '%s')" % (start_time, random.randint(1,10), random.randint(1,10)/10.0, 'hello'))
           #if insert, value is the affected rows
           print(value)
        except Exception as err:
            conn.close()
            raise(err)
        start_time += time_interval

    # query data and return data in the form of list
    try:
        c1.execute('select * from db.t')
    except Exception as err:
        conn.close()
        raise(err)

    # Column names are in c1.description list
    cols = c1.description
    # Use fetchall to fetch data in a list
    data = c1.fetchall()

    for col in data:
        print(col)

    print('Another query method ')

    try:
        c1.execute('select * from db.t')
    except Exception as err:
        conn.close()
        raise(err)

    # Use iterator to go through the retreived data
    for col in c1:
        print(col)

    conn.close()
