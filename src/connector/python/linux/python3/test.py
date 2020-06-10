from taos.cinterface import CTaosInterface
from taos.error import *
from taos.subscription import TDengineSubscription
from taos.connection import TDengineConnection


if __name__ == '__main__':
    conn = TDengineConnection(
        host="127.0.0.1", user="root", password="taosdata", database="test")

    # Generate a cursor object to run SQL commands
    sub = conn.subscribe(False, "test", "select * from log0601;", 1000)

    for i in range(100):
        print(i)
        data = sub.consume()
        for d in data:
            print(d)

    sub.close()
    conn.close()
