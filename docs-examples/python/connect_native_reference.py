import taos

conn: taos.TaosConnection = taos.connect(host="localhost",
                                         user="root",
                                         password="taosdata",
                                         database="test",
                                         port=6030,
                                         config="C:\TDengine\cfg",  # for linux the default value is /etc/taos
                                         timezone="Asia/Shanghai"  # default your host's timezone
                                         )
conn.close()
