import taos

conn: taos.TaosConnection = taos.connect(host="localhost",
                                         user="root",
                                         password="taosdata",
                                         database="test",
                                         port=6030,
                                         config="/etc/taos",  # for windows the default value is C:\TDengine\cfg
                                         timezone="Asia/Shanghai")  # default your host's timezone

server_version = conn.server_info
print("server_version", server_version)
client_version = conn.client_info
print("client_version", client_version)  # 3.0.0.0

conn.close()

# possible output:
# 3.0.0.0
# 3.0.0.0
