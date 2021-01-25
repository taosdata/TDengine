import taos
import datetime
import random
# host= '127.0.0.1'
# user = 'root'
# password = 'taosdata'
# conn = taos.connect(
#             host,
#             user,
#             password,
#             )
# cl = conn.cursor()
# cl.execute("select first(_c0),last(_c0) from test.st0 " )
# dd1=datetime.datetime.now()+ datetime.timedelta(days=5)
# sql = 'select count(*) from test.st0 where ts <= ' + "'" + str(dd1) + "'"
# print(sql)
# cl.execute(sql)
# for data in cl:
#     print(data[0])
# # print(cl[0],cl[1])
# # d1 = data[0]
# # d2 = data[1]

# # print(d1+(d2-d1)/2)
# print(random.randrange(-100,100))
# random.randrange(-100,100)
print(random.getrandbits(1))