import taos

conn = taos.connect(host='127.0.0.1',
                    user='root',
                    passworkd='taodata',
                    database='log')
cursor = conn.cursor()

sql = "select * from log.log limit 10"
cursor.execute(sql)
for row in cursor:
    print(row)
