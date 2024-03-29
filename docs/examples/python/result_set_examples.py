import taos

conn = taos.connect()
conn.execute("DROP DATABASE IF EXISTS test")
conn.execute("CREATE DATABASE test keep 36500")
conn.select_db("test")
conn.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)")
# prepare data
for i in range(2000):
    location = str(i % 10)
    tb = "t" + location
    conn.execute(f"INSERT INTO {tb} USING weather TAGS({location}) VALUES (now+{i}a, 23.5) (now+{i + 1}a, 23.5)")

result: taos.TaosResult = conn.query("SELECT * FROM weather")

block_index = 0
blocks: taos.TaosBlocks = result.blocks_iter()
for rows, length in blocks:
    print("block ", block_index, " length", length)
    print("first row in this block:", rows[0])
    block_index += 1

conn.close()

# possible output:
# block  0  length 1200
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 46000), 23.5, 0)
# block  1  length 1200
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 76000), 23.5, 3)
# block  2  length 1200
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 99000), 23.5, 6)
# block  3  length 400
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 122000), 23.5, 9)
