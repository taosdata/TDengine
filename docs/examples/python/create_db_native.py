import taos

conn = taos.connect(
    host="localhost",
    user="root",
    password="taosdata",
    port=6030,
)

db = "power"

conn.execute(f"DROP DATABASE IF EXISTS {db}")
conn.execute(f"CREATE DATABASE {db}")

# change database. same as execute "USE db"
conn.select_db(db)

# create super table
conn.execute(
    "CREATE TABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
)

# create table
conn.execute("CREATE TABLE `d0` USING `meters` TAGS(0, 'Los Angles')")

conn.close()
