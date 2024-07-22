import taosws

dsn = "taosws://root:taosdata@localhost:6041"
conn = taosws.connect(dsn)

db = "power"

conn.execute(f"DROP DATABASE IF EXISTS {db}")
conn.execute(f"CREATE DATABASE {db}")

# change database. 
conn.execute(f"USE {db}")

# create super table
conn.execute(
    "CREATE TABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
)

# create table
conn.execute("CREATE TABLE `d0` USING `meters` TAGS(0, 'Los Angles')")

conn.close()
