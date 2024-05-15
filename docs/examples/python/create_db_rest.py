import taosrest

conn = taosrest.connect(url="http://localhost:6041")

db = "power"

conn.execute(f"DROP DATABASE IF EXISTS {db}")
conn.execute(f"CREATE DATABASE {db}")

# create super table
conn.execute(
    f"CREATE TABLE `{db}`.`meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(16))"
)

# create table
conn.execute(f"CREATE TABLE `{db}`.`d0` USING `{db}`.`meters` TAGS(0, 'Los Angles')")

conn.close()
