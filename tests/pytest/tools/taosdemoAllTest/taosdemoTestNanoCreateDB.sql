drop database  if exists nsdbsql;
create database nsdbsql precision "ns" keep 36 days 6 update 1;
use nsdbsql;
CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupdId int);
CREATE TABLE d1001 USING meters TAGS ("Beijing.Chaoyang", 2);
INSERT INTO d1001 USING METERS TAGS ("Beijng.Chaoyang", 2) VALUES (now, 10.2, 219, 0.32);
INSERT INTO d1001 USING METERS TAGS ("Beijng.Chaoyang", 2) VALUES (now, 85, 32, 0.76);
