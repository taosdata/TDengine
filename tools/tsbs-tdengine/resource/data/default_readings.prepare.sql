DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
CREATE STABLE test.meters (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` VARCHAR(24));
CREATE TABLE test.d0 using test.meters TAGS (1, 'room1');
CREATE STREAM test.stream1 interval(1s) sliding (1s) from test.d0 into test.result_1 as select _twstart as ws , _twend as we, count(*) as cnt, sum(voltage) as sum_voltage from %%trows;