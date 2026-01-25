drop database test;
create database test;
CREATE STABLE test.`meters` (`ts` TIMESTAMP, `c0` INT, `c1` VARCHAR(64), `c2` NCHAR(64)) TAGS (`t0` INT, `t1` VARCHAR(24))
CREATE TABLE test.`d0` USING test.`meters` (`t0`, `t1`) TAGS (30204, "kmgDyyFvNptaYuvXgfbSJ0")
insert into test.d0 values('2025-01-01 10:00:01', 1,'binary1','nchar1')
insert into test.d0 values('2025-01-01 10:00:02', 2,'binary2','nchar2');
insert into test.d0 values('2025-01-01 10:00:03', 3,'binary3','nchar3');
insert into test.d0(ts) values('2025-01-01 10:00:04');
insert into test.d0(ts) values('2025-01-01 10:00:05');
