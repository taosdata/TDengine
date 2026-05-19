drop database if exists test;
create database test;
CREATE STABLE test.`meters` (`ts` TIMESTAMP, `c0` INT, `c1` VARCHAR(64), `c2` NCHAR(64)) TAGS (`t0` INT, `t1` VARCHAR(24))
CREATE TABLE test.`d0` USING test.`meters` (`t0`, `t1`) TAGS (0, "d0tags")
CREATE TABLE test.`d1` USING test.`meters` (`t0`, `t1`) TAGS (1, "d1tags")
CREATE TABLE test.`d2` USING test.`meters` (`t0`, `t1`) TAGS (2, "d2tags")
insert into test.d0 values('2025-01-01 10:00:01', 1,'binary1','nchar1')
insert into test.d0 values('2025-01-01 10:00:02', 2,'binary2','nchar2');
insert into test.d0 values('2025-01-01 10:00:03', 3,'binary3','nchar3');
insert into test.d1 values('2025-01-01 10:00:01', 1,'binary4','nchar4')
insert into test.d2 values('2025-01-01 10:00:01', 1,'binary5','nchar5')
insert into test.d0(ts) values('2025-01-01 10:00:04');
insert into test.d0(ts) values('2025-01-01 10:00:05');
create table test.st(ts timestamp, age int) tags(area int);
insert into test.t1 using test.st tags(1) values('2025-01-02 10:00:01', 21);
insert into test.t1 using test.st tags(1) values('2025-01-02 10:00:02', 22);
insert into test.t2 using test.st tags(1) values('2025-01-02 10:00:03', 23);
insert into test.t2 using test.st tags(1) values('2025-01-02 10:00:04', 24);