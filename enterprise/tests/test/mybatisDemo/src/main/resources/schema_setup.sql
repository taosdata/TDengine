drop database if exists mybatis_demo;
create database mybatis_demo;
use mybatis_demo;
create table devices (ts timestamp, c1 int, c2 nchar(5), c3 smallint, c4 bigint, c5 binary(20), c6 bool, c7 tinyint, c8 float, c9 double) tags(deviceId int, t2 nchar(10));
create table device1 using devices tags (1, "beijing");
create table device2 using devices tags (2, "beijing");
create table device3 using devices tags (3, "shanghai");
create table device4 using devices tags (4, "hongkong");
insert into device1 values ('2018-09-17 09:00:00.000', 1, '涛思', 1, 1, 'im a binary', true, 1, 1, 1);
