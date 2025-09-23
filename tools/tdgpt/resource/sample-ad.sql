create database if not exists test keep 36500;
use test;

drop table if exists ad_sample;
create table if not exists ad_sample(ts timestamp, val int);

insert into ad_sample values(1577808000000, 5);
insert into ad_sample values(1577808001000, 14);
insert into ad_sample values(1577808002000, 15);
insert into ad_sample values(1577808003000, 15);
insert into ad_sample values(1577808004000, 14);
insert into ad_sample values(1577808005000, 19);
insert into ad_sample values(1577808006000, 17);
insert into ad_sample values(1577808007000, 16);
insert into ad_sample values(1577808008000, 20);
insert into ad_sample values(1577808009000, 22);
insert into ad_sample values(1577808010000, 8);
insert into ad_sample values(1577808011000, 21);
insert into ad_sample values(1577808012000, 28);
insert into ad_sample values(1577808013000, 11);
insert into ad_sample values(1577808014000, 9);
insert into ad_sample values(1577808015000, 29);
insert into ad_sample values(1577808016000, 40);