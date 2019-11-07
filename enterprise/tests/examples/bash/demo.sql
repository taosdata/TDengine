create database if not exists db0
create table if not exists db0.tb0 (ts timestamp, field int)
import into db0.tb0 file demo.csv

create database if not exists db1
use db1
create table if not exists tb1 (ts timestamp, name binary(8))
insert into tb1 values('2010-07-23 11:01:02.000', 'xxxx')
insert into tb1 values(now, 'aaaa');    
insert into tb1 values(now+1a, 'bbbb'); 
insert into tb1 values(now+1s, 'cccc'); 
insert into tb1 values(now+1m, 'dddd'); 
insert into tb1 values(now+1h, 'eeee'); 
insert into tb1 values(now+1d, 'ffff'); 
insert into tb1 values(now+1w, 'gggg');
insert into tb1 values(now+1n, 'hhhh');
insert into tb1 values(now+1y, 'iiii');
