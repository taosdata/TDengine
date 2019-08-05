create database if not exists db0
create table if not exists db0.tb0 (ts timestamp, voltage int, current float)
import into db0.tb0 file demo.csv


create database if not exists db1
use db1
create table if not exists tb1 (ts timestamp, temperature int, humidity float)
insert into tb1 values('2010-07-23 11:01:02.000', 37, 50.1)
insert into tb1 values(now, 36, 47.8);    
insert into tb1 values(now+1a, 38, 65.3); 
insert into tb1 values(now+1s, 38, 53.9 ); 
insert into tb1 values(now+1m, 37, 45.6); 
insert into tb1 values(now+1h, 35, 41.1); 
