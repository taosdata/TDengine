# ignore expired = 1 and ignore update = 1
drop stream if exists at_once_session_tb1_stream;
drop database if exists dbt;
create database if not exists dbt;
use dbt;
create table dbt.at_once_session_tb1 (ts timestamp, c1 tinyint) ;
create stream if not exists at_once_session_tb1_stream trigger at_once ignore expired 1 ignore update 1  into at_once_session_tb1_output as select _wstart AS wstart, min(c1) from at_once_session_tb1 partition by tbname session(ts, 10s) ;
insert into at_once_session_tb1 values (1688093224866, 11);
insert into at_once_session_tb1 values (1688093226866, 12);
insert into at_once_session_tb1 values (1688093231866, 13);
insert into at_once_session_tb1 values (1688093242866, 15);
insert into at_once_session_tb1 values (1688093253866, 16);
select wstart, `min(c1)` from at_once_session_tb1_output;
select _wstart AS wstart, min(c1) from at_once_session_tb1 partition by tbname session(ts, 10s);


insert into at_once_session_tb1 values (1688093104866, 64);
insert into at_once_session_tb1 values (1688093253866, 17);
select wstart, `min(c1)` from at_once_session_tb1_output;
select _wstart AS wstart, min(c1) from at_once_session_tb1 partition by tbname session(ts, 10s);


# ignore expired = 0 and ignore update = 0
create table dbt.at_once_session_tb2 (ts timestamp, c1 tinyint) ;
create stream if not exists at_once_session_tb2_stream trigger at_once ignore expired 0 ignore update 0  into at_once_session_tb2_output as select _wstart AS wstart, min(c1) from at_once_session_tb2 partition by tbname session(ts, 10s) ;
insert into at_once_session_tb2 values (1688093224866, 11);
insert into at_once_session_tb2 values (1688093226866, 12);
insert into at_once_session_tb2 values (1688093231866, 13);
insert into at_once_session_tb2 values (1688093242866, 15);
insert into at_once_session_tb2 values (1688093253866, 16);
select wstart, `min(c1)` from at_once_session_tb2_output;
select _wstart AS wstart, min(c1) from at_once_session_tb2 partition by tbname session(ts, 10s);


insert into at_once_session_tb2 values (1688093104866, 64);
insert into at_once_session_tb2 values (1688093253866, 17);
select wstart, `min(c1)` from at_once_session_tb2_output;
select _wstart AS wstart, min(c1) from at_once_session_tb2 partition by tbname session(ts, 10s);



