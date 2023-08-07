drop stream if exists at_once_interval_stb_stream;
drop database if exists dbt;
create database if not exists dbt;
use dbt;
create table dbt.at_once_interval_stb (ts timestamp, c1 tinyint) tags (t1 int);
create table dbt.at_once_interval_ct1 using dbt.at_once_interval_stb tags (122) ;
create stream if not exists at_once_interval_stb_stream trigger at_once ignore expired 0 ignore update 0  into at_once_interval_stb_output as select _wstart AS wstart, min(c1) from at_once_interval_stb partition by tbname interval(11s)  sliding (10s)  ;
insert into at_once_interval_ct1 values (1688021291570+0s, 53);
insert into at_once_interval_ct1 values (1688021291572+10s, 63);
insert into at_once_interval_ct1 values (1688021291574+20s, -54);
insert into at_once_interval_ct1 values (1688021291576+30s, -19);
insert into at_once_interval_ct1 values (1688021291578+40s, -70);
select wstart, `min(c1)` from at_once_interval_stb_output order by wstart;
select _wstart AS wstart, min(c1) from at_once_interval_stb partition by tbname interval(11s)  sliding (10s)  order by wstart;

