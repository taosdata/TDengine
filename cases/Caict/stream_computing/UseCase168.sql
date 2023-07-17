drop stream if exists at_once_interval_stb_stream;
drop database if exists dbt;
create database if not exists dbt ;
use dbt;
create table dbt.at_once_state_window_stb (ts timestamp, c1 tinyint) tags (t1 tinyint) ;
create table dbt.at_once_state_window_ct1 using dbt.at_once_state_window_stb tags (122) ;
create stream if not exists at_once_state_window_ct1_stream trigger at_once ignore expired 0 ignore update 0  into at_once_state_window_ct1_output  as select _wstart AS wstart, min(c1) from at_once_state_window_ct1 partition by tbname state_window(c1) ;
insert into at_once_state_window_ct1 (ts, c1) values (1687950078014, 6);
insert into at_once_state_window_ct1 (ts, c1) values (1687950078015, 6);
insert into at_once_state_window_ct1 (ts, c1) values (1687950078020, 38);
insert into at_once_state_window_ct1 (ts, c1) values (1687950078038, 127);
insert into at_once_state_window_ct1 (ts, c1) values (1687950078039, 127);
select wstart, `min(c1)` from at_once_state_window_ct1_output order by wstart;
select _wstart AS wstart, min(c1) from at_once_state_window_ct1 partition by tbname state_window(c1)  order by wstart,c1;

