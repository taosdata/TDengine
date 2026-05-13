drop stream if exists at_once_state_window_ct1_stream;
drop database if exists dbt;
create database if not exists dbt ;
use dbt;
create table dbt.at_once_session_stb (ts timestamp, c1 tinyint) tags (t1 tinyint) ;
create table dbt.at_once_session_ct1 using dbt.at_once_session_stb tags (64) ;
create stream if not exists at_once_session_ct1_stream trigger at_once ignore expired 0 ignore update 0  into at_once_session_ct1_output as select _wstart AS wstart, min(c1) from at_once_session_ct1 partition by tbname session(ts, 10s) ;
insert into at_once_session_ct1 values (1688026775179, 66);
insert into at_once_session_ct1 values (1688026785180, -27);
insert into at_once_session_ct1 values (1688026785181, Null);
insert into at_once_session_ct1 values (1688026795182, -110);
insert into at_once_session_ct1 values (1688026795183, 18);
select wstart, `min(c1)` from at_once_session_ct1_output;
select _wstart AS wstart, min(c1) from at_once_session_ct1 partition by tbname session(ts, 10s);

