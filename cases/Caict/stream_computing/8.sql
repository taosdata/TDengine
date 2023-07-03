show streams;
drop stream if exists at_once_session_tb1_stream;
show databases;
drop database if exists `dbt`;
drop database if exists `dbt`;
drop database if exists dbt;
create database if not exists dbt vgroups 2 precision "ms"  cachemodel "both" ;
use dbt;
create table dbt.at_once_interval_ext_stb (ts timestamp, c1 tinyint) tags (t1 tinyint, t2 smallint, t3 int) ;
create table dbt.at_once_interval_ext_ct1 using dbt.at_once_interval_ext_stb tags (66, -27619, -1139947908) ;
create table dbt.ext_at_once_interval_ext_stb_output (ts timestamp, c1 tinyint) tags (t1 tinyint, t2 smallint, t3 int) ;
create stream if not exists at_once_interval_ext_stb_stream trigger at_once ignore expired 0 ignore update 0  into ext_at_once_interval_ext_stb_output tags(t1, t2, t3)  as select _wstart AS wstart, min(c1) from at_once_interval_ext_stb partition by t1, t2, t3 interval(11s) ;
insert into at_once_interval_ext_ct1 values (1688109463108+0s, 110);
select ts, c1, t1, t2, t3 from ext_at_once_interval_ext_stb_output order by ts;
select _wstart AS wstart, min(c1), min(t1),max(t2),sum(t3) from at_once_interval_ext_stb partition by t1, t2, t3 interval(11s) order by wstart;

