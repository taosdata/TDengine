create database db vgroups 1;
create table db.meters (ts timestamp, f1 int) tags(t1 int);
create topic topic_meters as select ts, tbname, f1, t1 from db.meters;
insert into db.tb using db.meters tags(1) values(now, 1);
create bnode on dnode 1;
