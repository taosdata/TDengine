-- Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
--
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This program is free software: you can use, redistribute, and/or modify
-- it under the terms of the GNU Affero General Public License, version 3
-- or later ("AGPL"), as published by the Free Software Foundation.
--
-- This program is distributed in the hope that it will be useful, but WITHOUT
-- ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
-- FITNESS FOR A PARTICULAR PURPOSE.
--
-- You should have received a copy of the GNU Affero General Public License
-- along with this program. If not, see <http://www.gnu.org/licenses/>.

drop database if exists db;
create database db ;
create table db.meters(ts timestamp, current float, voltage int, phase float) tags(groupid int, location varchar(24));
create table db.d0 using db.meters tags(1, 'china');
insert into db.d0 values(1700000000001,1,1,1);
insert into db.d0 values(1700000000002,2,2,2);
insert into db.d0 values(1700000000003,3,3,4);
insert into db.d0 values(1700000000004,4,4,4);
insert into db.d0 values(1700000000005,5,5,5);
show db.tables;
set max_binary_display_width 100;
explain select * from db.meters;
show dnodes;
desc db.d0;
select * from db.meters limit 2;
drop database if exists dbus;
create database dbus precision 'us';
create table dbus.meters(ts timestamp, current float, voltage int, phase float) tags(groupid int, location varchar(24));
create table dbus.d0 using dbus.meters tags(1, 'china');
insert into dbus.d0 values(1700000000000001,1,1,1);
insert into dbus.d0 values(1700000000000002,2,2,2);
insert into dbus.d0 values(1700000000000003,3,3,3);
insert into dbus.d0 values(1700000000000004,4,4,4);
insert into dbus.d0 values(1700000000000005,5,5,5);
select * from dbus.meters;
select * from dbus.meters\G;
drop database if exists duns;
create database duns precision 'ns';
create table duns.meters(ts timestamp, current float, voltage int, phase float) tags(groupid int, location varchar(24));
create table duns.d0 using duns.meters tags(1, 'china');
insert into duns.d0 values(1700000000000000001,1,1,1);
insert into duns.d0 values(1700000000000000002,2,2,2);
insert into duns.d0 values(1700000000000000003,3,3,4);
insert into duns.d0 values(1700000000000000004,4,4,4);
insert into duns.d0 values(1700000000000000005,5,5,5);
select * from duns.meters;
select * from duns.meters\G;