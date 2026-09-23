#!server_ver: ver:3.1.0.0
#!taosdump_ver: 2.5.2_cf16c4d
#!os_id: LINUX
#!escape_char: true
#!loose_mode: false
#!charset: UTF-8

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

CREATE DATABASE IF NOT EXISTS test REPLICA 1   DURATION 14400m KEEP 5256000m,5256000m,5256000m     PRECISION 'ms'   MINROWS 100 MAXROWS 4096 COMP 2 ;

CREATE TABLE IF NOT EXISTS test.`meters`(`ts` timestamp,`bc` bool,`fc` float,`dc` double,`ti` tinyint,`si` smallint,`ic` int,`bi` bigint,`uti` tinyint unsigned,`usi` smallint unsigned,`ui` int unsigned,`ubi` bigint unsigned,`bin` binary(4),`nch` nchar(8)) TAGS(`tbc` bool,`tfc` float,`tdc` double,`tti` tinyint,`tsi` smallint,`tic` int,`tbi` bigint,`tuti` tinyint unsigned,`tusi` smallint unsigned,`tui` int unsigned,`tubi` bigint unsigned,`tbin` binary(4),`tnch` nchar(8));

