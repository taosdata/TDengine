#!server_ver: ver:3.4.1.0.alpha.enterprise
#!taosdump_ver: 3.4.1.6.alpha_28d9f6ca0d0caedb8ab176cac05089dbfa50ada9
#!os_id: LINUX
#!escape_char: true
#!loose_mode: false
#!charset: UTF-8

-- Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

CREATE DATABASE IF NOT EXISTS db REPLICA 1 VGROUPS 2   DURATION 10d KEEP 3649d,3649d,3649d     PRECISION 'ms'   MINROWS 100 MAXROWS 4096 COMP 2 ;

CREATE STABLE IF NOT EXISTS `db`.`st` (`ts` TIMESTAMP, `c1` INT, `c2` BOOL, `c3` TINYINT, `c4` SMALLINT, `c5` BIGINT, `c6` FLOAT, `c7` DOUBLE, `c8` TIMESTAMP, `c9` VARCHAR(10), `c10` NCHAR(10), `c11` TINYINT UNSIGNED, `c12` SMALLINT UNSIGNED, `c13` INT UNSIGNED, `c14` BIGINT UNSIGNED, `c15` DECIMAL(30,16), `c16` BLOB) TAGS (`n1` INT, `w2` BOOL, `t3` TINYINT, `t4` SMALLINT, `t5` BIGINT, `t6` FLOAT, `t7` DOUBLE, `t8` TIMESTAMP, `t9` VARCHAR(10), `t10` NCHAR(10), `t11` TINYINT UNSIGNED, `t12` SMALLINT UNSIGNED, `t13` INT UNSIGNED, `t14` BIGINT UNSIGNED) SECURITY_LEVEL 0 SECURE_DELETE 0
CREATE STABLE IF NOT EXISTS `db`.`vst` (`ts` TIMESTAMP, `c1` INT, `c2` BOOL, `c3` TINYINT, `c4` SMALLINT, `c5` BIGINT, `c6` FLOAT, `c7` DOUBLE, `c8` TIMESTAMP, `c9` VARCHAR(10), `c10` NCHAR(10), `c11` TINYINT UNSIGNED, `c12` SMALLINT UNSIGNED, `c13` INT UNSIGNED, `c14` BIGINT UNSIGNED) TAGS (`n1` INT, `w2` BOOL, `t3` TINYINT, `t4` SMALLINT, `t5` BIGINT, `t6` FLOAT, `t7` DOUBLE, `t8` TIMESTAMP, `t9` VARCHAR(10), `t10` NCHAR(10), `t11` TINYINT UNSIGNED, `t12` SMALLINT UNSIGNED, `t13` INT UNSIGNED, `t14` BIGINT UNSIGNED) SECURITY_LEVEL 0 VIRTUAL 1 SECURE_DELETE 0
