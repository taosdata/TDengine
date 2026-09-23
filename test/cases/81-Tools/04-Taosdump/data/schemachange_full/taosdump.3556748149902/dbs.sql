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

CREATE DATABASE IF NOT EXISTS dd REPLICA 1 VGROUPS 2   DURATION 10d KEEP 3650d,3650d,3650d     PRECISION 'ms'   MINROWS 100 MAXROWS 4096 COMP 2 ;

CREATE STABLE IF NOT EXISTS `dd`.`meters2` (`ts` TIMESTAMP, `bc` BOOL, `fc` FLOAT, `dc` DOUBLE, `ti` TINYINT, `si` SMALLINT, `ic` INT, `bi` BIGINT, `uti` TINYINT UNSIGNED, `usi` SMALLINT UNSIGNED, `ui` INT UNSIGNED, `ubi` BIGINT UNSIGNED, `bin` VARCHAR(4), `nch` NCHAR(8)) TAGS (`tbc` BOOL, `tfc` FLOAT, `tdc` DOUBLE, `tti` TINYINT, `tsi` SMALLINT, `tic` INT, `tbi` BIGINT, `tuti` TINYINT UNSIGNED, `tusi` SMALLINT UNSIGNED, `tui` INT UNSIGNED, `tubi` BIGINT UNSIGNED, `tbin` VARCHAR(4), `tnch` NCHAR(8)) SECURITY_LEVEL 0 SECURE_DELETE 0
CREATE STABLE IF NOT EXISTS `dd`.`meters4` (`ts` TIMESTAMP, `bc` BOOL, `fc` FLOAT, `dc` DOUBLE, `ti` TINYINT, `si` SMALLINT, `ic` INT, `bi` BIGINT, `uti` TINYINT UNSIGNED, `usi` SMALLINT UNSIGNED, `ui` INT UNSIGNED, `ubi` BIGINT UNSIGNED, `bin` VARCHAR(4), `nch` NCHAR(8)) TAGS (`tbc` BOOL, `tfc` FLOAT, `tdc` DOUBLE, `tti` TINYINT, `tsi` SMALLINT, `tic` INT, `tbi` BIGINT, `tuti` TINYINT UNSIGNED, `tusi` SMALLINT UNSIGNED, `tui` INT UNSIGNED, `tubi` BIGINT UNSIGNED, `tbin` VARCHAR(4), `tnch` NCHAR(8)) SECURITY_LEVEL 0 SECURE_DELETE 0
CREATE STABLE IF NOT EXISTS `dd`.`meters3` (`ts` TIMESTAMP, `bc` BOOL, `fc` FLOAT, `dc` DOUBLE, `ti` TINYINT, `si` SMALLINT, `ic` INT, `bi` BIGINT, `uti` TINYINT UNSIGNED, `usi` SMALLINT UNSIGNED, `ui` INT UNSIGNED, `ubi` BIGINT UNSIGNED, `bin` VARCHAR(4), `nch` NCHAR(8)) TAGS (`tbc` BOOL, `tfc` FLOAT, `tdc` DOUBLE, `tti` TINYINT, `tsi` SMALLINT, `tic` INT, `tbi` BIGINT, `tuti` TINYINT UNSIGNED, `tusi` SMALLINT UNSIGNED, `tui` INT UNSIGNED, `tubi` BIGINT UNSIGNED, `tbin` VARCHAR(4), `tnch` NCHAR(8)) SECURITY_LEVEL 0 SECURE_DELETE 0
CREATE STABLE IF NOT EXISTS `dd`.`meters1` (`ts` TIMESTAMP, `bc` BOOL, `fc` FLOAT, `dc` DOUBLE, `ti` TINYINT, `si` SMALLINT, `ic` INT, `bi` BIGINT, `uti` TINYINT UNSIGNED, `usi` SMALLINT UNSIGNED, `ui` INT UNSIGNED, `ubi` BIGINT UNSIGNED, `bin` VARCHAR(4), `nch` NCHAR(8)) TAGS (`tbc` BOOL, `tfc` FLOAT, `tdc` DOUBLE, `tti` TINYINT, `tsi` SMALLINT, `tic` INT, `tbi` BIGINT, `tuti` TINYINT UNSIGNED, `tusi` SMALLINT UNSIGNED, `tui` INT UNSIGNED, `tubi` BIGINT UNSIGNED, `tbin` VARCHAR(4), `tnch` NCHAR(8)) SECURITY_LEVEL 0 SECURE_DELETE 0
