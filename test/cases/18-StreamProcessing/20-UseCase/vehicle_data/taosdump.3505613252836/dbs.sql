#!server_ver: ver:3.3.6.14.0717
#!taosdump_ver: 3.3.6.13.alpha_66c1a46d50347d42959aa3670f4c3d557f29c625
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

CREATE DATABASE IF NOT EXISTS idmp_sample_vehicle REPLICA 1   DURATION 10d KEEP 3650d,3650d,3650d     PRECISION 'ms'   MINROWS 100 MAXROWS 4096 COMP 2 ;

CREATE TABLE IF NOT EXISTS `idmp_sample_vehicle`.`vehicles` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `longitude` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `latitude` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `elevation` SMALLINT ENCODE 'simple8b' COMPRESS 'zlib' LEVEL 'medium', `speed` SMALLINT ENCODE 'simple8b' COMPRESS 'zlib' LEVEL 'medium', `direction` SMALLINT ENCODE 'simple8b' COMPRESS 'zlib' LEVEL 'medium', `alarm` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `mileage` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`vehicle_asset` NCHAR(128), `vehicle_id` VARCHAR(32), `vehicle_no` NCHAR(17), `vehicle_plate_color` TINYINT, `producer` VARCHAR(11), `terminal_id` VARCHAR(15))
