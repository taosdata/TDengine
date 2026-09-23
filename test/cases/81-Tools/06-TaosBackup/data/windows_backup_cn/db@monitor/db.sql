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

CREATE DATABASE `db@monitor` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' CACHESHARDBITS -1 COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3649d,3649d,3649d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 2 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h IS_AUDIT 0 SECURE_DELETE 0 ALLOW_DROP 1 SECURITY_LEVEL 0
