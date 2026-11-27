#!/bin/sh

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
. "$script_dir/find-client.sh"

: "${FQ_MYSQL_HOST:?FQ_MYSQL_HOST is required}"
: "${FQ_MYSQL_PORT:?FQ_MYSQL_PORT is required}"
: "${FQ_MYSQL_USER:?FQ_MYSQL_USER is required}"
: "${FQ_MYSQL_PASSWORD:?FQ_MYSQL_PASSWORD is required}"

mysql_bin="$(qst_find_client QST_MYSQL_CLIENT mysql '/opt/taostest/fq/mysql/*/bin/mysql')" ||
  { printf '%s\n' 'mysql client is required (set QST_MYSQL_CLIENT or install it)' >&2; exit 1; }

MYSQL_PWD="$FQ_MYSQL_PASSWORD" \
  "$mysql_bin" --protocol=TCP --host="$FQ_MYSQL_HOST" --port="$FQ_MYSQL_PORT" \
  --user="$FQ_MYSQL_USER" --default-character-set=utf8mb4 <<'SQL'
SET time_zone = '+00:00';
DROP DATABASE IF EXISTS fq_alt;
DROP DATABASE IF EXISTS fq_default;
CREATE DATABASE fq_default CHARACTER SET utf8mb4;
CREATE DATABASE fq_alt CHARACTER SET utf8mb4;
USE fq_default;
CREATE TABLE fq_series (ts TIMESTAMP(6) NOT NULL, device_id INT NOT NULL, reading DOUBLE, active BOOLEAN, region VARCHAR(16), note VARCHAR(64), jdoc JSON, event_local DATETIME(6), payload VARBINARY(8), PRIMARY KEY (ts, device_id));
CREATE TABLE fq_device (ts TIMESTAMP(6) NOT NULL, device_id INT NOT NULL, device_name VARCHAR(32) NOT NULL, tier VARCHAR(16) NOT NULL, PRIMARY KEY (ts, device_id));
CREATE TABLE fq_series_empty LIKE fq_series;
INSERT INTO fq_series VALUES ('2026-01-01 00:00:00.123000',1001,10.5,TRUE,'north','alpha','{"k":"v","n":1}','2026-01-01 08:00:00.123000',X'0102'),('2026-01-01 00:00:01.000000',1001,11.0,FALSE,'north',NULL,'{"k":"w","n":2}','2026-01-01 08:00:01.000000',X'0304'),('2026-01-01 00:00:02.000000',1002,20.25,TRUE,'south','中文','{"k":"中","n":3}','2026-01-01 08:00:02.000000',X'0506'),('2026-01-01 00:00:03.000000',1003,-1.5,TRUE,'east','%_\\\'','{"k":"x","n":4}','2026-01-01 08:00:03.000000',X'0708');
INSERT INTO fq_device VALUES ('2026-01-01 00:00:00.123000',1001,'meter-a','gold'),('2026-01-01 00:00:01.000000',1001,'meter-a','gold'),('2026-01-01 00:00:02.000000',1002,'meter-b','silver'),('2026-01-01 00:00:03.000000',1003,'meter-c','bronze');
USE fq_alt;
CREATE TABLE fq_series_alt LIKE fq_default.fq_series;
INSERT INTO fq_series_alt VALUES ('2026-01-01 00:00:00.123000',1001,10.5,TRUE,'north','alpha','{"k":"v","n":1}','2026-01-01 08:00:00.123000',X'0102');
SQL
