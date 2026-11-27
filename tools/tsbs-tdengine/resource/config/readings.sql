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

CREATE DATABASE db1 vgroups 8;
CREATE TABLE db1.`readings` (`ts` TIMESTAMP, `latitude` DOUBLE, `longitude` DOUBLE, `elevation` DOUBLE, `velocity` DOUBLE, `heading` DOUBLE, `grade` DOUBLE, `fuel_consumption` DOUBLE) TAGS (`name` VARCHAR(30), `fleet` VARCHAR(30), `driver` VARCHAR(30), `model` VARCHAR(30), `device_version` VARCHAR(30), `load_capacity` DOUBLE, `fuel_capacity` DOUBLE, `nominal_fuel_consumption` DOUBLE)