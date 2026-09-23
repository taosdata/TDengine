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

CREATE STABLE `vst_气象传感器_237150` (`ts` TIMESTAMP , `环境温度` DOUBLE, `组件温度` DOUBLE, `辐照` DOUBLE ) TAGS (`_ignore_path` VARCHAR(20), `光伏资产模型` VARCHAR(128), `电站ID` VARCHAR(10), `气象传感器ID` VARCHAR(20), `path1` VARCHAR(512)) SMA(`ts`,`环境温度`) VIRTUAL 1;
CREATE STABLE `vst_光伏逆变器_833471` (`ts` TIMESTAMP , `直流电量` DOUBLE, `交流电量` DOUBLE, `日发电量` DOUBLE, `累计发电量` DOUBLE) TAGS (`_ignore_path` VARCHAR(20), `光伏资产模型` VARCHAR(128), `电站ID` VARCHAR(10), `逆变器ID` VARCHAR(20), `path1` VARCHAR(512)) SMA(`ts`,`直流电量`) VIRTUAL 1;