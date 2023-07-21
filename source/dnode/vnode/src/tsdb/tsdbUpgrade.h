/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "tsdb.h"
#include "tsdbDataFileRW.h"
#include "tsdbDef.h"
#include "tsdbFS2.h"
#include "tsdbUtil2.h"

#ifndef _TSDB_UPGRADE_H_
#define _TSDB_UPGRADE_H_

#ifdef __cplusplus
extern "C" {
#endif

int32_t tsdbCheckAndUpgradeFileSystem(STsdb *tsdb, int8_t rollback);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_UPGRADE_H_*/