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

#define _DEFAULT_SOURCE
#ifndef _GRANT
#include "os.h"
#include "taoserror.h"
#include "tgrant.h"
#include "mnodeInt.h"

int32_t grantInit() { return TSDB_CODE_SUCCESS; }
void    grantCleanUp() {}
void    grantParseParameter() { mError("can't parsed parameter k"); }
int32_t grantCheck(EGrantType grant) { return TSDB_CODE_SUCCESS; }
void    grantReset(EGrantType grant, uint64_t value) {}
void    grantAdd(EGrantType grant, uint64_t value) {}
void    grantRestore(EGrantType grant, uint64_t value) {}

#endif