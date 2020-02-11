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

#ifndef TBASE_MNODE_UTIL_H
#define TBASE_MNODE_UTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "mnode.h"

bool    mgmtTableCreateFromSuperTable(STabObj *pTableObj);
bool    mgmtIsSuperTable(STabObj *pTableObj);
bool    mgmtIsNormalTable(STabObj *pTableObj);
char*   mgmtTableGetTag(STabObj* pTable, int32_t col, SSchema* pTagColSchema);
int32_t mgmtGetTagsLength(STabObj* pSuperTable, int32_t col);
bool    mgmtCheckIsMonitorDB(char *db, char *monitordb);
int32_t mgmtCheckDBParams(SCreateDbMsg *pCreate);

#ifdef __cplusplus
}
#endif

#endif
