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

#ifndef TDENGINE_MGMT_DB_H
#define TDENGINE_MGMT_DB_H

#ifdef __cplusplus
extern "C" {
#endif

#include "mnode.h"

int32_t mgmtAddVgroupIntoDb(SDbObj *pDb, SVgObj *pVgroup);
int32_t mgmtAddVgroupIntoDbTail(SDbObj *pDb, SVgObj *pVgroup);
int32_t mgmtRemoveVgroupFromDb(SDbObj *pDb, SVgObj *pVgroup);
int32_t mgmtMoveVgroupToTail(SDbObj *pDb, SVgObj *pVgroup);
int32_t mgmtMoveVgroupToHead(SDbObj *pDb, SVgObj *pVgroup);

int32_t mgmtInitDbs();
void    mgmtCleanUpDbs();
SDbObj *mgmtGetDb(char *db);
SDbObj *mgmtGetDbByTableId(char *db);
bool    mgmtCheckIsMonitorDB(char *db, char *monitordb);

void mgmtAddSuperTableIntoDb(SDbObj *pDb);
void mgmtRemoveSuperTableFromDb(SDbObj *pDb);
void mgmtAddTableIntoDb(SDbObj *pDb);
void mgmtRemoveTableFromDb(SDbObj *pDb);

#ifdef __cplusplus
}
#endif

#endif
