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

void    mgmtMonitorDbDrop(void *unused, void *unusedt);
int32_t mgmtAlterDb(SAcctObj *pAcct, SCMAlterDbMsg *pAlter);
int32_t mgmtAddVgroupIntoDb(SDbObj *pDb, SVgObj *pVgroup);
int32_t mgmtAddVgroupIntoDbTail(SDbObj *pDb, SVgObj *pVgroup);
int32_t mgmtRemoveVgroupFromDb(SDbObj *pDb, SVgObj *pVgroup);
int32_t mgmtMoveVgroupToTail(SDbObj *pDb, SVgObj *pVgroup);
int32_t mgmtMoveVgroupToHead(SDbObj *pDb, SVgObj *pVgroup);
int32_t mgmtGetDbMeta(SMeterMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t mgmtRetrieveDbs(SShowObj *pShow, char *data, int32_t rows, void *pConn);
void    mgmtCleanUpDbs();

int32_t mgmtInitDbs();
int32_t mgmtUpdateDb(SDbObj *pDb);
SDbObj *mgmtGetDb(char *db);
SDbObj *mgmtGetDbByTableId(char *db);
int32_t mgmtCreateDb(SAcctObj *pAcct, SCMCreateDbMsg *pCreate);
int32_t mgmtDropDbByName(SAcctObj *pAcct, char *name, short ignoreNotExists);
int32_t mgmtDropDb(SDbObj *pDb);
bool    mgmtCheckIsMonitorDB(char *db, char *monitordb);

#ifdef __cplusplus
}
#endif

#endif
