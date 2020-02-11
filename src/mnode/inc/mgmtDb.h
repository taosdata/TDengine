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

void mgmtMonitorDbDrop(void *unused, void *unusedt);
int mgmtAlterDb(SAcctObj *pAcct, SAlterDbMsg *pAlter);
int mgmtUseDb(SConnObj *pConn, char *name);
int mgmtAddVgroupIntoDb(SDbObj *pDb, SVgObj *pVgroup);
int mgmtAddVgroupIntoDbTail(SDbObj *pDb, SVgObj *pVgroup);
int mgmtRemoveVgroupFromDb(SDbObj *pDb, SVgObj *pVgroup);
int mgmtAddMetricIntoDb(SDbObj *pDb, STabObj *pMetric);
int mgmtRemoveMetricFromDb(SDbObj *pDb, STabObj *pMetric);
int mgmtMoveVgroupToTail(SDbObj *pDb, SVgObj *pVgroup);
int mgmtMoveVgroupToHead(SDbObj *pDb, SVgObj *pVgroup);
int mgmtGetDbMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn);
int mgmtRetrieveDbs(SShowObj *pShow, char *data, int rows, SConnObj *pConn);
void mgmtCleanUpDbs();


int32_t mgmtInitDbs();
int     mgmtUpdateDb(SDbObj *pDb);
SDbObj *mgmtGetDb(char *db);
SDbObj *mgmtGetDbByMeterId(char *db);
int     mgmtCreateDb(SAcctObj *pAcct, SCreateDbMsg *pCreate);
int     mgmtDropDbByName(SAcctObj *pAcct, char *name, short ignoreNotExists);
int     mgmtDropDb(SDbObj *pDb);

#ifdef __cplusplus
}
#endif

#endif
