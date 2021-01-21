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

#ifndef TDENGINE_MNODE_DB_H
#define TDENGINE_MNODE_DB_H

#ifdef __cplusplus
extern "C" {
#endif

#include "mnodeDef.h"

enum _TSDB_DB_STATUS {
  TSDB_DB_STATUS_READY,
  TSDB_DB_STATUS_DROPPING
};

// api
int32_t mnodeInitDbs();
void    mnodeCleanupDbs();
int64_t mnodeGetDbNum();
SDbObj *mnodeGetDb(char *db);
SDbObj *mnodeGetDbByTableName(char *db);
void *  mnodeGetNextDb(void *pIter, SDbObj **pDb);
void    mnodeCancelGetNextDb(void *pIter);
void    mnodeIncDbRef(SDbObj *pDb);
void    mnodeDecDbRef(SDbObj *pDb);
bool    mnodeCheckIsMonitorDB(char *db, char *monitordb);
void    mnodeDropAllDbs(SAcctObj *pAcct);

// util func
void mnodeAddSuperTableIntoDb(SDbObj *pDb);
void mnodeRemoveSuperTableFromDb(SDbObj *pDb);
void mnodeAddTableIntoDb(SDbObj *pDb);
void mnodeRemoveTableFromDb(SDbObj *pDb);
void mnodeAddVgroupIntoDb(SVgObj *pVgroup);
void mnodeRemoveVgroupFromDb(SVgObj *pVgroup);

#ifdef __cplusplus
}
#endif

#endif
