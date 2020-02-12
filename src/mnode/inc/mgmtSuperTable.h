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

#ifndef TBASE_MNODE_SUPER_TABLE_H
#define TBASE_MNODE_SUPER_TABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

#include "taosdef.h"


typedef struct {
  char    superTableId[TSDB_TABLE_ID_LEN + 1];
  int64_t uid;
  int32_t sid;
  int32_t vgId;
  int32_t sversion;
  int32_t createdTime;
  char    reserved[7];
  char    updateEnd[1];
} SSuperTableObj;

int32_t         mgmtInitSuperTables();
void            mgmtCleanUpSuperTables();
int32_t         mgmtCreateSuperTable(SDbObj *pDb, SCreateTableMsg *pCreate);
int32_t         mgmtDropSuperTable(SDbObj *pDb, char *meterId, int ignore);
int32_t         mgmtAlterSuperTable(SDbObj *pDb, SAlterTableMsg *pAlter);
SSuperTableObj* mgmtGetSuperTable(char *tableId);
SSchema*        mgmtGetSuperTableSchema(SSuperTableObj *pTable);




#ifdef __cplusplus
}
#endif

#endif
