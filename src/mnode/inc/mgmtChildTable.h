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

#ifndef TBASE_MNODE_CHILD_TABLE_H
#define TBASE_MNODE_CHILD_TABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "taosdef.h"

struct SSuperTableObj;


typedef struct {
  char    tableId[TSDB_TABLE_ID_LEN + 1];
  char    superTableId[TSDB_TABLE_ID_LEN + 1];
  int64_t uid;
  int32_t sid;
  int32_t vgId;
  int64_t createdTime;
  int32_t sversion;
  char    reserved[3];
  char    updateEnd[1];
  struct SSuperTableObj *superTable;
} SChildTableObj;

int32_t         mgmtInitChildTables();
void            mgmtCleanUpChildTables();
int32_t         mgmtCreateChildTable(SDbObj *pDb, SCreateTableMsg *pCreate);
int32_t         mgmtDropChildTable(SDbObj *pDb, char *meterId, int ignore);
int32_t         mgmtAlterChildTable(SDbObj *pDb, SAlterTableMsg *pAlter);
SChildTableObj* mgmtGetChildTable(char *tableId);
SSchema*        mgmtGetChildTableSchema(SChildTableObj *pTable);



#ifdef __cplusplus
}
#endif

#endif
