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

#include "mnode.h"

int32_t mgmtInitChildTables();
void    mgmtCleanUpChildTables();
void *  mgmtGetChildTable(char *tableId);

void *mgmtCreateChildTable(SCMCreateTableMsg *pCreate, SVgObj *pVgroup, int32_t sid);
void *mgmtBuildCreateChildTableMsg(SCMCreateTableMsg *pCreate, SChildTableObj *pTable);

int32_t mgmtDropChildTable(SQueuedMsg *newMsg, SChildTableObj *pTable);
int32_t mgmtModifyChildTableTagValueByName(SChildTableObj *pTable, char *tagName, char *nContent);

int32_t mgmtGetChildTableMeta(SDbObj *pDb, SChildTableObj *pTable, STableMeta *pMeta, bool usePublicIp);

#ifdef __cplusplus
}
#endif

#endif
