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

#ifndef TBASE_MNODE_TABLE_H
#define TBASE_MNODE_TABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "mnodeDef.h"

int32_t mnodeInitTables();
void    mnodeCleanupTables();
int64_t mnodeGetSuperTableNum();
int64_t mnodeGetChildTableNum();
void *  mnodeGetTable(char *tableId);
void    mnodeIncTableRef(void *pTable);
void    mnodeDecTableRef(void *pTable);
void *  mnodeGetNextChildTable(void *pIter, SCTableObj **pTable);
void *  mnodeGetNextSuperTable(void *pIter, SSTableObj **pTable);
void    mnodeCancelGetNextChildTable(void *pIter);
void    mnodeCancelGetNextSuperTable(void *pIter);
void    mnodeDropAllChildTables(SDbObj *pDropDb);
void    mnodeDropAllSuperTables(SDbObj *pDropDb);
void    mnodeDropAllChildTablesInVgroups(SVgObj *pVgroup);

#ifdef __cplusplus
}
#endif

#endif
