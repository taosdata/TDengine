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

#include "mgmtDef.h"

int32_t mgmtInitTables();
void    mgmtCleanUpTables();
void *  mgmtGetTable(char *tableId);
void    mgmtIncTableRef(void *pTable);
void    mgmtDecTableRef(void *pTable);
void *  mgmtGetNextChildTable(void *pIter, SChildTableObj **pTable);
void *  mgmtGetNextSuperTable(void *pIter, SSuperTableObj **pTable);
void    mgmtDropAllChildTables(SDbObj *pDropDb);
void    mgmtDropAllSuperTables(SDbObj *pDropDb);
void    mgmtDropAllChildTablesInVgroups(SVgObj *pVgroup);

#ifdef __cplusplus
}
#endif

#endif
