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
#if !defined(_TSDB_META_H_)
#define _TSDB_META_H_

#include <pthread.h>

#include "tsdb.h"
#include "dataformat.h"
#include "tskiplist.h"
#include "tsdbMetaFile.h"

#ifdef __cplusplus
extern "C" {
#endif

// #include "taosdef.h"

// Initially, there are 4 tables
#define TSDB_INIT_NUMBER_OF_SUPER_TABLE 4

#define IS_CREATE_STABLE(pCfg) ((pCfg)->tagValues != NULL)

typedef struct {
  TSKEY   keyFirst;
  TSKEY   keyLast;
  int32_t numOfPoints;
  void *  pData;
} SMemTable;

// ---------- TSDB TABLE DEFINITION
typedef struct STable {
  int8_t         type;
  STableId       tableId;
  int32_t        superUid;  // Super table UID
  int32_t        sversion;
  STSchema *     schema;
  STSchema *     tagSchema;
  SDataRow       tagVal;
  SMemTable *    mem;
  SMemTable *    imem;
  void *         pIndex;         // For TSDB_SUPER_TABLE, it is the skiplist index
  void *         eventHandler;   // TODO
  void *         streamHandler;  // TODO
  struct STable *next;           // TODO: remove the next
} STable;

void *  tsdbEncodeTable(STable *pTable, int *contLen);
STable *tsdbDecodeTable(void *cont, int contLen);
void *  tsdbFreeEncode(void *cont);

// ---------- TSDB META HANDLE DEFINITION
typedef struct {
  int32_t maxTables;  // Max number of tables

  int32_t nTables;  // Tables created

  STable **tables;  // table array

  STable *superList;  // super table list TODO: change  it to list container

  void *map; // table map of (uid ===> table)

  SMetaFile *mfh; // meta file handle
  int        maxRowBytes;
  int        maxCols;
} STsdbMeta;

STsdbMeta *tsdbInitMeta(const char *rootDir, int32_t maxTables);
int32_t    tsdbFreeMeta(STsdbMeta *pMeta);

// ---- Operation on STable
#define TSDB_TABLE_ID(pTable) ((pTable)->tableId)
#define TSDB_TABLE_UID(pTable) ((pTable)->uid)
#define TSDB_TABLE_NAME(pTable) ((pTable)->tableName)
#define TSDB_TABLE_TYPE(pTable) ((pTable)->type)
#define TSDB_TABLE_SUPER_TABLE_UID(pTable) ((pTable)->stableUid)
#define TSDB_TABLE_IS_SUPER_TABLE(pTable) (TSDB_TABLE_TYPE(pTable) == TSDB_SUPER_TABLE)
#define TSDB_TABLE_TAG_VALUE(pTable) ((pTable)->pTagVal)
#define TSDB_TABLE_CACHE_DATA(pTable) ((pTable)->content.pData)
#define TSDB_SUPER_TABLE_INDEX(pTable) ((pTable)->content.pIndex)

// ---- Operation on SMetaHandle
#define TSDB_NUM_OF_TABLES(pHandle) ((pHandle)->numOfTables)
#define TSDB_NUM_OF_SUPER_TABLES(pHandle) ((pHandle)->numOfSuperTables)
#define TSDB_TABLE_OF_ID(pHandle, id) ((pHandle)->pTables)[id]
#define TSDB_GET_TABLE_OF_NAME(pHandle, name) /* TODO */

int32_t tsdbCreateTableImpl(STsdbMeta *pMeta, STableCfg *pCfg);
int32_t tsdbDropTableImpl(STsdbMeta *pMeta, STableId tableId);
STable *tsdbIsValidTableToInsert(STsdbMeta *pMeta, STableId tableId);
// int32_t tsdbInsertRowToTableImpl(SSkipListNode *pNode, STable *pTable);
STable *tsdbGetTableByUid(STsdbMeta *pMeta, int64_t uid);
char *getTupleKey(const void * data);

#ifdef __cplusplus
}
#endif

#endif  // _TSDB_META_H_