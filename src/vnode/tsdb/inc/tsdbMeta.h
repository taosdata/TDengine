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

#ifdef __cplusplus
extern "C" {
#endif

// #include "taosdef.h"

// Initially, there are 4 tables
#define TSDB_INIT_NUMBER_OF_SUPER_TABLE 4

typedef enum {
  TSDB_SUPER_TABLE,  // super table
  TSDB_NTABLE,       // table not created from super table
  TSDB_STABLE        // table created from super table
} TSDB_TABLE_TYPE;

#define IS_CREATE_STABLE(pCfg) ((pCfg)->tagValues != NULL)

typedef struct STable {
  STableId        tableId;
  TSDB_TABLE_TYPE type;

  int64_t createdTime;

  // super table UID -1 for normal table
  int32_t stableUid;

  int32_t numOfCols;

  // Schema for this table
  // For TSDB_SUPER_TABLE, it is the schema including tags
  // For TSDB_NTABLE, it is only the schema, not including tags
  // For TSDB_STABLE, it is NULL
  SSchema *pSchema;

  // Tag value for this table
  // For TSDB_SUPER_TABLE and TSDB_NTABLE, it is NULL
  // For TSDB_STABLE, it is the tag value string
  SDataRow pTagVal;

  // Object content;
  // For TSDB_SUPER_TABLE, it is the index of tables created from it
  // For TSDB_STABLE and TSDB_NTABLE, it is the cache data
  union {
    void *pData;
    void *pIndex;
  } content;

  // A handle to deal with event
  void *eventHandler;

  // A handle to deal with stream
  void *streamHandler;

  struct STable *next;

} STable;

typedef struct {
  int32_t  maxTables;
  int32_t  nTables;
  STable **tables;    // array of normal tables
  STable * stables;   // linked list of super tables // TODO use container to implement this
  void *   tableMap;  // hash map of uid ==> STable *
} STsdbMeta;

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

SSchema *tsdbGetTableSchema(STable *pTable);

// ---- Operation on SMetaHandle
#define TSDB_NUM_OF_TABLES(pHandle) ((pHandle)->numOfTables)
#define TSDB_NUM_OF_SUPER_TABLES(pHandle) ((pHandle)->numOfSuperTables)
#define TSDB_TABLE_OF_ID(pHandle, id) ((pHandle)->pTables)[id]
#define TSDB_GET_TABLE_OF_NAME(pHandle, name) /* TODO */

// Create a new meta handle with configuration
STsdbMeta *tsdbCreateMeta(int32_t maxTables);
int32_t    tsdbFreeMeta(STsdbMeta *pMeta);

// Recover the meta handle from the file
STsdbMeta *tsdbOpenMeta(char *tsdbDir);

int32_t tsdbCreateTableImpl(STsdbMeta *pMeta, STableCfg *pCfg);
int32_t tsdbDropTableImpl(STsdbMeta *pMeta, STableId tableId);

int32_t tsdbInsertDataImpl(STsdbMeta *pMeta, STableId tableId, SDataRows rows);

#ifdef __cplusplus
}
#endif

#endif  // _TSDB_META_H_