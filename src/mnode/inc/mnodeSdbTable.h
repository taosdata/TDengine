/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
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

#ifndef TDENGINE_MNODE_SDB_TABLE_H
#define TDENGINE_MNODE_SDB_TABLE_H

typedef enum mnodeSdbTableType {
  SDB_TABLE_HASH_TABLE  = 0,
  SDB_TABLE_CACHE_TABLE   = 1,
} mnodeSdbTableType;

struct mnodeSdbTable;
typedef struct mnodeSdbTable mnodeSdbTable;

struct SWalHead;
struct SSdbRow;
struct SWalHeadInfo;

typedef struct mnodeSdbTableOption {
  int32_t           hashSessions;
  int               keyType;  
  mnodeSdbTableType tableType;
  void*             userData;

  /* only used in SDB_TABLE_CACHE_TABLE */
  int       (*afterLoadFp)(void*,void* value, int32_t nBytes, void* pHead, void* pRet);

  int32_t           cacheDataLen; /* only used in SDB_TABLE_CACHE_TABLE */
  int32_t           expireTime; /* item expire time,only used in SDB_TABLE_CACHE_TABLE */
} mnodeSdbTableOption ;

mnodeSdbTable* mnodeSdbTableInit(mnodeSdbTableOption);

void mnodeSdbTableClear(mnodeSdbTable *pTable);

int  mnodeSdbTableGet(mnodeSdbTable *pTable, const void *key, size_t keyLen, void** pRet);
void mnodeSdbTablePut(mnodeSdbTable *pTable, struct SSdbRow* pRow);

void mnodeSdbTableSyncWal(mnodeSdbTable *pTable, void*, void*, void*);

void mnodeSdbTableRemove(mnodeSdbTable *pTable, const struct SSdbRow* pRow);

void *mnodeSdbTableIterate(mnodeSdbTable *pTable, void *p);
int  mnodeSdbTableIterValue(mnodeSdbTable *pTable,void *p, void** pRet);
void mnodeSdbTableCancelIterate(mnodeSdbTable *pTable, void *p);

void mnodeSdbTableFreeValue(mnodeSdbTable *pTable, void *p);

#endif // TDENGINE_MNODE_SDB_TABLE_H