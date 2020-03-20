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

#ifndef TDENGINE_MNODE_SDB_H
#define TDENGINE_MNODE_SDB_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  SDB_KEYTYPE_STRING, 
  SDB_KEYTYPE_AUTO, 
  SDB_KEYTYPE_MAX
} ESdbKeyType;

typedef enum {
  SDB_OPER_GLOBAL,
  SDB_OPER_LOCAL,
  SDB_OPER_DISK
} ESdbOperType;

enum _sdbaction {
  SDB_TYPE_INSERT,
  SDB_TYPE_DELETE,
  SDB_TYPE_UPDATE,
} ESdbForwardType;

typedef struct {
  char   *tableName;
  int32_t hashSessions;
  int32_t maxRowSize;
  ESdbKeyType keyType;
  int32_t (*insertFp)(void *pObj);
  int32_t (*deleteFp)(void *pObj);
  int32_t (*updateFp)(void *pObj);
  int32_t (*encodeFp)(void *pObj, void *pData, int32_t maxRowSize);
  void *  (*decodeFp)(void *pData);  
  int32_t (*destroyFp)(void *pObj);
} SSdbTableDesc;

void *sdbOpenTable(SSdbTableDesc *desc);
void sdbCloseTable(void *handle);

int32_t sdbInsertRow(void *handle, void *row, ESdbOperType oper);
int32_t sdbDeleteRow(void *handle, void *key, ESdbOperType oper);
int32_t sdbUpdateRow(void *handle, void *row, int32_t rowSize, ESdbOperType oper);

void *sdbGetRow(void *handle, void *key);
void *sdbFetchRow(void *handle, void *pNode, void **ppRow);
int64_t sdbGetId(void *handle);
int64_t sdbGetNumOfRows(void *handle);
uint64_t sdbGetVersion();

#ifdef __cplusplus
}
#endif

#endif