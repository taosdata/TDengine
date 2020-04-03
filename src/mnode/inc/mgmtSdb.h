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
  SDB_KEY_TYPE_STRING, 
  SDB_KEY_TYPE_AUTO
} ESdbKeyType;

typedef enum {
  SDB_OPER_TYPE_GLOBAL,
  SDB_OPER_TYPE_LOCAL
} ESdbOperType;

typedef struct {
  ESdbOperType type;
  void *  table;
  void *  pObj;
  int64_t version;
  int32_t maxRowSize;
  int32_t rowSize;
  void *  rowData;
} SSdbOperDesc;

typedef struct {
  char   *tableName;
  int32_t hashSessions;
  int32_t maxRowSize;
  ESdbKeyType keyType;
  int32_t (*insertFp)(SSdbOperDesc *pOper);
  int32_t (*deleteFp)(SSdbOperDesc *pOper);
  int32_t (*updateFp)(SSdbOperDesc *pOper);
  int32_t (*encodeFp)(SSdbOperDesc *pOper);
  int32_t (*decodeFp)(SSdbOperDesc *pDesc);  
  int32_t (*destroyFp)(SSdbOperDesc *pDesc);
} SSdbTableDesc;

void *  sdbOpenTable(SSdbTableDesc *desc);
void    sdbCloseTable(void *handle);

int32_t sdbInsertRow(SSdbOperDesc *pOper);
int32_t sdbDeleteRow(SSdbOperDesc *pOper);
int32_t sdbUpdateRow(SSdbOperDesc *pOper);

void    *sdbGetRow(void *handle, void *key);
void    *sdbFetchRow(void *handle, void *pNode, void **ppRow);
void     sdbIncRef(void *thandle, void *pRow);
void     sdbDecRef(void *thandle, void *pRow);
int64_t  sdbGetNumOfRows(void *handle);
int64_t  sdbGetId(void *handle);
uint64_t sdbGetVersion();

#ifdef __cplusplus
}
#endif

#endif