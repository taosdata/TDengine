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
  SDB_TABLE_DNODE   = 0,
  SDB_TABLE_MNODE   = 1,
  SDB_TABLE_ACCOUNT = 2,
  SDB_TABLE_USER    = 3,
  SDB_TABLE_DB      = 4,
  SDB_TABLE_VGROUP  = 5,
  SDB_TABLE_STABLE  = 6,
  SDB_TABLE_CTABLE  = 7,
  SDB_TABLE_MAX     = 8
} ESdbTable;

typedef enum {
  SDB_KEY_STRING, 
  SDB_KEY_INT,
  SDB_KEY_AUTO
} ESdbKeyType;

typedef enum {
  SDB_OPER_GLOBAL,
  SDB_OPER_LOCAL
} ESdbOperType;

typedef struct {
  ESdbOperType type;
  void *  table;
  void *  pObj;
  int32_t rowSize;
  void *  rowData;
} SSdbOperDesc;

typedef struct {
  char   *tableName;
  int32_t hashSessions;
  int32_t maxRowSize;
  int32_t refCountPos;
  ESdbTable   tableId;
  ESdbKeyType keyType;
  int32_t (*insertFp)(SSdbOperDesc *pOper);
  int32_t (*deleteFp)(SSdbOperDesc *pOper);
  int32_t (*updateFp)(SSdbOperDesc *pOper);
  int32_t (*encodeFp)(SSdbOperDesc *pOper);
  int32_t (*decodeFp)(SSdbOperDesc *pDesc);  
  int32_t (*destroyFp)(SSdbOperDesc *pDesc);
  int32_t (*restoredFp)();
} SSdbTableDesc;

typedef struct {
  int64_t version;
  void *  wal;
  pthread_mutex_t mutex;
} SSdbObject;

int32_t sdbInit();
void    sdbCleanUp();
SSdbObject *sdbGetObj();

void *  sdbOpenTable(SSdbTableDesc *desc);
void    sdbCloseTable(void *handle);
int     sdbProcessWrite(void *param, void *data, int type);

int32_t sdbInsertRow(SSdbOperDesc *pOper);
int32_t sdbDeleteRow(SSdbOperDesc *pOper);
int32_t sdbUpdateRow(SSdbOperDesc *pOper);

void    *sdbGetRow(void *handle, void *key);
void    *sdbFetchRow(void *handle, void *pNode, void **ppRow);
void     sdbIncRef(void *thandle, void *pRow);
void     sdbDecRef(void *thandle, void *pRow);
int64_t  sdbGetNumOfRows(void *handle);
int32_t  sdbGetId(void *handle);
uint64_t sdbGetVersion();

#ifdef __cplusplus
}
#endif

#endif