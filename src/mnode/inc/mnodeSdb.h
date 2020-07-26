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

struct SMnodeMsg;

typedef enum {
  SDB_TABLE_CLUSTER = 0,
  SDB_TABLE_DNODE   = 1,
  SDB_TABLE_MNODE   = 2,
  SDB_TABLE_ACCOUNT = 3,
  SDB_TABLE_USER    = 4,
  SDB_TABLE_DB      = 5,
  SDB_TABLE_VGROUP  = 6,
  SDB_TABLE_STABLE  = 7,
  SDB_TABLE_CTABLE  = 8,
  SDB_TABLE_MAX     = 9
} ESdbTable;

typedef enum {
  SDB_KEY_STRING, 
  SDB_KEY_INT,
  SDB_KEY_AUTO,
  SDB_KEY_VAR_STRING,
} ESdbKey;

typedef enum {
  SDB_OPER_GLOBAL,
  SDB_OPER_LOCAL
} ESdbOper;

typedef struct {
  ESdbOper type;
  void *   table;
  void *   pObj;
  void *   rowData;
  int32_t  rowSize;
  int32_t  retCode; // for callback in sdb queue
  int32_t  processedCount; // for sync fwd callback
  int32_t  (*cb)(struct SMnodeMsg *pMsg, int32_t code);
  struct SMnodeMsg *pMsg;
} SSdbOper;

typedef struct {
  char   *tableName;
  int32_t hashSessions;
  int32_t maxRowSize;
  int32_t refCountPos;
  ESdbTable tableId;
  ESdbKey   keyType;
  int32_t (*insertFp)(SSdbOper *pOper);
  int32_t (*deleteFp)(SSdbOper *pOper);
  int32_t (*updateFp)(SSdbOper *pOper);
  int32_t (*encodeFp)(SSdbOper *pOper);
  int32_t (*decodeFp)(SSdbOper *pDesc);  
  int32_t (*destroyFp)(SSdbOper *pDesc);
  int32_t (*restoredFp)();
} SSdbTableDesc;

int32_t sdbInit();
void    sdbCleanUp();
void *  sdbOpenTable(SSdbTableDesc *desc);
void    sdbCloseTable(void *handle);
bool    sdbIsMaster();
bool    sdbIsServing();
void    sdbUpdateMnodeRoles();

int32_t sdbInsertRow(SSdbOper *pOper);
int32_t sdbDeleteRow(SSdbOper *pOper);
int32_t sdbUpdateRow(SSdbOper *pOper);

void    *sdbGetRow(void *handle, void *key);
void    *sdbFetchRow(void *handle, void *pIter, void **ppRow);
void     sdbFreeIter(void *pIter);
void     sdbIncRef(void *thandle, void *pRow);
void     sdbDecRef(void *thandle, void *pRow);
int64_t  sdbGetNumOfRows(void *handle);
int32_t  sdbGetId(void *handle);
uint64_t sdbGetVersion();
bool     sdbCheckRowDeleted(void *thandle, void *pRow);

#ifdef __cplusplus
}
#endif

#endif