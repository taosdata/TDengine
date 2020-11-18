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

#include "mnode.h"

struct SSdbTable;

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
  SDB_KEY_STRING     = 0, 
  SDB_KEY_INT        = 1,
  SDB_KEY_AUTO       = 2,
  SDB_KEY_VAR_STRING = 3,
} ESdbKey;

typedef enum {
  SDB_OPER_GLOBAL = 0,
  SDB_OPER_LOCAL  = 1
} ESdbOper;

typedef struct SSWriteMsg {
  ESdbOper type;
  int32_t  processedCount;  // for sync fwd callback
  int32_t  code;            // for callback in sdb queue
  int32_t  rowSize;
  void *   rowData;
  int32_t  (*fpReq)(SMnodeMsg *pMsg);
  int32_t  (*fpRsp)(SMnodeMsg *pMsg, int32_t code);
  void *   pRow;
  SMnodeMsg *pMsg;
  struct SSdbTable *pTable;
} SSWriteMsg;

typedef struct {
  char *    tableName;
  int32_t   hashSessions;
  int32_t   maxRowSize;
  int32_t   refCountPos;
  ESdbTable tableId;
  ESdbKey   keyType;
  int32_t (*fpInsert)(SSWriteMsg *pWrite);
  int32_t (*fpDelete)(SSWriteMsg *pWrite);
  int32_t (*fpUpdate)(SSWriteMsg *pWrite);
  int32_t (*fpEncode)(SSWriteMsg *pWrite);
  int32_t (*fpDecode)(SSWriteMsg *pWrite);  
  int32_t (*fpDestroy)(SSWriteMsg *pWrite);
  int32_t (*fpRestored)();
} SSdbTableDesc;

int32_t sdbInit();
void    sdbCleanUp();
void *  sdbOpenTable(SSdbTableDesc *desc);
void    sdbCloseTable(void *handle);
bool    sdbIsMaster();
bool    sdbIsServing();
void    sdbUpdateMnodeRoles();

int32_t sdbInsertRow(SSWriteMsg *pWrite);
int32_t sdbDeleteRow(SSWriteMsg *pWrite);
int32_t sdbUpdateRow(SSWriteMsg *pWrite);
int32_t sdbInsertRowImp(SSWriteMsg *pWrite);

void    *sdbGetRow(void *pTable, void *key);
void    *sdbFetchRow(void *pTable, void *pIter, void **ppRow);
void     sdbFreeIter(void *pIter);
void     sdbIncRef(void *pTable, void *pRow);
void     sdbDecRef(void *pTable, void *pRow);
int64_t  sdbGetNumOfRows(void *pTable);
int32_t  sdbGetId(void *pTable);
uint64_t sdbGetVersion();
bool     sdbCheckRowDeleted(void *pTable, void *pRow);

#ifdef __cplusplus
}
#endif

#endif