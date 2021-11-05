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

#ifndef _TD_SDB_H_
#define _TD_SDB_H_

#include "cJSON.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  MN_SDB_START = 0,
  MN_SDB_CLUSTER = 1,
  MN_SDB_DNODE = 2,
  MN_SDB_MNODE = 3,
  MN_SDB_ACCT = 4,
  MN_SDB_AUTH = 5,
  MN_SDB_USER = 6,
  MN_SDB_DB = 7,
  MN_SDB_VGROUP = 8,
  MN_SDB_STABLE = 9,
  MN_SDB_FUNC = 10,
  MN_SDB_OPER = 11,
  MN_SDB_MAX = 12
} EMnSdb;

typedef enum { MN_OP_START = 0, MN_OP_INSERT = 1, MN_OP_UPDATE = 2, MN_OP_DELETE = 3, MN_OP_MAX = 4 } EMnOp;

typedef enum { MN_KEY_START = 0, MN_KEY_BINARY = 1, MN_KEY_INT32 = 2, MN_KEY_INT64 = 3, MN_KEY_MAX } EMnKey;

typedef enum { MN_SDB_STAT_AVAIL = 0, MN_SDB_STAT_DROPPED = 1 } EMnSdbStat;

typedef struct {
  int8_t type;
  int8_t status;
  int8_t align[6];
} SdbHead;

typedef void (*SdbDeployFp)();
typedef void *(*SdbDecodeFp)(cJSON *root);
typedef int32_t (*SdbEncodeFp)(void *pHead, char *buf, int32_t maxLen);

int32_t sdbInit();
void    sdbCleanup();

int32_t sdbRead();
int32_t sdbCommit();

int32_t sdbDeploy();
void    sdbUnDeploy();

void   *sdbInsertRow(EMnSdb sdb, void *pObj);
void    sdbDeleteRow(EMnSdb sdb, void *pHead);
void   *sdbUpdateRow(EMnSdb sdb, void *pHead);
void   *sdbGetRow(EMnSdb sdb, void *pKey);
void   *sdbFetchRow(EMnSdb sdb, void *pIter);
void    sdbCancelFetch(EMnSdb sdb, void *pIter);
int32_t sdbGetCount(EMnSdb sdb);

void sdbSetFp(EMnSdb, EMnKey, SdbDeployFp, SdbEncodeFp, SdbDecodeFp, int32_t dataSize);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SDB_H_*/
