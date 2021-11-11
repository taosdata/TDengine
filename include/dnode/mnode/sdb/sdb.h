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

#ifdef __cplusplus
extern "C" {
#endif

#define SDB_GET_BINARY_VAL(pData, dataLen, val, valLen, code) \
  if (code == 0) {                                            \
    if ((dataLen) >= (valLen)) {                              \
      memcpy((val), (char *)(pData), (valLen));               \
      (dataLen) -= (valLen);                                  \
      (pData) = (char *)(pData) + (valLen);                   \
    } else {                                                  \
      code = TSDB_CODE_SDB_INVALID_DATA_LEN;                  \
    }                                                         \
  }

#define SDB_GET_INT32_VAL(pData, dataLen, val, code) \
  if (code == 0) {                                   \
    if (dataLen >= sizeof(int32_t)) {                \
      *(int32_t *)(pData) = (int32_t)(val);          \
      (dataLen) -= sizeof(int32_t);                  \
      (pData) = (char *)(pData) + sizeof(int32_t);   \
    } else {                                         \
      code = TSDB_CODE_SDB_INVALID_DATA_LEN;         \
    }                                                \
  }

#define SDB_GET_INT64_VAL(pData, dataLen, val, code) \
  if (code == 0) {                                   \
    if (dataLen >= sizeof(int64_t)) {                \
      *(int64_t *)(pData) = (int64_t)(val);          \
      (dataLen) -= sizeof(int64_t);                  \
      (pData) = (char *)(pData) + sizeof(int64_t);   \
    } else {                                         \
      code = TSDB_CODE_SDB_INVALID_DATA_LEN;         \
    }                                                \
  }

#define SDB_SET_INT64_VAL(pData, dataLen, val) \
  {                                            \
    *(int64_t *)(pData) = (int64_t)(val);      \
    (dataLen) += sizeof(int64_t);              \
    (pData) += sizeof(int64_t);                \
  }

#define SDB_SET_INT32_VAL(pData, dataLen, val) \
  {                                            \
    *(int32_t *)(pData) = (int32_t)(val);      \
    (dataLen) += sizeof(int32_t);              \
    (pData) += sizeof(int32_t);                \
  }

#define SDB_SET_BINARY_VAL(pData, dataLen, val, valLen) \
  {                                                     \
    memcpy((char *)(pData), (val), (valLen));           \
    (dataLen) += (valLen);                              \
    (pData) += (valLen);                                \
  }

typedef enum {
  SDB_START = 0,
  SDB_TRANS = 1,
  SDB_CLUSTER = 2,
  SDB_DNODE = 3,
  SDB_MNODE = 4,
  SDB_ACCT = 5,
  SDB_AUTH = 6,
  SDB_USER = 7,
  SDB_DB = 8,
  SDB_VGROUP = 9,
  SDB_STABLE = 10,
  SDB_FUNC = 11,
  SDB_MAX = 12
} ESdbType;

typedef enum { SDB_ACTION_INSERT = 1, SDB_ACTION_UPDATE = 2, SDB_ACTION_DELETE = 3 } ESdbAction;
typedef enum { SDB_KEY_BINARY = 1, SDB_KEY_INT32 = 2, SDB_KEY_INT64 = 3 } EKeyType;
typedef enum { SDB_STATUS_CREATING = 1, SDB_STATUS_READY = 2, SDB_STATUS_DROPPING = 3 } ESdbStatus;

typedef struct {
  int8_t  type;
  int8_t  sver;
  int8_t  status;
  int8_t  action;
  int8_t  reserved[4];
  int32_t cksum;
  int32_t dataLen;
  char    data[];
} SSdbRaw;

typedef int32_t (*SdbInsertFp)(void *pObj);
typedef int32_t (*SdbUpdateFp)(void *pSrcObj, void *pDstObj);
typedef int32_t (*SdbDeleteFp)(void *pObj);
typedef int32_t (*SdbDeployFp)();
typedef void *(*SdbDecodeFp)(SSdbRaw *pRaw);
typedef SSdbRaw *(*SdbEncodeFp)(void *pObj);

typedef struct {
  ESdbType    sdbType;
  EKeyType    keyType;
  SdbDeployFp deployFp;
  SdbEncodeFp encodeFp;
  SdbDecodeFp decodeFp;
  SdbInsertFp insertFp;
  SdbUpdateFp updateFp;
  SdbDeleteFp deleteFp;
} SSdbTable;

int32_t sdbInit();
void    sdbCleanup();
void    sdbSetTable(SSdbTable table);

int32_t sdbRead();
int32_t sdbWrite(SSdbRaw *pRaw);
int32_t sdbCommit();

int32_t sdbDeploy();
void    sdbUnDeploy();

void   *sdbAcquire(ESdbType sdb, void *pKey);
void    sdbRelease(void *pObj);
void   *sdbFetch(ESdbType sdb, void *pIter);
void    sdbCancelFetch(ESdbType sdb, void *pIter);
int32_t sdbGetSize(ESdbType sdb);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SDB_H_*/
