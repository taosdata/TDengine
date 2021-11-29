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

#define SDB_GET_INT64(pData, pRow, dataPos, val)   \
  {                                                \
    if (sdbGetRawInt64(pRaw, dataPos, val) != 0) { \
      sdbFreeRow(pRow);                            \
      return NULL;                                 \
    }                                              \
    dataPos += sizeof(int64_t);                    \
  }

#define SDB_GET_INT32(pData, pRow, dataPos, val)   \
  {                                                \
    if (sdbGetRawInt32(pRaw, dataPos, val) != 0) { \
      sdbFreeRow(pRow);                            \
      return NULL;                                 \
    }                                              \
    dataPos += sizeof(int32_t);                    \
  }

#define SDB_GET_INT8(pData, pRow, dataPos, val)   \
  {                                               \
    if (sdbGetRawInt8(pRaw, dataPos, val) != 0) { \
      sdbFreeRow(pRow);                           \
      return NULL;                                \
    }                                             \
    dataPos += sizeof(int8_t);                    \
  }

#define SDB_GET_BINARY(pRaw, pRow, dataPos, val, valLen)    \
  {                                                         \
    if (sdbGetRawBinary(pRaw, dataPos, val, valLen) != 0) { \
      sdbFreeRow(pRow);                                     \
      return NULL;                                          \
    }                                                       \
    dataPos += valLen;                                      \
  }

#define SDB_SET_INT64(pRaw, dataPos, val)          \
  {                                                \
    if (sdbSetRawInt64(pRaw, dataPos, val) != 0) { \
      sdbFreeRaw(pRaw);                            \
      return NULL;                                 \
    }                                              \
    dataPos += sizeof(int64_t);                    \
  }

#define SDB_SET_INT32(pRaw, dataPos, val)          \
  {                                                \
    if (sdbSetRawInt32(pRaw, dataPos, val) != 0) { \
      sdbFreeRaw(pRaw);                            \
      return NULL;                                 \
    }                                              \
    dataPos += sizeof(int32_t);                    \
  }

#define SDB_SET_INT8(pRaw, dataPos, val)          \
  {                                               \
    if (sdbSetRawInt8(pRaw, dataPos, val) != 0) { \
      sdbFreeRaw(pRaw);                           \
      return NULL;                                \
    }                                             \
    dataPos += sizeof(int8_t);                    \
  }

#define SDB_SET_BINARY(pRaw, dataPos, val, valLen)          \
  {                                                         \
    if (sdbSetRawBinary(pRaw, dataPos, val, valLen) != 0) { \
      sdbFreeRaw(pRaw);                                     \
      return NULL;                                          \
    }                                                       \
    dataPos += valLen;                                      \
  }

#define SDB_SET_DATALEN(pRaw, dataLen)          \
  {                                             \
    if (sdbSetRawDataLen(pRaw, dataLen) != 0) { \
      sdbFreeRaw(pRaw);                         \
      return NULL;                              \
    }                                           \
  }

typedef struct SSdbRaw SSdbRaw;
typedef struct SSdbRow SSdbRow;
typedef enum { SDB_KEY_BINARY = 1, SDB_KEY_INT32 = 2, SDB_KEY_INT64 = 3 } EKeyType;
typedef enum {
  SDB_STATUS_CREATING = 1,
  SDB_STATUS_READY = 2,
  SDB_STATUS_DROPPING = 3,
  SDB_STATUS_DROPPED = 4
} ESdbStatus;

typedef enum {
  SDB_START = 0,
  SDB_TRANS = 1,
  SDB_CLUSTER = 2,
  SDB_DNODE = 3,
  SDB_MNODE = 4,
  SDB_USER = 5,
  SDB_AUTH = 6,
  SDB_ACCT = 7,
  SDB_DB = 8,
  SDB_VGROUP = 9,
  SDB_STABLE = 10,
  SDB_FUNC = 11,
  SDB_MAX = 12
} ESdbType;

typedef struct SSdbOpt {
  const char *path;
} SSdbOpt;

typedef int32_t (*SdbInsertFp)(void *pObj);
typedef int32_t (*SdbUpdateFp)(void *pSrcObj, void *pDstObj);
typedef int32_t (*SdbDeleteFp)(void *pObj);
typedef int32_t (*SdbDeployFp)();
typedef SSdbRow *(*SdbDecodeFp)(SSdbRaw *pRaw);
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

typedef struct SSdb SSdb;

SSdb *sdbOpen(SSdbOpt *pOption);
void  sdbClose(SSdb *pSdb);
void  sdbSetTable(SSdb *pSdb, SSdbTable table);

// int32_t sdbOpen();
// void    sdbClose();
int32_t sdbWrite(SSdbRaw *pRaw);

int32_t sdbDeploy();
void    sdbUnDeploy();

void   *sdbAcquire(ESdbType sdb, void *pKey);
void    sdbRelease(void *pObj);
void   *sdbFetch(ESdbType sdb, void *pIter, void **ppObj);
void    sdbCancelFetch(void *pIter);
int32_t sdbGetSize(ESdbType sdb);

SSdbRaw *sdbAllocRaw(ESdbType sdb, int8_t sver, int32_t dataLen);
void     sdbFreeRaw(SSdbRaw *pRaw);
int32_t  sdbSetRawInt8(SSdbRaw *pRaw, int32_t dataPos, int8_t val);
int32_t  sdbSetRawInt32(SSdbRaw *pRaw, int32_t dataPos, int32_t val);
int32_t  sdbSetRawInt64(SSdbRaw *pRaw, int32_t dataPos, int64_t val);
int32_t  sdbSetRawBinary(SSdbRaw *pRaw, int32_t dataPos, const char *pVal, int32_t valLen);
int32_t  sdbSetRawDataLen(SSdbRaw *pRaw, int32_t dataLen);
int32_t  sdbSetRawStatus(SSdbRaw *pRaw, ESdbStatus status);
int32_t  sdbGetRawInt8(SSdbRaw *pRaw, int32_t dataPos, int8_t *val);
int32_t  sdbGetRawInt32(SSdbRaw *pRaw, int32_t dataPos, int32_t *val);
int32_t  sdbGetRawInt64(SSdbRaw *pRaw, int32_t dataPos, int64_t *val);
int32_t  sdbGetRawBinary(SSdbRaw *pRaw, int32_t dataPos, char *pVal, int32_t valLen);
int32_t  sdbGetRawSoftVer(SSdbRaw *pRaw, int8_t *sver);
int32_t  sdbGetRawTotalSize(SSdbRaw *pRaw);

SSdbRow *sdbAllocRow(int32_t objSize);
void     sdbFreeRow(SSdbRow *pRow);
void    *sdbGetRowObj(SSdbRow *pRow);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SDB_H_*/
