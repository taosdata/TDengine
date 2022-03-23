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

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SDB_GET_VAL(pData, dataPos, val, pos, func, type) \
  {                                                       \
    if (func(pRaw, dataPos, val) != 0) {                  \
      goto pos;                                           \
    }                                                     \
    dataPos += sizeof(type);                              \
  }

#define SDB_GET_BINARY(pRaw, dataPos, val, valLen, pos)     \
  {                                                         \
    if (sdbGetRawBinary(pRaw, dataPos, val, valLen) != 0) { \
      goto pos;                                             \
    }                                                       \
    dataPos += valLen;                                      \
  }

#define SDB_GET_INT64(pData, dataPos, val, pos) SDB_GET_VAL(pData, dataPos, val, pos, sdbGetRawInt64, int64_t)

#define SDB_GET_INT32(pData, dataPos, val, pos) SDB_GET_VAL(pData, dataPos, val, pos, sdbGetRawInt32, int32_t)

#define SDB_GET_INT16(pData, dataPos, val, pos) SDB_GET_VAL(pData, dataPos, val, pos, sdbGetRawInt16, int16_t)

#define SDB_GET_INT8(pData, dataPos, val, pos) SDB_GET_VAL(pData, dataPos, val, pos, sdbGetRawInt8, int8_t)

#define SDB_GET_RESERVE(pRaw, dataPos, valLen, pos) \
  {                                                 \
    char val[valLen] = {0};                         \
    SDB_GET_BINARY(pRaw, dataPos, val, valLen, pos) \
  }

#define SDB_SET_VAL(pRaw, dataPos, val, pos, func, type) \
  {                                                      \
    if (func(pRaw, dataPos, val) != 0) {                 \
      goto pos;                                          \
    }                                                    \
    dataPos += sizeof(type);                             \
  }

#define SDB_SET_INT64(pRaw, dataPos, val, pos) SDB_SET_VAL(pRaw, dataPos, val, pos, sdbSetRawInt64, int64_t)

#define SDB_SET_INT32(pRaw, dataPos, val, pos) SDB_SET_VAL(pRaw, dataPos, val, pos, sdbSetRawInt32, int32_t)

#define SDB_SET_INT16(pRaw, dataPos, val, pos) SDB_SET_VAL(pRaw, dataPos, val, pos, sdbSetRawInt16, int16_t)

#define SDB_SET_INT8(pRaw, dataPos, val, pos) SDB_SET_VAL(pRaw, dataPos, val, pos, sdbSetRawInt8, int8_t)

#define SDB_SET_BINARY(pRaw, dataPos, val, valLen, pos)     \
  {                                                         \
    if (sdbSetRawBinary(pRaw, dataPos, val, valLen) != 0) { \
      goto pos;                                             \
    }                                                       \
    dataPos += valLen;                                      \
  }

#define SDB_SET_RESERVE(pRaw, dataPos, valLen, pos) \
  {                                                 \
    char val[valLen] = {0};                         \
    SDB_SET_BINARY(pRaw, dataPos, val, valLen, pos) \
  }

#define SDB_SET_DATALEN(pRaw, dataLen, pos)     \
  {                                             \
    if (sdbSetRawDataLen(pRaw, dataLen) != 0) { \
      goto pos;                                 \
    }                                           \
  }

typedef struct SMnode  SMnode;
typedef struct SSdbRaw SSdbRaw;
typedef struct SSdbRow SSdbRow;
typedef enum { SDB_KEY_BINARY = 1, SDB_KEY_INT32 = 2, SDB_KEY_INT64 = 3 } EKeyType;
typedef enum {
  SDB_STATUS_INIT = 0,
  SDB_STATUS_CREATING = 1,
  SDB_STATUS_UPDATING = 2,
  SDB_STATUS_DROPPING = 3,
  SDB_STATUS_READY = 4,
  SDB_STATUS_DROPPED = 5
} ESdbStatus;

typedef enum {
  SDB_TRANS = 0,
  SDB_CLUSTER = 1,
  SDB_MNODE = 2,
  SDB_QNODE = 3,
  SDB_SNODE = 4,
  SDB_BNODE = 5,
  SDB_DNODE = 6,
  SDB_USER = 7,
  SDB_AUTH = 8,
  SDB_ACCT = 9,
  SDB_STREAM = 10,
  SDB_OFFSET = 11,
  SDB_SUBSCRIBE = 12,
  SDB_CONSUMER = 13,
  SDB_TOPIC = 14,
  SDB_VGROUP = 15,
  SDB_SMA = 16,
  SDB_STB = 17,
  SDB_DB = 18,
  SDB_FUNC = 19,
  SDB_MAX = 20
} ESdbType;

typedef struct SSdb SSdb;
typedef int32_t (*SdbInsertFp)(SSdb *pSdb, void *pObj);
typedef int32_t (*SdbUpdateFp)(SSdb *pSdb, void *pSrcObj, void *pDstObj);
typedef int32_t (*SdbDeleteFp)(SSdb *pSdb, void *pObj);
typedef int32_t (*SdbDeployFp)(SMnode *pMnode);
typedef SSdbRow *(*SdbDecodeFp)(SSdbRaw *pRaw);
typedef SSdbRaw *(*SdbEncodeFp)(void *pObj);
typedef bool (*sdbTraverseFp)(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3);

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

typedef struct SSdbOpt {
  const char *path;
  SMnode     *pMnode;
} SSdbOpt;

/**
 * @brief Initialize and start the sdb.
 *
 * @param pOption Option of the sdb.
 * @return SSdb* The sdb object.
 */
SSdb *sdbInit(SSdbOpt *pOption);

/**
 * @brief Stop and cleanup the sdb.
 *
 * @param pSdb The sdb object to close.
 */
void sdbCleanup(SSdb *pSdb);

/**
 * @brief Set the properties of sdb table.
 *
 * @param pSdb The sdb object.
 * @param table The properties of the table.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t sdbSetTable(SSdb *pSdb, SSdbTable table);

/**
 * @brief Set the initial rows of sdb.
 *
 * @param pSdb The sdb object.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t sdbDeploy(SSdb *pSdb);

/**
 * @brief Load sdb from file.
 *
 * @param pSdb The sdb object.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t sdbReadFile(SSdb *pSdb);

/**
 * @brief Write sdb file.
 *
 * @param pSdb The sdb object.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t sdbWriteFile(SSdb *pSdb);

/**
 * @brief Parse and write raw data to sdb, then free the pRaw object
 *
 * @param pSdb The sdb object.
 * @param pRaw The raw data.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t sdbWrite(SSdb *pSdb, SSdbRaw *pRaw);

/**
 * @brief Parse and write raw data to sdb.
 *
 * @param pSdb The sdb object.
 * @param pRaw The raw data.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t sdbWriteNotFree(SSdb *pSdb, SSdbRaw *pRaw);

/**
 * @brief Acquire a row from sdb
 *
 * @param pSdb The sdb object.
 * @param type The type of the row.
 * @param pKey The key value of the row.
 * @return void* The object of the row.
 */
void *sdbAcquire(SSdb *pSdb, ESdbType type, const void *pKey);

/**
 * @brief Release a row from sdb.
 *
 * @param pSdb The sdb object.
 * @param pObj The object of the row.
 */
void sdbRelease(SSdb *pSdb, void *pObj);

/**
 * @brief Traverse a sdb table
 *
 * @param pSdb The sdb object.
 * @param type The type of the table.
 * @param pIter The initial iterator of the table.
 * @param pObj The object of the row just fetched.
 * @return void* The next iterator of the table.
 */
void *sdbFetch(SSdb *pSdb, ESdbType type, void *pIter, void **ppObj);

/**
 * @brief Cancel a traversal
 *
 * @param pSdb The sdb object.
 * @param type The initial iterator of table.
 */
void sdbCancelFetch(SSdb *pSdb, void *pIter);

/**
 * @brief Traverse a sdb
 *
 * @param pSdb The sdb object.
 * @param type The initial iterator of table.
 * @param fp The function pointer.
 * @param p1 The callback param.
 * @param p2 The callback param.
 * @param p3 The callback param.
 */
void sdbTraverse(SSdb *pSdb, ESdbType type, sdbTraverseFp fp, void *p1, void *p2, void *p3);

/**
 * @brief Get the number of rows in the table
 *
 * @param pSdb The sdb object.
 * @param pIter The type of the table.
 * @return int32_t The number of rows in the table
 */
int32_t sdbGetSize(SSdb *pSdb, ESdbType type);

/**
 * @brief Get the max id of the table, keyType of table should be INT32
 *
 * @param pSdb The sdb object.
 * @param pIter The type of the table.
 * @return int32_t The max id of the table
 */
int32_t sdbGetMaxId(SSdb *pSdb, ESdbType type);

/**
 * @brief Get the version of the table
 *
 * @param pSdb The sdb object.
 * @param pIter The type of the table.
 * @return int32_t The version of the table
 */
int64_t sdbGetTableVer(SSdb *pSdb, ESdbType type);

/**
 * @brief Update the version of sdb
 *
 * @param pSdb The sdb object.
 * @param val The update value of the version.
 * @return int32_t The current version of sdb
 */
int64_t sdbUpdateVer(SSdb *pSdb, int32_t val);

SSdbRaw *sdbAllocRaw(ESdbType type, int8_t sver, int32_t dataLen);
void     sdbFreeRaw(SSdbRaw *pRaw);
int32_t  sdbSetRawInt8(SSdbRaw *pRaw, int32_t dataPos, int8_t val);
int32_t  sdbSetRawInt16(SSdbRaw *pRaw, int32_t dataPos, int16_t val);
int32_t  sdbSetRawInt32(SSdbRaw *pRaw, int32_t dataPos, int32_t val);
int32_t  sdbSetRawInt64(SSdbRaw *pRaw, int32_t dataPos, int64_t val);
int32_t  sdbSetRawBinary(SSdbRaw *pRaw, int32_t dataPos, const char *pVal, int32_t valLen);
int32_t  sdbSetRawDataLen(SSdbRaw *pRaw, int32_t dataLen);
int32_t  sdbSetRawStatus(SSdbRaw *pRaw, ESdbStatus status);
int32_t  sdbGetRawInt8(SSdbRaw *pRaw, int32_t dataPos, int8_t *val);
int32_t  sdbGetRawInt16(SSdbRaw *pRaw, int32_t dataPos, int16_t *val);
int32_t  sdbGetRawInt32(SSdbRaw *pRaw, int32_t dataPos, int32_t *val);
int32_t  sdbGetRawInt64(SSdbRaw *pRaw, int32_t dataPos, int64_t *val);
int32_t  sdbGetRawBinary(SSdbRaw *pRaw, int32_t dataPos, char *pVal, int32_t valLen);
int32_t  sdbGetRawSoftVer(SSdbRaw *pRaw, int8_t *sver);
int32_t  sdbGetRawTotalSize(SSdbRaw *pRaw);

SSdbRow *sdbAllocRow(int32_t objSize);
void     sdbFreeRow(SSdb *pSdb, SSdbRow *pRow);
void    *sdbGetRowObj(SSdbRow *pRow);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SDB_H_*/
