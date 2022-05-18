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

#ifndef _TD_TDB_H_
#define _TD_TDB_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*tdb_cmpr_fn_t)(const void *pKey1, int kLen1, const void *pKey2, int kLen2);

// exposed types
typedef struct STEnv TENV;
typedef struct STDB  TDB;
typedef struct STDBC TDBC;
typedef struct STxn  TXN;

// TENV
int tdbEnvOpen(const char *rootDir, int szPage, int pages, TENV **ppEnv);
int tdbEnvClose(TENV *pEnv);
int tdbBegin(TENV *pEnv, TXN *pTxn);
int tdbCommit(TENV *pEnv, TXN *pTxn);

// TDB
int tdbOpen(const char *fname, int keyLen, int valLen, tdb_cmpr_fn_t keyCmprFn, TENV *pEnv, TDB **ppDb);
int tdbClose(TDB *pDb);
int tdbDrop(TDB *pDb);
int tdbInsert(TDB *pDb, const void *pKey, int keyLen, const void *pVal, int valLen, TXN *pTxn);
int tdbDelete(TDB *pDb, const void *pKey, int kLen, TXN *pTxn);
int tdbUpsert(TDB *pDb, const void *pKey, int kLen, const void *pVal, int vLen, TXN *pTxn);
int tdbGet(TDB *pDb, const void *pKey, int kLen, void **ppVal, int *vLen);
int tdbPGet(TDB *pDb, const void *pKey, int kLen, void **ppKey, int *pkLen, void **ppVal, int *vLen);

// TDBC
int tdbDbcOpen(TDB *pDb, TDBC **ppDbc, TXN *pTxn);
int tdbDbcClose(TDBC *pDbc);
int tdbDbcIsValid(TDBC *pDbc);
int tdbDbcMoveTo(TDBC *pDbc, const void *pKey, int kLen, int *c);
int tdbDbcMoveToFirst(TDBC *pDbc);
int tdbDbcMoveToLast(TDBC *pDbc);
int tdbDbcMoveToNext(TDBC *pDbc);
int tdbDbcMoveToPrev(TDBC *pDbc);
int tdbDbcGet(TDBC *pDbc, const void **ppKey, int *pkLen, const void **ppVal, int *pvLen);
int tdbDbcDelete(TDBC *pDbc);
int tdbDbcNext(TDBC *pDbc, void **ppKey, int *kLen, void **ppVal, int *vLen);
int tdbDbcUpsert(TDBC *pDbc, const void *pKey, int nKey, const void *pData, int nData, int insert);

// TXN
#define TDB_TXN_WRITE            0x1
#define TDB_TXN_READ_UNCOMMITTED 0x2

int tdbTxnOpen(TXN *pTxn, int64_t txnid, void *(*xMalloc)(void *, size_t), void (*xFree)(void *, void *), void *xArg,
               int flags);
int tdbTxnClose(TXN *pTxn);

// other
void tdbFree(void *);

struct STxn {
  int     flags;
  int64_t txnId;
  void *(*xMalloc)(void *, size_t);
  void (*xFree)(void *, void *);
  void *xArg;
};

// error code
enum { TDB_CODE_SUCCESS = 0, TDB_CODE_MAX };

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_H_*/