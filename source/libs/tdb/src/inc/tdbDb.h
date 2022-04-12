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

#ifndef _TD_TDB_DB_H_
#define _TD_TDB_DB_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STDB  TDB;
typedef struct STDBC TDBC;

// TDB
int tdbDbOpen(const char *fname, int keyLen, int valLen, FKeyComparator keyCmprFn, TENV *pEnv, TDB **ppDb);
int tdbDbClose(TDB *pDb);
int tdbDbDrop(TDB *pDb);
int tdbDbInsert(TDB *pDb, const void *pKey, int keyLen, const void *pVal, int valLen, TXN *pTxn);
int tdbDbGet(TDB *pDb, const void *pKey, int kLen, void **ppVal, int *vLen);
int tdbDbPGet(TDB *pDb, const void *pKey, int kLen, void **ppKey, int *pkLen, void **ppVal, int *vLen);

// TDBC
int tdbDbcOpen(TDB *pDb, TDBC **ppDbc);
int tdbDbNext(TDBC *pDbc, void **ppKey, int *kLen, void **ppVal, int *vLen);
int tdbDbcClose(TDBC *pDbc);
int tdbDbcInsert(TDBC *pDbc, const void *pKey, int keyLen, const void *pVal, int valLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_DB_H_*/