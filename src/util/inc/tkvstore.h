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
#ifndef _TD_KVSTORE_H_
#define _TD_KVSTORE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef int (*iterFunc)(void *, void *cont, int contLen);
typedef void (*afterFunc)(void *);

typedef struct {
  int64_t size;
  int64_t tombSize;
  int64_t nRecords;
  int64_t nDels;
} SStoreInfo;

typedef struct {
  char *     fname;
  int        fd;
  char *     fsnap;
  int        sfd;
  char *     fnew;
  int        nfd;
  SHashObj * map;
  iterFunc   iFunc;
  afterFunc  aFunc;
  void *     appH;
  SStoreInfo info;
} SKVStore;

int       tdCreateKVStore(char *fname);
int       tdDestroyKVStore();
SKVStore *tdOpenKVStore(char *fname, iterFunc iFunc, afterFunc aFunc, void *appH);
void      tdCloseKVStore(SKVStore *pStore);
int       tdKVStoreStartCommit(SKVStore *pStore);
int       tdUpdateRecordInKVStore(SKVStore *pStore, uint64_t uid, void *cont, int contLen);
int       tdKVStoreEndCommit(SKVStore *pStore);

#ifdef __cplusplus
}
#endif

#endif