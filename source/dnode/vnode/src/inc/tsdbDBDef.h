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

#ifndef _TD_TSDB_DB_DEF_H_
#define _TD_TSDB_DB_DEF_H_

#include "db.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SDBFile SDBFile;
typedef DB_ENV*        TDBEnv;

struct SDBFile {
  DB*   pDB;
  char* path;
};

int32_t tsdbOpenDBF(TDBEnv pEnv, SDBFile* pDBF);
void    tsdbCloseDBF(SDBFile* pDBF);
int32_t tsdbOpenBDBEnv(DB_ENV** ppEnv, const char* path);
void    tsdbCloseBDBEnv(DB_ENV* pEnv);
int32_t tsdbSaveSmaToDB(SDBFile* pDBF, void* key, uint32_t keySize, void* data, uint32_t dataSize);
void*   tsdbGetSmaDataByKey(SDBFile* pDBF, void* key, uint32_t keySize, uint32_t* valueSize);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TSDB_DB_DEF_H_*/
