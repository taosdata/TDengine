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

#ifndef _TD_META_DEF_H_
#define _TD_META_DEF_H_

#include "tmallocator.h"

#include "meta.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMetaCache SMetaCache;
typedef struct SMetaIdx   SMetaIdx;
typedef struct SMetaDB    SMetaDB;

// SMetaDB
int  metaOpenDB(SMeta* pMeta);
void metaCloseDB(SMeta* pMeta);
int  metaSaveTableToDB(SMeta* pMeta, STbCfg* pTbCfg);
int  metaRemoveTableFromDb(SMeta* pMeta, tb_uid_t uid);
int  metaSaveSmaToDB(SMeta* pMeta, STSma* pTbCfg);
int  metaRemoveSmaFromDb(SMeta* pMeta, int64_t indexUid);

// SMetaCache
int  metaOpenCache(SMeta* pMeta);
void metaCloseCache(SMeta* pMeta);

// SMetaCfg
extern const SMetaCfg defaultMetaOptions;
// int                   metaValidateOptions(const SMetaCfg*);
void metaOptionsCopy(SMetaCfg* pDest, const SMetaCfg* pSrc);

// SMetaIdx
int  metaOpenIdx(SMeta* pMeta);
void metaCloseIdx(SMeta* pMeta);
int  metaSaveTableToIdx(SMeta* pMeta, const STbCfg* pTbOptions);
int  metaRemoveTableFromIdx(SMeta* pMeta, tb_uid_t uid);

// STbUidGnrt
typedef struct STbUidGenerator {
  tb_uid_t nextUid;
} STbUidGenerator;

// STableUidGenerator
int  metaOpenUidGnrt(SMeta* pMeta);
void metaCloseUidGnrt(SMeta* pMeta);

// tb_uid_t
#define IVLD_TB_UID 0
tb_uid_t metaGenerateUid(SMeta* pMeta);

struct SMeta {
  char*                 path;
  SMetaCfg              options;
  SMetaDB*              pDB;
  SMetaIdx*             pIdx;
  SMetaCache*           pCache;
  STbUidGenerator       uidGnrt;
  SMemAllocatorFactory* pmaf;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_META_DEF_H_*/
