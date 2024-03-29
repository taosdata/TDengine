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

#ifndef _TD_VNODE_META_H_
#define _TD_VNODE_META_H_

#include "index.h"
#include "metaTtl.h"
#include "vnodeInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMetaIdx   SMetaIdx;
typedef struct SMetaDB    SMetaDB;
typedef struct SMetaCache SMetaCache;

// metaDebug ==================
// clang-format off
#define metaFatal(...) do { if (metaDebugFlag & DEBUG_FATAL) { taosPrintLog("MTA FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define metaError(...) do { if (metaDebugFlag & DEBUG_ERROR) { taosPrintLog("MTA ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define metaWarn(...)  do { if (metaDebugFlag & DEBUG_WARN)  { taosPrintLog("MTA WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define metaInfo(...)  do { if (metaDebugFlag & DEBUG_INFO)  { taosPrintLog("MTA ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define metaDebug(...) do { if (metaDebugFlag & DEBUG_DEBUG) { taosPrintLog("MTA ", DEBUG_DEBUG, metaDebugFlag, __VA_ARGS__); }} while(0)
#define metaTrace(...) do { if (metaDebugFlag & DEBUG_TRACE) { taosPrintLog("MTA ", DEBUG_TRACE, metaDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

// metaOpen ==================
int32_t metaRLock(SMeta* pMeta);
int32_t metaWLock(SMeta* pMeta);
int32_t metaULock(SMeta* pMeta);

// metaEntry ==================
int metaEncodeEntry(SEncoder* pCoder, const SMetaEntry* pME);
int metaDecodeEntry(SDecoder* pCoder, SMetaEntry* pME);

// metaQuery ==================
int metaGetTableEntryByVersion(SMetaReader* pReader, int64_t version, tb_uid_t uid);

// metaIdx ==================
int  metaOpenIdx(SMeta* pMeta);
void metaCloseIdx(SMeta* pMeta);
int  metaSaveTableToIdx(SMeta* pMeta, const STbCfg* pTbOptions);
int  metaRemoveTableFromIdx(SMeta* pMeta, tb_uid_t uid);

// metaCommit ==================
static FORCE_INLINE tb_uid_t metaGenerateUid(SMeta* pMeta) { return tGenIdPI64(); }

// metaTable ==================
int metaHandleEntry(SMeta* pMeta, const SMetaEntry* pME);

// metaCache ==================
int32_t metaCacheOpen(SMeta* pMeta);
void    metaCacheClose(SMeta* pMeta);
int32_t metaCacheUpsert(SMeta* pMeta, SMetaInfo* pInfo);
int32_t metaCacheDrop(SMeta* pMeta, int64_t uid);

int32_t metaStatsCacheUpsert(SMeta* pMeta, SMetaStbStats* pInfo);
int32_t metaStatsCacheDrop(SMeta* pMeta, int64_t uid);
int32_t metaStatsCacheGet(SMeta* pMeta, int64_t uid, SMetaStbStats* pInfo);
void    metaUpdateStbStats(SMeta* pMeta, int64_t uid, int64_t deltaCtb, int32_t deltaCol);
int32_t metaUidFilterCacheGet(SMeta* pMeta, uint64_t suid, const void* pKey, int32_t keyLen, LRUHandle** pHandle);

struct SMeta {
  TdThreadRwlock lock;

  char*   path;
  SVnode* pVnode;
  bool    changed;
  TDB*    pEnv;
  TXN*    txn;
  TTB*    pTbDb;
  TTB*    pSkmDb;
  TTB*    pUidIdx;
  TTB*    pNameIdx;
  TTB*    pCtbIdx;
  TTB*    pSuidIdx;
  // ivt idx and idx
  void* pTagIvtIdx;

  TTB*        pTagIdx;
  STtlManger* pTtlMgr;

  TTB* pBtimeIdx;  // table created time idx
  TTB* pNcolIdx;   // ncol of table idx, normal table only

  TTB* pSmaIdx;

  // stream
  TTB* pStreamDb;

  SMetaIdx* pIdx;

  SMetaCache* pCache;
};

typedef struct {
  int64_t  version;
  tb_uid_t uid;
} STbDbKey;

#pragma pack(push, 1)
typedef struct {
  tb_uid_t suid;
  int64_t  version;
  int32_t  skmVer;
} SUidIdxVal;

typedef struct {
  tb_uid_t uid;
  int32_t  sver;
} SSkmDbKey;
#pragma pack(pop)

typedef struct {
  tb_uid_t suid;
  tb_uid_t uid;
} SCtbIdxKey;

#pragma pack(push, 1)
typedef struct {
  tb_uid_t suid;
  int32_t  cid;
  uint8_t  isNull : 1;
  uint8_t  type : 7;
  uint8_t  data[];  // val + uid
} STagIdxKey;
#pragma pack(pop)

typedef struct {
  tb_uid_t uid;
  int64_t  smaUid;
} SSmaIdxKey;

typedef struct {
  int64_t  btime;
  tb_uid_t uid;
} SBtimeIdxKey;

typedef struct {
  int64_t  ncol;
  tb_uid_t uid;
} SNcolIdxKey;

// metaTable ==================
int metaCreateTagIdxKey(tb_uid_t suid, int32_t cid, const void* pTagData, int32_t nTagData, int8_t type, tb_uid_t uid,
                        STagIdxKey** ppTagIdxKey, int32_t* nTagIdxKey);

// TODO, refactor later
int32_t metaFilterTableIds(void *pVnode, SMetaFltParam *param, SArray *results);
int32_t metaFilterCreateTime(void *pVnode, SMetaFltParam *parm, SArray *pUids);
int32_t metaFilterTableName(void *pVnode, SMetaFltParam *param, SArray *pUids);
int32_t metaFilterTtl(void *pVnode, SMetaFltParam *param, SArray *pUids);

#ifndef META_REFACT
// SMetaDB
int  metaOpenDB(SMeta* pMeta);
void metaCloseDB(SMeta* pMeta);
int  metaSaveTableToDB(SMeta* pMeta, STbCfg* pTbCfg, STbDdlH* pHandle);
int  metaRemoveTableFromDb(SMeta* pMeta, tb_uid_t uid);
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_META_H_*/
