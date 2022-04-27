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

#include "vnodeInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMetaIdx    SMetaIdx;
typedef struct SMetaDB     SMetaDB;
typedef struct SMSmaCursor SMSmaCursor;

// metaDebug ==================
// clang-format off
#define metaFatal(...) do { if (metaDebugFlag & DEBUG_FATAL) { taosPrintLog("META FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define metaError(...) do { if (metaDebugFlag & DEBUG_ERROR) { taosPrintLog("META ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define metaWarn(...)  do { if (metaDebugFlag & DEBUG_WARN)  { taosPrintLog("META WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define metaInfo(...)  do { if (metaDebugFlag & DEBUG_INFO)  { taosPrintLog("META ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define metaDebug(...) do { if (metaDebugFlag & DEBUG_DEBUG) { taosPrintLog("META ", DEBUG_DEBUG, metaDebugFlag, __VA_ARGS__); }} while(0)
#define metaTrace(...) do { if (metaDebugFlag & DEBUG_TRACE) { taosPrintLog("META ", DEBUG_TRACE, metaDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

// metaOpen ==================

// metaEntry ==================
int metaEncodeEntry(SCoder* pCoder, const SMetaEntry* pME);
int metaDecodeEntry(SCoder* pCoder, SMetaEntry* pME);

// metaTable ==================
int metaDropSTable(SMeta* pMeta, int64_t verison, SVDropStbReq* pReq);

// metaQuery ==================
int metaGetTableEntryByVersion(SMetaReader* pReader, int64_t version, tb_uid_t uid);

// metaIdx ==================
int  metaOpenIdx(SMeta* pMeta);
void metaCloseIdx(SMeta* pMeta);
int  metaSaveTableToIdx(SMeta* pMeta, const STbCfg* pTbOptions);
int  metaRemoveTableFromIdx(SMeta* pMeta, tb_uid_t uid);

// metaCommit ==================
static FORCE_INLINE tb_uid_t metaGenerateUid(SMeta* pMeta) { return tGenIdPI64(); }

struct SMeta {
  char*     path;
  SVnode*   pVnode;
  TENV*     pEnv;
  TXN       txn;
  TDB*      pTbDb;
  TDB*      pSkmDb;
  TDB*      pUidIdx;
  TDB*      pNameIdx;
  TDB*      pCtbIdx;
  TDB*      pTagIdx;
  TDB*      pTtlIdx;
  SMetaIdx* pIdx;
};

typedef struct {
  int64_t  version;
  tb_uid_t uid;
} STbDbKey;

typedef struct __attribute__((__packed__)) {
  tb_uid_t uid;
  int32_t  sver;
} SSkmDbKey;

typedef struct {
  tb_uid_t suid;
  tb_uid_t uid;
} SCtbIdxKey;

typedef struct __attribute__((__packed__)) {
  tb_uid_t suid;
  int16_t  cid;
  char     data[];
} STagIdxKey;

typedef struct {
  int64_t  dtime;
  tb_uid_t uid;
} STtlIdxKey;

#if 1

// int             metaCreateTable(SMeta* pMeta, STbCfg* pTbCfg, STbDdlH* pHandle);
int          metaDropTable(SMeta* pMeta, tb_uid_t uid);
SMSmaCursor* metaOpenSmaCursor(SMeta* pMeta, tb_uid_t uid);
void         metaCloseSmaCursor(SMSmaCursor* pSmaCur);
int64_t      metaSmaCursorNext(SMSmaCursor* pSmaCur);

#ifndef META_REFACT
// SMetaDB
int  metaOpenDB(SMeta* pMeta);
void metaCloseDB(SMeta* pMeta);
int  metaSaveTableToDB(SMeta* pMeta, STbCfg* pTbCfg, STbDdlH* pHandle);
int  metaRemoveTableFromDb(SMeta* pMeta, tb_uid_t uid);
int  metaSaveSmaToDB(SMeta* pMeta, STSma* pTbCfg);
int  metaRemoveSmaFromDb(SMeta* pMeta, int64_t indexUid);
#endif

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_META_H_*/