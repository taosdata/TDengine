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

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMetaIdx    SMetaIdx;
typedef struct SMetaDB     SMetaDB;
typedef struct SMCtbCursor SMCtbCursor;
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
int metaOpen(SVnode* pVnode, SMeta** ppMeta);
int metaClose(SMeta* pMeta);

// metaIdx ==================
int  metaOpenIdx(SMeta* pMeta);
void metaCloseIdx(SMeta* pMeta);
int  metaSaveTableToIdx(SMeta* pMeta, const STbCfg* pTbOptions);
int  metaRemoveTableFromIdx(SMeta* pMeta, tb_uid_t uid);

static FORCE_INLINE tb_uid_t metaGenerateUid(SMeta* pMeta) { return tGenIdPI64(); }

#define META_SUPER_TABLE  TD_SUPER_TABLE
#define META_CHILD_TABLE  TD_CHILD_TABLE
#define META_NORMAL_TABLE TD_NORMAL_TABLE

int             metaCreateTable(SMeta* pMeta, STbCfg* pTbCfg, STbDdlH* pHandle);
int             metaDropTable(SMeta* pMeta, tb_uid_t uid);
int             metaCommit(SMeta* pMeta);
int32_t         metaCreateTSma(SMeta* pMeta, SSmaCfg* pCfg);
int32_t         metaDropTSma(SMeta* pMeta, int64_t indexUid);
STbCfg*         metaGetTbInfoByUid(SMeta* pMeta, tb_uid_t uid);
STbCfg*         metaGetTbInfoByName(SMeta* pMeta, char* tbname, tb_uid_t* uid);
SSchemaWrapper* metaGetTableSchema(SMeta* pMeta, tb_uid_t uid, int32_t sver, bool isinline);
STSchema*       metaGetTbTSchema(SMeta* pMeta, tb_uid_t uid, int32_t sver);
void*           metaGetSmaInfoByIndex(SMeta* pMeta, int64_t indexUid, bool isDecode);
STSmaWrapper*   metaGetSmaInfoByTable(SMeta* pMeta, tb_uid_t uid);
SArray*         metaGetSmaTbUids(SMeta* pMeta, bool isDup);
int             metaGetTbNum(SMeta* pMeta);
SMSmaCursor*    metaOpenSmaCursor(SMeta* pMeta, tb_uid_t uid);
void            metaCloseSmaCursor(SMSmaCursor* pSmaCur);
int64_t         metaSmaCursorNext(SMSmaCursor* pSmaCur);
SMCtbCursor*    metaOpenCtbCursor(SMeta* pMeta, tb_uid_t uid);
void            metaCloseCtbCurosr(SMCtbCursor* pCtbCur);
tb_uid_t        metaCtbCursorNext(SMCtbCursor* pCtbCur);

// SMetaDB
int  metaOpenDB(SMeta* pMeta);
void metaCloseDB(SMeta* pMeta);
int  metaSaveTableToDB(SMeta* pMeta, STbCfg* pTbCfg, STbDdlH* pHandle);
int  metaRemoveTableFromDb(SMeta* pMeta, tb_uid_t uid);
int  metaSaveSmaToDB(SMeta* pMeta, STSma* pTbCfg);
int  metaRemoveSmaFromDb(SMeta* pMeta, int64_t indexUid);

// SMetaIdx

tb_uid_t metaGenerateUid(SMeta* pMeta);

struct SMeta {
  char*     path;
  SVnode*   pVnode;
  SMetaDB*  pDB;
  SMetaIdx* pIdx;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_META_H_*/