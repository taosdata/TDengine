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

// metaEntry ==================
typedef struct SMetaEntry SMetaEntry;

int metaEncodeEntry(SCoder* pCoder, const SMetaEntry* pME);
int metaDecodeEntry(SCoder* pCoder, SMetaEntry* pME);

// metaTable ==================
int metaCreateSTable(SMeta* pMeta, int64_t version, SVCreateStbReq* pReq);
int metaDropSTable(SMeta* pMeta, int64_t verison, SVDropStbReq* pReq);
int metaCreateTable(SMeta* pMeta, int64_t version, SVCreateTbReq* pReq);

// metaQuery ==================
typedef struct SMetaEntryReader SMetaEntryReader;

void metaEntryReaderInit(SMetaEntryReader* pReader);
void metaEntryReaderClear(SMetaEntryReader* pReader);
int  metaGetTableEntryByVersion(SMeta* pMeta, SMetaEntryReader* pReader, int64_t version, tb_uid_t uid);
int  metaGetTableEntryByUid(SMeta* pMeta, SMetaEntryReader* pReader, tb_uid_t uid);
int  metaGetTableEntryByName(SMeta* pMeta, SMetaEntryReader* pReader, const char* name);

// metaIdx ==================
int  metaOpenIdx(SMeta* pMeta);
void metaCloseIdx(SMeta* pMeta);
int  metaSaveTableToIdx(SMeta* pMeta, const STbCfg* pTbOptions);
int  metaRemoveTableFromIdx(SMeta* pMeta, tb_uid_t uid);

// metaCommit ==================
int metaBegin(SMeta* pMeta);

static FORCE_INLINE tb_uid_t metaGenerateUid(SMeta* pMeta) { return tGenIdPI64(); }

struct SMeta {
  char*     path;
  SVnode*   pVnode;
  TENV*     pEnv;
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
#define META_SUPER_TABLE  TD_SUPER_TABLE
#define META_CHILD_TABLE  TD_CHILD_TABLE
#define META_NORMAL_TABLE TD_NORMAL_TABLE

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

struct SMetaEntry {
  int8_t      type;
  tb_uid_t    uid;
  const char* name;
  union {
    struct {
      SSchemaWrapper schema;
      SSchemaWrapper schemaTag;
    } stbEntry;
    struct {
      int64_t     ctime;
      int32_t     ttlDays;
      tb_uid_t    suid;
      const void* pTags;
    } ctbEntry;
    struct {
      int64_t        ctime;
      int32_t        ttlDays;
      SSchemaWrapper schema;
    } ntbEntry;
  };
};

struct SMetaEntryReader {
  SCoder     coder;
  SMetaEntry me;
  void*      pBuf;
  int        szBuf;
};

#ifndef META_REFACT
// SMetaDB
int  metaOpenDB(SMeta* pMeta);
void metaCloseDB(SMeta* pMeta);
int  metaSaveTableToDB(SMeta* pMeta, STbCfg* pTbCfg);
int  metaRemoveTableFromDb(SMeta* pMeta, tb_uid_t uid);
int  metaSaveSmaToDB(SMeta* pMeta, STSma* pTbCfg);
int  metaRemoveSmaFromDb(SMeta* pMeta, int64_t indexUid);
#endif

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_META_H_*/