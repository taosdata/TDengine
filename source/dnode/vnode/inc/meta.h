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

#ifndef _TD_META_H_
#define _TD_META_H_

#include "tmallocator.h"
#include "tmsg.h"
#include "trow.h"

#ifdef __cplusplus
extern "C" {
#endif

#define META_SUPER_TABLE  TD_SUPER_TABLE
#define META_CHILD_TABLE  TD_CHILD_TABLE
#define META_NORMAL_TABLE TD_NORMAL_TABLE

// Types exported
typedef struct SMeta SMeta;

typedef struct SMetaCfg {
  /// LRU cache size
  uint64_t lruSize;
} SMetaCfg;

typedef struct SMTbCursor  SMTbCursor;
typedef struct SMCtbCursor SMCtbCursor;
typedef struct SMSmaCursor SMSmaCursor;

typedef SVCreateTbReq   STbCfg;
typedef SVCreateTSmaReq SSmaCfg;

// SMeta operations
SMeta * metaOpen(const char *path, const SMetaCfg *pMetaCfg, SMemAllocatorFactory *pMAF);
void    metaClose(SMeta *pMeta);
void    metaRemove(const char *path);
int     metaCreateTable(SMeta *pMeta, STbCfg *pTbCfg);
int     metaDropTable(SMeta *pMeta, tb_uid_t uid);
int     metaCommit(SMeta *pMeta);
int32_t metaCreateTSma(SMeta *pMeta, SSmaCfg *pCfg);
int32_t metaDropTSma(SMeta *pMeta, int64_t indexUid);

// For Query
STbCfg *        metaGetTbInfoByUid(SMeta *pMeta, tb_uid_t uid);
STbCfg *        metaGetTbInfoByName(SMeta *pMeta, char *tbname, tb_uid_t *uid);
SSchemaWrapper *metaGetTableSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver, bool isinline);
STSchema *      metaGetTbTSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver);
STSma *         metaGetSmaInfoByIndex(SMeta *pMeta, int64_t indexUid);
STSmaWrapper *  metaGetSmaInfoByTable(SMeta *pMeta, tb_uid_t uid);
SArray *        metaGetSmaTbUids(SMeta *pMeta, bool isDup);
int             metaGetTbNum(SMeta *pMeta);

SMTbCursor *metaOpenTbCursor(SMeta *pMeta);
void        metaCloseTbCursor(SMTbCursor *pTbCur);
char *      metaTbCursorNext(SMTbCursor *pTbCur);

SMCtbCursor *metaOpenCtbCursor(SMeta *pMeta, tb_uid_t uid);
void         metaCloseCtbCurosr(SMCtbCursor *pCtbCur);
tb_uid_t     metaCtbCursorNext(SMCtbCursor *pCtbCur);

SMSmaCursor *metaOpenSmaCursor(SMeta *pMeta, tb_uid_t uid);
void         metaCloseSmaCurosr(SMSmaCursor *pSmaCur);
const char * metaSmaCursorNext(SMSmaCursor *pSmaCur);

// Options
void metaOptionsInit(SMetaCfg *pMetaCfg);
void metaOptionsClear(SMetaCfg *pMetaCfg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_META_H_*/
