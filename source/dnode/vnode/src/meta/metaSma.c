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

#include "meta.h"

static int metaHandleSmaEntry(SMeta *pMeta, const SMetaEntry *pME);
static int metaSaveSmaToDB(SMeta *pMeta,  const SMetaEntry *pME);

int32_t metaCreateTSma(SMeta *pMeta, int64_t version, SSmaCfg *pCfg) {
  // TODO: Validate the cfg
  // The table uid should exists and be super table or normal table.
  // Check other cfg value

  // TODO: add atomicity

  SMetaEntry  me = {0};
  int         kLen = 0;
  int         vLen = 0;
  const void *pKey = NULL;
  const void *pVal = NULL;
  void       *pBuf = NULL;
  int32_t     szBuf = 0;
  void       *p = NULL;
  SMetaReader mr = {0};

  // validate req
  metaReaderInit(&mr, pMeta, 0);
  if (metaGetTableEntryByUid(&mr, pCfg->indexUid) == 0) {
// TODO: just for pass case
#if 1
    terrno = TSDB_CODE_TDB_TSMA_ALREADY_EXIST;
    metaReaderClear(&mr);
    return -1;
#else
    metaReaderClear(&mr);
    return 0;
#endif
  }
  metaReaderClear(&mr);

  // set structs
  me.version = version;
  me.type = TSDB_TSMA_TABLE;
  me.uid = pCfg->indexUid;
  me.name = pCfg->indexName;
  // me.smaEntry = xx;

  if (metaHandleSmaEntry(pMeta, &me) < 0) goto _err;

  metaDebug("vgId:%d tsma is created, name:%s uid: %" PRId64, TD_VID(pMeta->pVnode), pCfg->indexName, pCfg->indexUid);

  return 0;

_err:
  metaError("vgId:%d failed to create tsma: %s uid: %" PRId64 " since %s", TD_VID(pMeta->pVnode), pCfg->indexName,
            pCfg->indexUid, tstrerror(terrno));
  return -1;
}

int32_t metaDropTSma(SMeta *pMeta, int64_t indexUid) {
  // TODO: Validate the cfg
  // TODO: add atomicity

#ifdef META_REFACT
#else
  if (metaRemoveSmaFromDb(pMeta, indexUid) < 0) {
    // TODO: handle error
    return -1;
  }
#endif
  return TSDB_CODE_SUCCESS;
}

// static int metaSaveSmaToDB(SMeta *pMeta, STSma *pSmaCfg) {
//   int32_t ret = 0;
//   void   *pBuf = NULL, *qBuf = NULL;
//   void   *key = {0}, *val = {0};

//   // save sma info
//   int32_t len = tEncodeTSma(NULL, pSmaCfg);
//   pBuf = taosMemoryCalloc(1, len);
//   if (pBuf == NULL) {
//     terrno = TSDB_CODE_OUT_OF_MEMORY;
//     return -1;
//   }

//   key = (void *)&pSmaCfg->indexUid;
//   qBuf = pBuf;
//   tEncodeTSma(&qBuf, pSmaCfg);
//   val = pBuf;

//   int32_t kLen = sizeof(pSmaCfg->indexUid);
//   int32_t vLen = POINTER_DISTANCE(qBuf, pBuf);

//   ret = tdbDbInsert(pMeta->pTbDb, key, kLen, val, vLen, &pMeta->txn);
//   if (ret < 0) {
//     taosMemoryFreeClear(pBuf);
//     return -1;
//   }

//   // add sma idx
//   SSmaIdxKey smaIdxKey;
//   smaIdxKey.uid = pSmaCfg->tableUid;
//   smaIdxKey.smaUid = pSmaCfg->indexUid;
//   key = &smaIdxKey;
//   kLen = sizeof(smaIdxKey);
//   val = NULL;
//   vLen = 0;

//   ret = tdbDbInsert(pMeta->pSmaIdx, key, kLen, val, vLen, &pMeta->txn);
//   if (ret < 0) {
//     taosMemoryFreeClear(pBuf);
//     return -1;
//   }

//   // release
//   taosMemoryFreeClear(pBuf);

//   return 0;
// }


static int metaSaveSmaToDB(SMeta *pMeta, const SMetaEntry *pME) {
  STbDbKey tbDbKey;
  void    *pKey = NULL;
  void    *pVal = NULL;
  int      kLen = 0;
  int      vLen = 0;
  SEncoder coder = {0};

  // set key and value
  tbDbKey.version = pME->version;
  tbDbKey.uid = pME->uid;

  pKey = &tbDbKey;
  kLen = sizeof(tbDbKey);

  int32_t ret = 0;
  tEncodeSize(metaEncodeEntry, pME, vLen, ret);
  if (ret < 0) {
    goto _err;
  }

  pVal = taosMemoryMalloc(vLen);
  if (pVal == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  tEncoderInit(&coder, pVal, vLen);

  if (metaEncodeEntry(&coder, pME) < 0) {
    goto _err;
  }

  tEncoderClear(&coder);

  // write to table.db
  if (tdbDbInsert(pMeta->pTbDb, pKey, kLen, pVal, vLen, &pMeta->txn) < 0) {
    goto _err;
  }

  taosMemoryFree(pVal);
  return 0;

_err:
  taosMemoryFree(pVal);
  return -1;
}




static int metaHandleSmaEntry(SMeta *pMeta, const SMetaEntry *pME) {
  metaWLock(pMeta);

  // save to table.db
  if (metaSaveSmaToDB(pMeta, pME) < 0) goto _err;

  // // update uid.idx
  // if (metaUpdateUidIdx(pMeta, pME) < 0) goto _err;

  metaULock(pMeta);
  return 0;

_err:
  metaULock(pMeta);
  return -1;
}
