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

#include "vnodeInt.h"
#include "meta.h"


static int metaHandleSmaEntry(SMeta *pMeta, const SMetaEntry *pME);
static int metaSaveSmaToDB(SMeta *pMeta, const SMetaEntry *pME);

int32_t metaCreateTSma(SMeta *pMeta, int64_t version, SSmaCfg *pCfg) {
  // TODO: Validate the cfg
  // The table uid should exists and be super table or normal table.
  // Check other cfg value

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
  // save smaIndex
  metaReaderDoInit(&mr, pMeta, 0);
  if (metaReaderGetTableEntryByUidCache(&mr, pCfg->indexUid) == 0) {
#if 1
    terrno = TSDB_CODE_TSMA_ALREADY_EXIST;
    metaReaderClear(&mr);
    return -1;  // don't goto _err;
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
  me.smaEntry.tsma = pCfg;

  if (metaHandleSmaEntry(pMeta, &me) < 0) goto _err;

  metaDebug("vgId:%d, tsma is created, name:%s uid:%" PRId64, TD_VID(pMeta->pVnode), pCfg->indexName, pCfg->indexUid);

  return 0;

_err:
  metaError("vgId:%d, failed to create tsma:%s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), pCfg->indexName,
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
  if (tdbTbInsert(pMeta->pTbDb, pKey, kLen, pVal, vLen, pMeta->txn) < 0) {
    goto _err;
  }

  taosMemoryFree(pVal);
  return 0;

_err:
  taosMemoryFree(pVal);
  return -1;
}

static int metaUpdateUidIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SUidIdxVal uidIdxVal = {.suid = pME->smaEntry.tsma->indexUid, .version = pME->version, .skmVer = 0};
  return tdbTbInsert(pMeta->pUidIdx, &pME->uid, sizeof(tb_uid_t), &uidIdxVal, sizeof(uidIdxVal), pMeta->txn);
}

static int metaUpdateNameIdx(SMeta *pMeta, const SMetaEntry *pME) {
  return tdbTbInsert(pMeta->pNameIdx, pME->name, strlen(pME->name) + 1, &pME->uid, sizeof(tb_uid_t), pMeta->txn);
}

static int metaUpdateSmaIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SSmaIdxKey smaIdxKey = {.uid = pME->smaEntry.tsma->tableUid, .smaUid = pME->smaEntry.tsma->indexUid};

  return tdbTbInsert(pMeta->pSmaIdx, &smaIdxKey, sizeof(smaIdxKey), NULL, 0, pMeta->txn);
}

static int metaHandleSmaEntry(SMeta *pMeta, const SMetaEntry *pME) {
  metaWLock(pMeta);

  // save to table.db
  if (metaSaveSmaToDB(pMeta, pME) < 0) goto _err;

  // update uid.idx
  if (metaUpdateUidIdx(pMeta, pME) < 0) goto _err;

  // update name.idx
  if (metaUpdateNameIdx(pMeta, pME) < 0) goto _err;

  // update sma.idx
  if (metaUpdateSmaIdx(pMeta, pME) < 0) goto _err;

  metaULock(pMeta);
  return 0;

_err:
  metaULock(pMeta);
  return -1;
}
