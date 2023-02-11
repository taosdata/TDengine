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

#include "tsdb.h"

#if 0
typedef struct {
  STsdb  *pTsdb;
  STsdbFS fs;
} STsdbMerger;

static int32_t tsdbStartMerge(STsdb *pTsdb, STsdbMerger *pMerger, SMergeInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  memset(pMerger, 0, sizeof(*pMerger));

  pMerger->pTsdb = pTsdb;
  pMerger->commitID = pInfo->info.state.commitID; 
  pCommitter->minutes = pTsdb->keepCfg.days;
  pCommitter->precision = pTsdb->keepCfg.precision;
  pCommitter->minRow = pInfo->info.config.tsdbCfg.minRows;
  pCommitter->maxRow = pInfo->info.config.tsdbCfg.maxRows;
  pCommitter->cmprAlg = pInfo->info.config.tsdbCfg.compression;
  pCommitter->sttTrigger = pInfo->info.config.sttTrigger;
  pCommitter->aTbDataP = tsdbMemTableGetTbDataArray(pTsdb->imem);
  if (pCommitter->aTbDataP == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  code = tsdbFSCopy(pTsdb, &pCommitter->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitDataStart(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  // reader
  pCommitter->dReader.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pCommitter->dReader.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataCreate(&pCommitter->dReader.bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  // merger
  for (int32_t iStt = 0; iStt < TSDB_MAX_STT_TRIGGER; iStt++) {
    SDataIter *pIter = &pCommitter->aDataIter[iStt];
    pIter->aSttBlk = taosArrayInit(0, sizeof(SSttBlk));
    if (pIter->aSttBlk == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tBlockDataCreate(&pIter->bData);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // writer
  pCommitter->dWriter.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pCommitter->dWriter.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pCommitter->dWriter.aSttBlk = taosArrayInit(0, sizeof(SSttBlk));
  if (pCommitter->dWriter.aSttBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataCreate(&pCommitter->dWriter.bData);
  TSDB_CHECK_CODE(code, lino, _exit);

#if USE_STREAM_COMPRESSION
  code = tDiskDataBuilderCreate(&pCommitter->dWriter.pBuilder);
#else
  code = tBlockDataCreate(&pCommitter->dWriter.bDatal);
#endif
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitData(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  // check
  if (pMemTable->nRow == 0) goto _exit;

  // start ====================
  code = tsdbCommitDataStart(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // impl ====================
  pCommitter->nextKey = pMemTable->minKey;
  while (pCommitter->nextKey < TSKEY_MAX) {
    code = tsdbCommitFileData(pCommitter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // end ====================
  tsdbCommitDataEnd(pCommitter);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

#endif

#if 1


int32_t tsdbMerge(STsdb *pTsdb, void* arg, int64_t varg) {
  int32_t code = 0;


  // // start commit
  // code = tsdbStartCommit(pTsdb, &commith, pInfo);
  // TSDB_CHECK_CODE(code, lino, _exit);

  // // commit impl
  // code = tsdbCommitData(&commith);
  // TSDB_CHECK_CODE(code, lino, _exit);


  return code;
}

#endif