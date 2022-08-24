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

typedef struct {
  STsdb  *pTsdb;
  int8_t  maxLast;
  int64_t commitID;
  STsdbFS fs;
  struct {
    SDataFReader *pReader;
    SArray       *aBlockIdx;
    SArray       *aBlockL[TSDB_MAX_LAST_FILE];
  } dReader;
  struct {
    SDataFWriter *pWriter;
    SArray       *aBlockIdx;
    SArray       *aBlockL;
  } dWriter;
} STsdbMerger;

static int32_t tsdbMergeFileDataStart(STsdbMerger *pMerger, SDFileSet *pSet) {
  int32_t code = 0;
  STsdb  *pTsdb = pMerger->pTsdb;

  // reader
  code = tsdbDataFReaderOpen(&pMerger->dReader.pReader, pTsdb, pSet);
  if (code) goto _err;

  code = tsdbReadBlockIdx(pMerger->dReader.pReader, pMerger->dReader.aBlockIdx);
  if (code) goto _err;

  for (int8_t iLast = 0; iLast < pSet->nLastF; iLast++) {
    code = tsdbReadBlockL(pMerger->dReader.pReader, iLast, pMerger->dReader.aBlockL[iLast]);
    if (code) goto _err;
  }

  // writer
  SHeadFile fHead = {.commitID = pMerger->commitID};
  SDataFile fData = *pSet->pDataF;
  SSmaFile  fSma = *pSet->pSmaF;
  SLastFile fLast = {.commitID = pMerger->commitID};
  SDFileSet wSet = {.diskId = pSet->diskId,
                    .fid = pSet->fid,
                    .nLastF = 1,
                    .pHeadF = &fHead,
                    .pDataF = &fData,
                    .pSmaF = &fSma,
                    .aLastF[0] = &fLast};
  code = tsdbDataFWriterOpen(&pMerger->dWriter.pWriter, pTsdb, &wSet);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d tsdb merge file data start failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbMergeFileDataEnd(STsdbMerger *pMerger) {
  int32_t code = 0;
  STsdb  *pTsdb = pMerger->pTsdb;

  // write aBlockIdx
  code = tsdbWriteBlockIdx(pMerger->dWriter.pWriter, pMerger->dWriter.aBlockIdx);
  if (code) goto _err;

  // write aBlockL
  code = tsdbWriteBlockL(pMerger->dWriter.pWriter, pMerger->dWriter.aBlockL);
  if (code) goto _err;

  // update file header
  code = tsdbUpdateDFileSetHeader(pMerger->dWriter.pWriter);
  if (code) goto _err;

  // upsert SDFileSet
  code = tsdbFSUpsertFSet(&pMerger->fs, &pMerger->dWriter.pWriter->wSet);
  if (code) goto _err;

  // close and sync
  code = tsdbDataFWriterClose(&pMerger->dWriter.pWriter, 1);
  if (code) goto _err;

  if (pMerger->dReader.pReader) {
    code = tsdbDataFReaderClose(&pMerger->dReader.pReader);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb merge file data end failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbMergeFileData(STsdbMerger *pMerger, SDFileSet *pSet) {
  int32_t code = 0;
  STsdb  *pTsdb = pMerger->pTsdb;

  // start
  code = tsdbMergeFileDataStart(pMerger, pSet);
  if (code) goto _err;

  // impl
  while (true) {
    if (1) break;
    // TODO
  }

  // end
  code = tsdbMergeFileDataEnd(pMerger);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d tsdb merge file data failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbStartMerge(STsdbMerger *pMerger, STsdb *pTsdb) {
  int32_t code = 0;

  pMerger->pTsdb = pTsdb;
  pMerger->maxLast = TSDB_DEFAULT_LAST_FILE;
  pMerger->commitID = ++pTsdb->pVnode->state.commitID;
  code = tsdbFSCopy(pTsdb, &pMerger->fs);
  if (code) goto _exit;

  // reader
  pMerger->dReader.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pMerger->dReader.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  for (int8_t iLast = 0; iLast < TSDB_MAX_LAST_FILE; iLast++) {
    pMerger->dReader.aBlockL[iLast] = taosArrayInit(0, sizeof(SBlockL));
    if (pMerger->dReader.aBlockL[iLast] == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

  // writer
  pMerger->dWriter.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pMerger->dWriter.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  pMerger->dWriter.aBlockL = taosArrayInit(0, sizeof(SBlockL));
  if (pMerger->dWriter.aBlockL == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:
  return code;
}

static int32_t tsdbEndMerge(STsdbMerger *pMerger) {
  int32_t code = 0;
  STsdb  *pTsdb = pMerger->pTsdb;

  code = tsdbFSCommit1(pTsdb, &pMerger->fs);
  if (code) goto _err;

  taosThreadRwlockWrlock(&pTsdb->rwLock);
  code = tsdbFSCommit2(pTsdb, &pMerger->fs);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    goto _err;
  }
  taosThreadRwlockUnlock(&pTsdb->rwLock);

  // writer
  taosArrayDestroy(pMerger->dWriter.aBlockL);
  taosArrayDestroy(pMerger->dWriter.aBlockIdx);

  // reader
  for (int8_t iLast = 0; iLast < TSDB_MAX_LAST_FILE; iLast++) {
    taosArrayDestroy(pMerger->dReader.aBlockL[iLast]);
  }
  taosArrayDestroy(pMerger->dReader.aBlockIdx);
  tsdbFSDestroy(&pMerger->fs);

  return code;

_err:
  tsdbError("vgId:%d, tsdb end merge failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbMerge(STsdb *pTsdb) {
  int32_t     code = 0;
  STsdbMerger merger = {0};

  code = tsdbStartMerge(&merger, pTsdb);
  if (code) goto _err;

  for (int32_t iSet = 0; iSet < taosArrayGetSize(merger.fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(merger.fs.aDFileSet, iSet);
    if (pSet->nLastF < merger.maxLast) continue;

    code = tsdbMergeFileData(&merger, pSet);
    if (code) goto _err;
  }

  code = tsdbEndMerge(&merger);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d tsdb merge failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}