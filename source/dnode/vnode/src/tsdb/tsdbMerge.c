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
  int64_t suid;
  int64_t uid;
  TSDBROW row;
} SRowInfo;

typedef struct {
  SRowInfo      rowInfo;
  SDataFReader *pReader;
  int32_t       iLast;
  SArray       *aBlockL;  // SArray<SBlockL>
  int32_t       iBlockL;
  SBlockData    bData;
  int32_t       iRow;
} SLDataIter;

typedef struct {
  SRBTreeNode *pNode;
  SRBTree      rbt;
} SDataMerger;

static int32_t tRowInfoCmprFn(const void *p1, const void *p2) {
  SRowInfo *pInfo1 = (SRowInfo *)p1;
  SRowInfo *pInfo2 = (SRowInfo *)p2;

  if (pInfo1->suid < pInfo2->suid) {
    return -1;
  } else if (pInfo1->suid > pInfo2->suid) {
    return 1;
  }

  if (pInfo1->uid < pInfo2->uid) {
    return -1;
  } else if (pInfo1->uid > pInfo2->uid) {
    return 1;
  }

  return tsdbRowCmprFn(&pInfo1->row, &pInfo2->row);
}

static void tDataMergerInit(SDataMerger *pMerger, SArray *aNodeP) {
  pMerger->pNode = NULL;
  tRBTreeCreate(&pMerger->rbt, tRowInfoCmprFn);
  for (int32_t iNode = 0; iNode < taosArrayGetSize(aNodeP); iNode++) {
    SRBTreeNode *pNode = (SRBTreeNode *)taosArrayGetP(aNodeP, iNode);

    pNode = tRBTreePut(&pMerger->rbt, pNode);
    ASSERT(pNode);
  }
}

extern int32_t tsdbReadLastBlockEx(SDataFReader *pReader, int32_t iLast, SBlockL *pBlockL,
                                   SBlockData *pBlockData);  // todo

static int32_t tDataMergeNext(SDataMerger *pMerger, SRowInfo **ppInfo) {
  int32_t code = 0;

  if (pMerger->pNode) {
    // next current iter
    SLDataIter *pIter = (SLDataIter *)pMerger->pNode->payload;

    pIter->iRow++;
    if (pIter->iRow < pIter->bData.nRow) {
      pIter->rowInfo.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[pIter->iRow];
      pIter->rowInfo.row = tsdbRowFromBlockData(&pIter->bData, pIter->iRow);
    } else {
      pIter->iBlockL++;
      if (pIter->iBlockL < taosArrayGetSize(pIter->aBlockL)) {
        SBlockL *pBlockL = (SBlockL *)taosArrayGet(pIter->aBlockL, pIter->iBlockL);
        code = tsdbReadLastBlockEx(pIter->pReader, pIter->iLast, pBlockL, &pIter->bData);
        if (code) goto _exit;

        pIter->iRow = 0;
        pIter->rowInfo.suid = pIter->bData.suid;
        pIter->rowInfo.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[0];
        pIter->rowInfo.row = tsdbRowFromBlockData(&pIter->bData, 0);
      } else {
        pMerger->pNode = NULL;
      }
    }

    SRBTreeNode *pMinNode = tRBTreeMin(&pMerger->rbt);
    if (pMerger->pNode && pMinNode) {
      int32_t c = tRowInfoCmprFn(pMerger->pNode->payload, pMinNode->payload);
      if (c > 0) {
        pMerger->pNode = tRBTreePut(&pMerger->rbt, pMerger->pNode);
        ASSERT(pMerger->pNode);
        pMerger->pNode = NULL;
      } else {
        ASSERT(c);
      }
    }
  }

  if (pMerger->pNode == NULL) {
    pMerger->pNode = tRBTreeMin(&pMerger->rbt);
    if (pMerger->pNode) {
      tRBTreeDrop(&pMerger->rbt, pMerger->pNode);
    }
  }

  if (pMerger->pNode) {
    *ppInfo = &((SLDataIter *)pMerger->pNode->payload)[0].rowInfo;
  } else {
    *ppInfo = NULL;
  }

_exit:
  return code;
}

// ================================================================================
typedef struct {
  STsdb  *pTsdb;
  int8_t  maxLast;
  int64_t commitID;
  STsdbFS fs;
  struct {
    SDataFReader *pReader;
    SArray       *aBlockIdx;
    SLDataIter   *aLDataiter[TSDB_MAX_LAST_FILE];
    SDataMerger   merger;
  } dReader;
  struct {
    SDataFWriter *pWriter;
    SArray       *aBlockIdx;
    SArray       *aBlockL;
    SBlockData    bData;
    SBlockData    bDatal;
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

  pMerger->dReader.merger.pNode = NULL;
  tRBTreeCreate(&pMerger->dReader.merger.rbt, tRowInfoCmprFn);
  for (int8_t iLast = 0; iLast < pSet->nLastF; iLast++) {
    SRBTreeNode *pNode = (SRBTreeNode *)taosMemoryCalloc(1, sizeof(*pNode) + sizeof(SLDataIter));
    if (pNode == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }

    SLDataIter *pIter = (SLDataIter *)pNode->payload;
    pIter->pReader = pMerger->dReader.pReader;
    pIter->iLast = iLast;

    pIter->aBlockL = taosArrayInit(0, sizeof(SBlockL));
    if (pIter->aBlockL == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    code = tBlockDataCreate(&pIter->bData);
    if (code) goto _err;

    code = tsdbReadBlockL(pMerger->dReader.pReader, iLast, pIter->aBlockL);
    if (code) goto _err;

    if (taosArrayGetSize(pIter->aBlockL) == 0) continue;
    pIter->iBlockL = 0;

    SBlockL *pBlockL = (SBlockL *)taosArrayGet(pIter->aBlockL, 0);
    code = tsdbReadLastBlockEx(pMerger->dReader.pReader, iLast, pBlockL, &pIter->bData);
    if (code) goto _err;

    pIter->iRow = 0;
    pIter->rowInfo.suid = pIter->bData.suid;
    pIter->rowInfo.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[0];
    pIter->rowInfo.row = tsdbRowFromBlockData(&pIter->bData, 0);

    pNode = tRBTreePut(&pMerger->dReader.merger.rbt, pNode);
    ASSERT(pNode);

    pMerger->dReader.aLDataiter[iLast] = pIter;
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

typedef struct {
  int64_t suid;
  int64_t uid;
  TSKEY   ts;
  int64_t version;
} SRInfo;

static int32_t tRInfoCmprFn(const void *p1, const void *p2) {
  SRInfo *pInfo1 = (SRInfo *)p1;
  SRInfo *pInfo2 = (SRInfo *)p2;

  // suid
  if (pInfo1->suid < pInfo2->suid) {
    return -1;
  } else if (pInfo1->suid > pInfo2->suid) {
    return 1;
  }

  // uid
  if (pInfo1->uid < pInfo2->uid) {
    return -1;
  } else if (pInfo1->uid > pInfo2->uid) {
    return 1;
  }

  // ts
  if (pInfo1->ts < pInfo2->ts) {
    return -1;
  } else if (pInfo1->ts > pInfo2->ts) {
    return 1;
  }

  // version
  if (pInfo1->version < pInfo2->version) {
    return -1;
  } else if (pInfo1->version > pInfo2->version) {
    return 1;
  }

  return 0;
}

static int32_t tsdbMergeFileData(STsdbMerger *pMerger, SDFileSet *pSet) {
  int32_t code = 0;
  STsdb  *pTsdb = pMerger->pTsdb;

  // start
  code = tsdbMergeFileDataStart(pMerger, pSet);
  if (code) goto _err;

  // impl
  SRowInfo *pInfo;
  int64_t   nRow = 0;
  SRInfo    rInfo = {.suid = INT64_MIN};
  while (true) {
    code = tDataMergeNext(&pMerger->dReader.merger, &pInfo);
    if (code) goto _err;

    if (pInfo == NULL) break;
    nRow++;

    SRInfo rInfoT = {
        .suid = pInfo->suid, .uid = pInfo->uid, .ts = TSDBROW_TS(&pInfo->row), .version = TSDBROW_VERSION(&pInfo->row)};
    ASSERT(tRInfoCmprFn(&rInfoT, &rInfo) > 0);
    rInfo = rInfoT;
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
  // for (int8_t iLast = 0; iLast < TSDB_MAX_LAST_FILE; iLast++) {
  //   pMerger->dReader.aBlockL[iLast] = taosArrayInit(0, sizeof(SBlockL));
  //   if (pMerger->dReader.aBlockL[iLast] == NULL) {
  //     code = TSDB_CODE_OUT_OF_MEMORY;
  //     goto _exit;
  //   }
  // }

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
  // for (int8_t iLast = 0; iLast < TSDB_MAX_LAST_FILE; iLast++) {
  //   taosArrayDestroy(pMerger->dReader.aBlockL[iLast]);
  // }
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