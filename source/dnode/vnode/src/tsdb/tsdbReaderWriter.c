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

// =============== PAGE-WISE FILE ===============
static int32_t tsdbOpenFile(const char *path, int32_t szPage, int32_t flag, STsdbFD **ppFD) {
  int32_t  code = 0;
  STsdbFD *pFD = NULL;

  *ppFD = NULL;

  pFD = (STsdbFD *)taosMemoryCalloc(1, sizeof(*pFD) + strlen(path) + 1);
  if (pFD == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  pFD->path = (char *)&pFD[1];
  strcpy(pFD->path, path);
  pFD->szPage = szPage;
  pFD->flag = flag;
  pFD->pFD = taosOpenFile(path, flag);
  if (pFD->pFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    taosMemoryFree(pFD);
    goto _exit;
  }
  pFD->szPage = szPage;
  pFD->pgno = 0;
  pFD->pBuf = taosMemoryCalloc(1, szPage);
  if (pFD->pBuf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    taosCloseFile(&pFD->pFD);
    taosMemoryFree(pFD);
    goto _exit;
  }

  // not check file size when reading data files.
  if (flag != TD_FILE_READ) {
    if (taosStatFile(path, &pFD->szFile, NULL) < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      taosMemoryFree(pFD->pBuf);
      taosCloseFile(&pFD->pFD);
      taosMemoryFree(pFD);
      goto _exit;
    }

    ASSERT(pFD->szFile % szPage == 0);
    pFD->szFile = pFD->szFile / szPage;
  }

  *ppFD = pFD;

_exit:
  return code;
}

static void tsdbCloseFile(STsdbFD **ppFD) {
  STsdbFD *pFD = *ppFD;
  if (pFD) {
    taosMemoryFree(pFD->pBuf);
    taosCloseFile(&pFD->pFD);
    taosMemoryFree(pFD);
    *ppFD = NULL;
  }
}

static int32_t tsdbWriteFilePage(STsdbFD *pFD) {
  int32_t code = 0;

  if (pFD->pgno > 0) {
    int64_t n = taosLSeekFile(pFD->pFD, PAGE_OFFSET(pFD->pgno, pFD->szPage), SEEK_SET);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _exit;
    }

    taosCalcChecksumAppend(0, pFD->pBuf, pFD->szPage);

    n = taosWriteFile(pFD->pFD, pFD->pBuf, pFD->szPage);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _exit;
    }

    if (pFD->szFile < pFD->pgno) {
      pFD->szFile = pFD->pgno;
    }
  }
  pFD->pgno = 0;

_exit:
  return code;
}

static int32_t tsdbReadFilePage(STsdbFD *pFD, int64_t pgno) {
  int32_t code = 0;

  // ASSERT(pgno <= pFD->szFile);

  // seek
  int64_t offset = PAGE_OFFSET(pgno, pFD->szPage);
  int64_t n = taosLSeekFile(pFD->pFD, offset, SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }

  // read
  n = taosReadFile(pFD->pFD, pFD->pBuf, pFD->szPage);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  } else if (n < pFD->szPage) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }

  // check
  if (pgno > 1 && !taosCheckChecksumWhole(pFD->pBuf, pFD->szPage)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }

  pFD->pgno = pgno;

_exit:
  return code;
}

static int32_t tsdbWriteFile(STsdbFD *pFD, int64_t offset, const uint8_t *pBuf, int64_t size) {
  int32_t code = 0;
  int64_t fOffset = LOGIC_TO_FILE_OFFSET(offset, pFD->szPage);
  int64_t pgno = OFFSET_PGNO(fOffset, pFD->szPage);
  int64_t bOffset = fOffset % pFD->szPage;
  int64_t n = 0;

  do {
    if (pFD->pgno != pgno) {
      code = tsdbWriteFilePage(pFD);
      if (code) goto _exit;

      if (pgno <= pFD->szFile) {
        code = tsdbReadFilePage(pFD, pgno);
        if (code) goto _exit;
      } else {
        pFD->pgno = pgno;
      }
    }

    int64_t nWrite = TMIN(PAGE_CONTENT_SIZE(pFD->szPage) - bOffset, size - n);
    memcpy(pFD->pBuf + bOffset, pBuf + n, nWrite);

    pgno++;
    bOffset = 0;
    n += nWrite;
  } while (n < size);

_exit:
  return code;
}

static int32_t tsdbReadFile(STsdbFD *pFD, int64_t offset, uint8_t *pBuf, int64_t size) {
  int32_t code = 0;
  int64_t n = 0;
  int64_t fOffset = LOGIC_TO_FILE_OFFSET(offset, pFD->szPage);
  int64_t pgno = OFFSET_PGNO(fOffset, pFD->szPage);
  int32_t szPgCont = PAGE_CONTENT_SIZE(pFD->szPage);
  int64_t bOffset = fOffset % pFD->szPage;

  // ASSERT(pgno && pgno <= pFD->szFile);
  ASSERT(bOffset < szPgCont);

  while (n < size) {
    if (pFD->pgno != pgno) {
      code = tsdbReadFilePage(pFD, pgno);
      if (code) goto _exit;
    }

    int64_t nRead = TMIN(szPgCont - bOffset, size - n);
    memcpy(pBuf + n, pFD->pBuf + bOffset, nRead);

    n += nRead;
    pgno++;
    bOffset = 0;
  }

_exit:
  return code;
}

static int32_t tsdbFsyncFile(STsdbFD *pFD) {
  int32_t code = 0;

  code = tsdbWriteFilePage(pFD);
  if (code) goto _exit;

  if (taosFsyncFile(pFD->pFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }

_exit:
  return code;
}

// SDataFWriter ====================================================
int32_t tsdbDataFWriterOpen(SDataFWriter **ppWriter, STsdb *pTsdb, SDFileSet *pSet) {
  int32_t       code = 0;
  int32_t       flag;
  int64_t       n;
  int32_t       szPage = pTsdb->pVnode->config.tsdbPageSize;
  SDataFWriter *pWriter = NULL;
  char          fname[TSDB_FILENAME_LEN];
  char          hdr[TSDB_FHDR_SIZE] = {0};

  // alloc
  pWriter = taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pTsdb = pTsdb;
  pWriter->wSet = (SDFileSet){.diskId = pSet->diskId,
                              .fid = pSet->fid,
                              .pHeadF = &pWriter->fHead,
                              .pDataF = &pWriter->fData,
                              .pSmaF = &pWriter->fSma,
                              .nSttF = pSet->nSttF};
  pWriter->fHead = *pSet->pHeadF;
  pWriter->fData = *pSet->pDataF;
  pWriter->fSma = *pSet->pSmaF;
  for (int8_t iStt = 0; iStt < pSet->nSttF; iStt++) {
    pWriter->wSet.aSttF[iStt] = &pWriter->fStt[iStt];
    pWriter->fStt[iStt] = *pSet->aSttF[iStt];
  }

  // head
  flag = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  tsdbHeadFileName(pTsdb, pWriter->wSet.diskId, pWriter->wSet.fid, &pWriter->fHead, fname);
  code = tsdbOpenFile(fname, szPage, flag, &pWriter->pHeadFD);
  if (code) goto _err;

  code = tsdbWriteFile(pWriter->pHeadFD, 0, hdr, TSDB_FHDR_SIZE);
  if (code) goto _err;
  pWriter->fHead.size += TSDB_FHDR_SIZE;

  // data
  if (pWriter->fData.size == 0) {
    flag = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  } else {
    flag = TD_FILE_READ | TD_FILE_WRITE;
  }
  tsdbDataFileName(pTsdb, pWriter->wSet.diskId, pWriter->wSet.fid, &pWriter->fData, fname);
  code = tsdbOpenFile(fname, szPage, flag, &pWriter->pDataFD);
  if (code) goto _err;
  if (pWriter->fData.size == 0) {
    code = tsdbWriteFile(pWriter->pDataFD, 0, hdr, TSDB_FHDR_SIZE);
    if (code) goto _err;
    pWriter->fData.size += TSDB_FHDR_SIZE;
  }

  // sma
  if (pWriter->fSma.size == 0) {
    flag = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  } else {
    flag = TD_FILE_READ | TD_FILE_WRITE;
  }
  tsdbSmaFileName(pTsdb, pWriter->wSet.diskId, pWriter->wSet.fid, &pWriter->fSma, fname);
  code = tsdbOpenFile(fname, szPage, flag, &pWriter->pSmaFD);
  if (code) goto _err;
  if (pWriter->fSma.size == 0) {
    code = tsdbWriteFile(pWriter->pSmaFD, 0, hdr, TSDB_FHDR_SIZE);
    if (code) goto _err;

    pWriter->fSma.size += TSDB_FHDR_SIZE;
  }

  // stt
  ASSERT(pWriter->fStt[pSet->nSttF - 1].size == 0);
  flag = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  tsdbSttFileName(pTsdb, pWriter->wSet.diskId, pWriter->wSet.fid, &pWriter->fStt[pSet->nSttF - 1], fname);
  code = tsdbOpenFile(fname, szPage, flag, &pWriter->pSttFD);
  if (code) goto _err;
  code = tsdbWriteFile(pWriter->pSttFD, 0, hdr, TSDB_FHDR_SIZE);
  if (code) goto _err;
  pWriter->fStt[pWriter->wSet.nSttF - 1].size += TSDB_FHDR_SIZE;

  *ppWriter = pWriter;
  return code;

_err:
  tsdbError("vgId:%d, tsdb data file writer open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

int32_t tsdbDataFWriterClose(SDataFWriter **ppWriter, int8_t sync) {
  int32_t code = 0;
  STsdb  *pTsdb = NULL;

  if (*ppWriter == NULL) goto _exit;

  pTsdb = (*ppWriter)->pTsdb;
  if (sync) {
    code = tsdbFsyncFile((*ppWriter)->pHeadFD);
    if (code) goto _err;

    code = tsdbFsyncFile((*ppWriter)->pDataFD);
    if (code) goto _err;

    code = tsdbFsyncFile((*ppWriter)->pSmaFD);
    if (code) goto _err;

    code = tsdbFsyncFile((*ppWriter)->pSttFD);
    if (code) goto _err;
  }

  tsdbCloseFile(&(*ppWriter)->pHeadFD);
  tsdbCloseFile(&(*ppWriter)->pDataFD);
  tsdbCloseFile(&(*ppWriter)->pSmaFD);
  tsdbCloseFile(&(*ppWriter)->pSttFD);

  for (int32_t iBuf = 0; iBuf < sizeof((*ppWriter)->aBuf) / sizeof(uint8_t *); iBuf++) {
    tFree((*ppWriter)->aBuf[iBuf]);
  }
  taosMemoryFree(*ppWriter);
_exit:
  *ppWriter = NULL;
  return code;

_err:
  tsdbError("vgId:%d, data file writer close failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbUpdateDFileSetHeader(SDataFWriter *pWriter) {
  int32_t code = 0;
  int64_t n;
  char    hdr[TSDB_FHDR_SIZE];

  // head ==============
  memset(hdr, 0, TSDB_FHDR_SIZE);
  tPutHeadFile(hdr, &pWriter->fHead);
  code = tsdbWriteFile(pWriter->pHeadFD, 0, hdr, TSDB_FHDR_SIZE);
  if (code) goto _err;

  // data ==============
  memset(hdr, 0, TSDB_FHDR_SIZE);
  tPutDataFile(hdr, &pWriter->fData);
  code = tsdbWriteFile(pWriter->pDataFD, 0, hdr, TSDB_FHDR_SIZE);
  if (code) goto _err;

  // sma ==============
  memset(hdr, 0, TSDB_FHDR_SIZE);
  tPutSmaFile(hdr, &pWriter->fSma);
  code = tsdbWriteFile(pWriter->pSmaFD, 0, hdr, TSDB_FHDR_SIZE);
  if (code) goto _err;

  // stt ==============
  memset(hdr, 0, TSDB_FHDR_SIZE);
  tPutSttFile(hdr, &pWriter->fStt[pWriter->wSet.nSttF - 1]);
  code = tsdbWriteFile(pWriter->pSttFD, 0, hdr, TSDB_FHDR_SIZE);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d, update DFileSet header failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteBlockIdx(SDataFWriter *pWriter, SArray *aBlockIdx) {
  int32_t    code = 0;
  SHeadFile *pHeadFile = &pWriter->fHead;
  int64_t    size;
  int64_t    n;

  // check
  if (taosArrayGetSize(aBlockIdx) == 0) {
    pHeadFile->offset = pHeadFile->size;
    goto _exit;
  }

  // prepare
  size = 0;
  for (int32_t iBlockIdx = 0; iBlockIdx < taosArrayGetSize(aBlockIdx); iBlockIdx++) {
    size += tPutBlockIdx(NULL, taosArrayGet(aBlockIdx, iBlockIdx));
  }

  // alloc
  code = tRealloc(&pWriter->aBuf[0], size);
  if (code) goto _err;

  // build
  n = 0;
  for (int32_t iBlockIdx = 0; iBlockIdx < taosArrayGetSize(aBlockIdx); iBlockIdx++) {
    n += tPutBlockIdx(pWriter->aBuf[0] + n, taosArrayGet(aBlockIdx, iBlockIdx));
  }
  ASSERT(n == size);

  // write
  code = tsdbWriteFile(pWriter->pHeadFD, pHeadFile->size, pWriter->aBuf[0], size);
  if (code) goto _err;

  // update
  pHeadFile->offset = pHeadFile->size;
  pHeadFile->size += size;

_exit:
  // tsdbTrace("vgId:%d, write block idx, offset:%" PRId64 " size:%" PRId64 " nBlockIdx:%d",
  // TD_VID(pWriter->pTsdb->pVnode),
  //           pHeadFile->offset, size, taosArrayGetSize(aBlockIdx));
  return code;

_err:
  tsdbError("vgId:%d, write block idx failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteDataBlk(SDataFWriter *pWriter, SMapData *mDataBlk, SBlockIdx *pBlockIdx) {
  int32_t    code = 0;
  SHeadFile *pHeadFile = &pWriter->fHead;
  int64_t    size;
  int64_t    n;

  ASSERT(mDataBlk->nItem > 0);

  // alloc
  size = tPutMapData(NULL, mDataBlk);
  code = tRealloc(&pWriter->aBuf[0], size);
  if (code) goto _err;

  // build
  n = tPutMapData(pWriter->aBuf[0], mDataBlk);

  // write
  code = tsdbWriteFile(pWriter->pHeadFD, pHeadFile->size, pWriter->aBuf[0], size);
  if (code) goto _err;

  // update
  pBlockIdx->offset = pHeadFile->size;
  pBlockIdx->size = size;
  pHeadFile->size += size;

  tsdbTrace("vgId:%d, write block, file ID:%d commit ID:%" PRId64 " suid:%" PRId64 " uid:%" PRId64 " offset:%" PRId64
            " size:%" PRId64 " nItem:%d",
            TD_VID(pWriter->pTsdb->pVnode), pWriter->wSet.fid, pHeadFile->commitID, pBlockIdx->suid, pBlockIdx->uid,
            pBlockIdx->offset, pBlockIdx->size, mDataBlk->nItem);
  return code;

_err:
  tsdbError("vgId:%d, write block failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteSttBlk(SDataFWriter *pWriter, SArray *aSttBlk) {
  int32_t   code = 0;
  SSttFile *pSttFile = &pWriter->fStt[pWriter->wSet.nSttF - 1];
  int64_t   size = 0;
  int64_t   n;

  // check
  if (taosArrayGetSize(aSttBlk) == 0) {
    pSttFile->offset = pSttFile->size;
    goto _exit;
  }

  // size
  size = 0;
  for (int32_t iBlockL = 0; iBlockL < taosArrayGetSize(aSttBlk); iBlockL++) {
    size += tPutSttBlk(NULL, taosArrayGet(aSttBlk, iBlockL));
  }

  // alloc
  code = tRealloc(&pWriter->aBuf[0], size);
  if (code) goto _err;

  // encode
  n = 0;
  for (int32_t iBlockL = 0; iBlockL < taosArrayGetSize(aSttBlk); iBlockL++) {
    n += tPutSttBlk(pWriter->aBuf[0] + n, taosArrayGet(aSttBlk, iBlockL));
  }

  // write
  code = tsdbWriteFile(pWriter->pSttFD, pSttFile->size, pWriter->aBuf[0], size);
  if (code) goto _err;

  // update
  pSttFile->offset = pSttFile->size;
  pSttFile->size += size;

_exit:
  tsdbTrace("vgId:%d, tsdb write stt block, loffset:%" PRId64 " size:%" PRId64, TD_VID(pWriter->pTsdb->pVnode),
            pSttFile->offset, size);
  return code;

_err:
  tsdbError("vgId:%d, tsdb write blockl failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbWriteBlockSma(SDataFWriter *pWriter, SBlockData *pBlockData, SSmaInfo *pSmaInfo) {
  int32_t code = 0;

  pSmaInfo->offset = 0;
  pSmaInfo->size = 0;

  // encode
  for (int32_t iColData = 0; iColData < pBlockData->nColData; iColData++) {
    SColData *pColData = tBlockDataGetColDataByIdx(pBlockData, iColData);

    if ((!pColData->smaOn) || ((pColData->flag & HAS_VALUE) == 0)) continue;

    SColumnDataAgg sma = {.colId = pColData->cid};
    tColDataCalcSMA[pColData->type](pColData, &sma.sum, &sma.max, &sma.min, &sma.numOfNull);

    code = tRealloc(&pWriter->aBuf[0], pSmaInfo->size + tPutColumnDataAgg(NULL, &sma));
    if (code) goto _err;
    pSmaInfo->size += tPutColumnDataAgg(pWriter->aBuf[0] + pSmaInfo->size, &sma);
  }

  // write
  if (pSmaInfo->size) {
    code = tsdbWriteFile(pWriter->pSmaFD, pWriter->fSma.size, pWriter->aBuf[0], pSmaInfo->size);
    if (code) goto _err;

    pSmaInfo->offset = pWriter->fSma.size;
    pWriter->fSma.size += pSmaInfo->size;
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb write block sma failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteBlockData(SDataFWriter *pWriter, SBlockData *pBlockData, SBlockInfo *pBlkInfo, SSmaInfo *pSmaInfo,
                           int8_t cmprAlg, int8_t toLast) {
  int32_t code = 0;

  ASSERT(pBlockData->nRow > 0);

  if (toLast) {
    pBlkInfo->offset = pWriter->fStt[pWriter->wSet.nSttF - 1].size;
  } else {
    pBlkInfo->offset = pWriter->fData.size;
  }
  pBlkInfo->szBlock = 0;
  pBlkInfo->szKey = 0;

  int32_t aBufN[4] = {0};
  code = tCmprBlockData(pBlockData, cmprAlg, NULL, NULL, pWriter->aBuf, aBufN);
  if (code) goto _err;

  // write =================
  STsdbFD *pFD = toLast ? pWriter->pSttFD : pWriter->pDataFD;

  pBlkInfo->szKey = aBufN[3] + aBufN[2];
  pBlkInfo->szBlock = aBufN[0] + aBufN[1] + aBufN[2] + aBufN[3];

  int64_t offset = pBlkInfo->offset;
  code = tsdbWriteFile(pFD, offset, pWriter->aBuf[3], aBufN[3]);
  if (code) goto _err;
  offset += aBufN[3];

  code = tsdbWriteFile(pFD, offset, pWriter->aBuf[2], aBufN[2]);
  if (code) goto _err;
  offset += aBufN[2];

  if (aBufN[1]) {
    code = tsdbWriteFile(pFD, offset, pWriter->aBuf[1], aBufN[1]);
    if (code) goto _err;
    offset += aBufN[1];
  }

  if (aBufN[0]) {
    code = tsdbWriteFile(pFD, offset, pWriter->aBuf[0], aBufN[0]);
    if (code) goto _err;
  }

  // update info
  if (toLast) {
    pWriter->fStt[pWriter->wSet.nSttF - 1].size += pBlkInfo->szBlock;
  } else {
    pWriter->fData.size += pBlkInfo->szBlock;
  }

  // ================= SMA ====================
  if (pSmaInfo) {
    code = tsdbWriteBlockSma(pWriter, pBlockData, pSmaInfo);
    if (code) goto _err;
  }

_exit:
  tsdbTrace("vgId:%d, tsdb write block data, suid:%" PRId64 " uid:%" PRId64 " nRow:%d, offset:%" PRId64 " size:%d",
            TD_VID(pWriter->pTsdb->pVnode), pBlockData->suid, pBlockData->uid, pBlockData->nRow, pBlkInfo->offset,
            pBlkInfo->szBlock);
  return code;

_err:
  tsdbError("vgId:%d, tsdb write block data failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteDiskData(SDataFWriter *pWriter, const SDiskData *pDiskData, SBlockInfo *pBlkInfo, SSmaInfo *pSmaInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbFD *pFD = NULL;
  if (pSmaInfo) {
    pFD = pWriter->pDataFD;
    pBlkInfo->offset = pWriter->fData.size;
  } else {
    pFD = pWriter->pSttFD;
    pBlkInfo->offset = pWriter->fStt[pWriter->wSet.nSttF - 1].size;
  }
  pBlkInfo->szBlock = 0;
  pBlkInfo->szKey = 0;

  // hdr
  int32_t n = tPutDiskDataHdr(NULL, &pDiskData->hdr);
  code = tRealloc(&pWriter->aBuf[0], n);
  TSDB_CHECK_CODE(code, lino, _exit);

  tPutDiskDataHdr(pWriter->aBuf[0], &pDiskData->hdr);

  code = tsdbWriteFile(pFD, pBlkInfo->offset, pWriter->aBuf[0], n);
  TSDB_CHECK_CODE(code, lino, _exit);
  pBlkInfo->szKey += n;
  pBlkInfo->szBlock += n;

  // uid + ver + key
  if (pDiskData->pUid) {
    code = tsdbWriteFile(pFD, pBlkInfo->offset + pBlkInfo->szBlock, pDiskData->pUid, pDiskData->hdr.szUid);
    TSDB_CHECK_CODE(code, lino, _exit);
    pBlkInfo->szKey += pDiskData->hdr.szUid;
    pBlkInfo->szBlock += pDiskData->hdr.szUid;
  }

  code = tsdbWriteFile(pFD, pBlkInfo->offset + pBlkInfo->szBlock, pDiskData->pVer, pDiskData->hdr.szVer);
  TSDB_CHECK_CODE(code, lino, _exit);
  pBlkInfo->szKey += pDiskData->hdr.szVer;
  pBlkInfo->szBlock += pDiskData->hdr.szVer;

  code = tsdbWriteFile(pFD, pBlkInfo->offset + pBlkInfo->szBlock, pDiskData->pKey, pDiskData->hdr.szKey);
  TSDB_CHECK_CODE(code, lino, _exit);
  pBlkInfo->szKey += pDiskData->hdr.szKey;
  pBlkInfo->szBlock += pDiskData->hdr.szKey;

  // aBlockCol
  if (pDiskData->hdr.szBlkCol) {
    code = tRealloc(&pWriter->aBuf[0], pDiskData->hdr.szBlkCol);
    TSDB_CHECK_CODE(code, lino, _exit);

    n = 0;
    for (int32_t iDiskCol = 0; iDiskCol < taosArrayGetSize(pDiskData->aDiskCol); iDiskCol++) {
      SDiskCol *pDiskCol = (SDiskCol *)taosArrayGet(pDiskData->aDiskCol, iDiskCol);
      n += tPutBlockCol(pWriter->aBuf[0] + n, pDiskCol);
    }
    ASSERT(n == pDiskData->hdr.szBlkCol);

    code = tsdbWriteFile(pFD, pBlkInfo->offset + pBlkInfo->szBlock, pWriter->aBuf[0], pDiskData->hdr.szBlkCol);
    TSDB_CHECK_CODE(code, lino, _exit);

    pBlkInfo->szBlock += pDiskData->hdr.szBlkCol;
  }

  // aDiskCol
  for (int32_t iDiskCol = 0; iDiskCol < taosArrayGetSize(pDiskData->aDiskCol); iDiskCol++) {
    SDiskCol *pDiskCol = (SDiskCol *)taosArrayGet(pDiskData->aDiskCol, iDiskCol);

    if (pDiskCol->pBit) {
      code = tsdbWriteFile(pFD, pBlkInfo->offset + pBlkInfo->szBlock, pDiskCol->pBit, pDiskCol->bCol.szBitmap);
      TSDB_CHECK_CODE(code, lino, _exit);

      pBlkInfo->szBlock += pDiskCol->bCol.szBitmap;
    }

    if (pDiskCol->pOff) {
      code = tsdbWriteFile(pFD, pBlkInfo->offset + pBlkInfo->szBlock, pDiskCol->pOff, pDiskCol->bCol.szOffset);
      TSDB_CHECK_CODE(code, lino, _exit);

      pBlkInfo->szBlock += pDiskCol->bCol.szOffset;
    }

    if (pDiskCol->pVal) {
      code = tsdbWriteFile(pFD, pBlkInfo->offset + pBlkInfo->szBlock, pDiskCol->pVal, pDiskCol->bCol.szValue);
      TSDB_CHECK_CODE(code, lino, _exit);

      pBlkInfo->szBlock += pDiskCol->bCol.szValue;
    }
  }

  if (pSmaInfo) {
    pWriter->fData.size += pBlkInfo->szBlock;
  } else {
    pWriter->fStt[pWriter->wSet.nSttF - 1].size += pBlkInfo->szBlock;
    goto _exit;
  }

  pSmaInfo->offset = 0;
  pSmaInfo->size = 0;
  for (int32_t iDiskCol = 0; iDiskCol < taosArrayGetSize(pDiskData->aDiskCol); iDiskCol++) {
    SDiskCol *pDiskCol = (SDiskCol *)taosArrayGet(pDiskData->aDiskCol, iDiskCol);

    if (IS_VAR_DATA_TYPE(pDiskCol->bCol.type)) continue;
    if (pDiskCol->bCol.flag == HAS_NULL || pDiskCol->bCol.flag == (HAS_NULL | HAS_NONE)) continue;
    if (!pDiskCol->bCol.smaOn) continue;

    code = tRealloc(&pWriter->aBuf[0], pSmaInfo->size + tPutColumnDataAgg(NULL, &pDiskCol->agg));
    TSDB_CHECK_CODE(code, lino, _exit);
    pSmaInfo->size += tPutColumnDataAgg(pWriter->aBuf[0] + pSmaInfo->size, &pDiskCol->agg);
  }

  if (pSmaInfo->size) {
    pSmaInfo->offset = pWriter->fSma.size;

    code = tsdbWriteFile(pWriter->pSmaFD, pSmaInfo->offset, pWriter->aBuf[0], pSmaInfo->size);
    TSDB_CHECK_CODE(code, lino, _exit);

    pWriter->fSma.size += pSmaInfo->size;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbDFileSetCopy(STsdb *pTsdb, SDFileSet *pSetFrom, SDFileSet *pSetTo) {
  int32_t   code = 0;
  int64_t   n;
  int64_t   size;
  TdFilePtr pOutFD = NULL;
  TdFilePtr PInFD = NULL;
  int32_t   szPage = pTsdb->pVnode->config.szPage;
  char      fNameFrom[TSDB_FILENAME_LEN];
  char      fNameTo[TSDB_FILENAME_LEN];

  // head
  tsdbHeadFileName(pTsdb, pSetFrom->diskId, pSetFrom->fid, pSetFrom->pHeadF, fNameFrom);
  tsdbHeadFileName(pTsdb, pSetTo->diskId, pSetTo->fid, pSetTo->pHeadF, fNameTo);
  pOutFD = taosCreateFile(fNameTo, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (pOutFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  PInFD = taosOpenFile(fNameFrom, TD_FILE_READ);
  if (PInFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  n = taosFSendFile(pOutFD, PInFD, 0, tsdbLogicToFileSize(pSetFrom->pHeadF->size, szPage));
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  taosCloseFile(&pOutFD);
  taosCloseFile(&PInFD);

  // data
  tsdbDataFileName(pTsdb, pSetFrom->diskId, pSetFrom->fid, pSetFrom->pDataF, fNameFrom);
  tsdbDataFileName(pTsdb, pSetTo->diskId, pSetTo->fid, pSetTo->pDataF, fNameTo);
  pOutFD = taosCreateFile(fNameTo, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (pOutFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  PInFD = taosOpenFile(fNameFrom, TD_FILE_READ);
  if (PInFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  n = taosFSendFile(pOutFD, PInFD, 0, tsdbLogicToFileSize(pSetFrom->pDataF->size, szPage));
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  taosCloseFile(&pOutFD);
  taosCloseFile(&PInFD);

  // sma
  tsdbSmaFileName(pTsdb, pSetFrom->diskId, pSetFrom->fid, pSetFrom->pSmaF, fNameFrom);
  tsdbSmaFileName(pTsdb, pSetTo->diskId, pSetTo->fid, pSetTo->pSmaF, fNameTo);
  pOutFD = taosCreateFile(fNameTo, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (pOutFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  PInFD = taosOpenFile(fNameFrom, TD_FILE_READ);
  if (PInFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  n = taosFSendFile(pOutFD, PInFD, 0, tsdbLogicToFileSize(pSetFrom->pSmaF->size, szPage));
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  taosCloseFile(&pOutFD);
  taosCloseFile(&PInFD);

  // stt
  for (int8_t iStt = 0; iStt < pSetFrom->nSttF; iStt++) {
    tsdbSttFileName(pTsdb, pSetFrom->diskId, pSetFrom->fid, pSetFrom->aSttF[iStt], fNameFrom);
    tsdbSttFileName(pTsdb, pSetTo->diskId, pSetTo->fid, pSetTo->aSttF[iStt], fNameTo);
    pOutFD = taosCreateFile(fNameTo, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
    if (pOutFD == NULL) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    PInFD = taosOpenFile(fNameFrom, TD_FILE_READ);
    if (PInFD == NULL) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    n = taosFSendFile(pOutFD, PInFD, 0, tsdbLogicToFileSize(pSetFrom->aSttF[iStt]->size, szPage));
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    taosCloseFile(&pOutFD);
    taosCloseFile(&PInFD);
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb DFileSet copy failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

// SDataFReader ====================================================
int32_t tsdbDataFReaderOpen(SDataFReader **ppReader, STsdb *pTsdb, SDFileSet *pSet) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SDataFReader *pReader = NULL;
  int32_t       szPage = pTsdb->pVnode->config.tsdbPageSize;
  char          fname[TSDB_FILENAME_LEN];

  // alloc
  pReader = (SDataFReader *)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pReader->pTsdb = pTsdb;
  pReader->pSet = pSet;

  // head
  tsdbHeadFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pHeadF, fname);
  code = tsdbOpenFile(fname, szPage, TD_FILE_READ, &pReader->pHeadFD);
  TSDB_CHECK_CODE(code, lino, _exit);

  // data
  tsdbDataFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pDataF, fname);
  code = tsdbOpenFile(fname, szPage, TD_FILE_READ, &pReader->pDataFD);
  TSDB_CHECK_CODE(code, lino, _exit);

  // sma
  tsdbSmaFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pSmaF, fname);
  code = tsdbOpenFile(fname, szPage, TD_FILE_READ, &pReader->pSmaFD);
  TSDB_CHECK_CODE(code, lino, _exit);

  // stt
  for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
    tsdbSttFileName(pTsdb, pSet->diskId, pSet->fid, pSet->aSttF[iStt], fname);
    code = tsdbOpenFile(fname, szPage, TD_FILE_READ, &pReader->aSttFD[iStt]);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    *ppReader = NULL;
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));

    if (pReader) {
      for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) tsdbCloseFile(&pReader->aSttFD[iStt]);
      tsdbCloseFile(&pReader->pSmaFD);
      tsdbCloseFile(&pReader->pDataFD);
      tsdbCloseFile(&pReader->pHeadFD);
      taosMemoryFree(pReader);
    }
  } else {
    *ppReader = pReader;
  }
  return code;
}

int32_t tsdbDataFReaderClose(SDataFReader **ppReader) {
  int32_t code = 0;
  if (*ppReader == NULL) return code;

  // head
  tsdbCloseFile(&(*ppReader)->pHeadFD);

  // data
  tsdbCloseFile(&(*ppReader)->pDataFD);

  // sma
  tsdbCloseFile(&(*ppReader)->pSmaFD);

  // stt
  for (int32_t iStt = 0; iStt < TSDB_MAX_STT_TRIGGER; iStt++) {
    if ((*ppReader)->aSttFD[iStt]) {
      tsdbCloseFile(&(*ppReader)->aSttFD[iStt]);
    }
  }

  for (int32_t iBuf = 0; iBuf < sizeof((*ppReader)->aBuf) / sizeof(uint8_t *); iBuf++) {
    tFree((*ppReader)->aBuf[iBuf]);
  }
  taosMemoryFree(*ppReader);
  *ppReader = NULL;
  return code;
}

int32_t tsdbReadBlockIdx(SDataFReader *pReader, SArray *aBlockIdx) {
  int32_t    code = 0;
  SHeadFile *pHeadFile = pReader->pSet->pHeadF;
  int64_t    offset = pHeadFile->offset;
  int64_t    size = pHeadFile->size - offset;

  taosArrayClear(aBlockIdx);
  if (size == 0) return code;

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  if (code) goto _err;

  // read
  code = tsdbReadFile(pReader->pHeadFD, offset, pReader->aBuf[0], size);
  if (code) goto _err;

  // decode
  int64_t n = 0;
  while (n < size) {
    SBlockIdx blockIdx;
    n += tGetBlockIdx(pReader->aBuf[0] + n, &blockIdx);

    if (taosArrayPush(aBlockIdx, &blockIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }
  ASSERT(n == size);

  return code;

_err:
  tsdbError("vgId:%d, read block idx failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadSttBlk(SDataFReader *pReader, int32_t iStt, SArray *aSttBlk) {
  int32_t   code = 0;
  SSttFile *pSttFile = pReader->pSet->aSttF[iStt];
  int64_t   offset = pSttFile->offset;
  int64_t   size = pSttFile->size - offset;

  taosArrayClear(aSttBlk);
  if (size == 0) return code;

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  if (code) goto _err;

  // read
  code = tsdbReadFile(pReader->aSttFD[iStt], offset, pReader->aBuf[0], size);
  if (code) goto _err;

  // decode
  int64_t n = 0;
  while (n < size) {
    SSttBlk sttBlk;
    n += tGetSttBlk(pReader->aBuf[0] + n, &sttBlk);

    if (taosArrayPush(aSttBlk, &sttBlk) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }
  ASSERT(n == size);

  return code;

_err:
  tsdbError("vgId:%d, read stt blk failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadDataBlk(SDataFReader *pReader, SBlockIdx *pBlockIdx, SMapData *mDataBlk) {
  int32_t code = 0;
  int64_t offset = pBlockIdx->offset;
  int64_t size = pBlockIdx->size;

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  if (code) goto _err;

  // read
  code = tsdbReadFile(pReader->pHeadFD, offset, pReader->aBuf[0], size);
  if (code) goto _err;

  // decode
  int64_t n = tGetMapData(pReader->aBuf[0], mDataBlk);
  if (n < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  ASSERT(n == size);

  return code;

_err:
  tsdbError("vgId:%d, read block failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadBlockSma(SDataFReader *pReader, SDataBlk *pDataBlk, SArray *aColumnDataAgg) {
  int32_t   code = 0;
  SSmaInfo *pSmaInfo = &pDataBlk->smaInfo;

  ASSERT(pSmaInfo->size > 0);

  taosArrayClear(aColumnDataAgg);

  // alloc
  code = tRealloc(&pReader->aBuf[0], pSmaInfo->size);
  if (code) goto _err;

  // read
  code = tsdbReadFile(pReader->pSmaFD, pSmaInfo->offset, pReader->aBuf[0], pSmaInfo->size);
  if (code) goto _err;

  // decode
  int32_t n = 0;
  while (n < pSmaInfo->size) {
    SColumnDataAgg sma;
    n += tGetColumnDataAgg(pReader->aBuf[0] + n, &sma);

    if (taosArrayPush(aColumnDataAgg, &sma) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }
  ASSERT(n == pSmaInfo->size);
  return code;

_err:
  tsdbError("vgId:%d, tsdb read block sma failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbReadBlockDataImpl(SDataFReader *pReader, SBlockInfo *pBlkInfo, SBlockData *pBlockData,
                                     int32_t iStt) {
  int32_t code = 0;

  tBlockDataClear(pBlockData);

  STsdbFD *pFD = (iStt < 0) ? pReader->pDataFD : pReader->aSttFD[iStt];

  // uid + version + tskey
  code = tRealloc(&pReader->aBuf[0], pBlkInfo->szKey);
  if (code) goto _err;

  code = tsdbReadFile(pFD, pBlkInfo->offset, pReader->aBuf[0], pBlkInfo->szKey);
  if (code) goto _err;

  SDiskDataHdr hdr;
  uint8_t     *p = pReader->aBuf[0] + tGetDiskDataHdr(pReader->aBuf[0], &hdr);

  ASSERT(hdr.delimiter == TSDB_FILE_DLMT);
  ASSERT(pBlockData->suid == hdr.suid);

  pBlockData->uid = hdr.uid;
  pBlockData->nRow = hdr.nRow;

  // uid
  if (hdr.uid == 0) {
    ASSERT(hdr.szUid);
    code = tsdbDecmprData(p, hdr.szUid, TSDB_DATA_TYPE_BIGINT, hdr.cmprAlg, (uint8_t **)&pBlockData->aUid,
                          sizeof(int64_t) * hdr.nRow, &pReader->aBuf[1]);
    if (code) goto _err;
  } else {
    ASSERT(!hdr.szUid);
  }
  p += hdr.szUid;

  // version
  code = tsdbDecmprData(p, hdr.szVer, TSDB_DATA_TYPE_BIGINT, hdr.cmprAlg, (uint8_t **)&pBlockData->aVersion,
                        sizeof(int64_t) * hdr.nRow, &pReader->aBuf[1]);
  if (code) goto _err;
  p += hdr.szVer;

  // TSKEY
  code = tsdbDecmprData(p, hdr.szKey, TSDB_DATA_TYPE_TIMESTAMP, hdr.cmprAlg, (uint8_t **)&pBlockData->aTSKEY,
                        sizeof(TSKEY) * hdr.nRow, &pReader->aBuf[1]);
  if (code) goto _err;
  p += hdr.szKey;

  ASSERT(p - pReader->aBuf[0] == pBlkInfo->szKey);

  // read and decode columns
  if (pBlockData->nColData == 0) goto _exit;

  if (hdr.szBlkCol > 0) {
    int64_t offset = pBlkInfo->offset + pBlkInfo->szKey;

    code = tRealloc(&pReader->aBuf[0], hdr.szBlkCol);
    if (code) goto _err;

    code = tsdbReadFile(pFD, offset, pReader->aBuf[0], hdr.szBlkCol);
    if (code) goto _err;
  }

  SBlockCol  blockCol = {.cid = 0};
  SBlockCol *pBlockCol = &blockCol;
  int32_t    n = 0;

  for (int32_t iColData = 0; iColData < pBlockData->nColData; iColData++) {
    SColData *pColData = tBlockDataGetColDataByIdx(pBlockData, iColData);

    while (pBlockCol && pBlockCol->cid < pColData->cid) {
      if (n < hdr.szBlkCol) {
        n += tGetBlockCol(pReader->aBuf[0] + n, pBlockCol);
      } else {
        ASSERT(n == hdr.szBlkCol);
        pBlockCol = NULL;
      }
    }

    if (pBlockCol == NULL || pBlockCol->cid > pColData->cid) {
      // add a lot of NONE
      for (int32_t iRow = 0; iRow < hdr.nRow; iRow++) {
        code = tColDataAppendValue(pColData, &COL_VAL_NONE(pColData->cid, pColData->type));
        if (code) goto _err;
      }
    } else {
      ASSERT(pBlockCol->type == pColData->type);
      ASSERT(pBlockCol->flag && pBlockCol->flag != HAS_NONE);

      if (pBlockCol->flag == HAS_NULL) {
        // add a lot of NULL
        for (int32_t iRow = 0; iRow < hdr.nRow; iRow++) {
          code = tColDataAppendValue(pColData, &COL_VAL_NULL(pBlockCol->cid, pBlockCol->type));
          if (code) goto _err;
        }
      } else {
        // decode from binary
        int64_t offset = pBlkInfo->offset + pBlkInfo->szKey + hdr.szBlkCol + pBlockCol->offset;
        int32_t size = pBlockCol->szBitmap + pBlockCol->szOffset + pBlockCol->szValue;

        code = tRealloc(&pReader->aBuf[1], size);
        if (code) goto _err;

        code = tsdbReadFile(pFD, offset, pReader->aBuf[1], size);
        if (code) goto _err;

        code = tsdbDecmprColData(pReader->aBuf[1], pBlockCol, hdr.cmprAlg, hdr.nRow, pColData, &pReader->aBuf[2]);
        if (code) goto _err;
      }
    }
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d, tsdb read block data impl failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadDataBlockEx(SDataFReader *pReader, SDataBlk *pDataBlk, SBlockData *pBlockData) {
  int32_t     code = 0;
  SBlockInfo *pBlockInfo = &pDataBlk->aSubBlock[0];

  // alloc
  code = tRealloc(&pReader->aBuf[0], pBlockInfo->szBlock);
  if (code) goto _err;

  // read
  code = tsdbReadFile(pReader->pDataFD, pBlockInfo->offset, pReader->aBuf[0], pBlockInfo->szBlock);
  if (code) goto _err;

  // decmpr
  code = tDecmprBlockData(pReader->aBuf[0], pBlockInfo->szBlock, pBlockData, &pReader->aBuf[1]);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d, tsdb read data block ex failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadDataBlock(SDataFReader *pReader, SDataBlk *pDataBlk, SBlockData *pBlockData) {
  int32_t code = 0;

  code = tsdbReadBlockDataImpl(pReader, &pDataBlk->aSubBlock[0], pBlockData, -1);
  if (code) goto _err;

  ASSERT(pDataBlk->nSubBlock == 1);

  return code;

_err:
  tsdbError("vgId:%d, tsdb read data block failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadSttBlock(SDataFReader *pReader, int32_t iStt, SSttBlk *pSttBlk, SBlockData *pBlockData) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbReadBlockDataImpl(pReader, &pSttBlk->bInfo, pBlockData, iStt);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %d since %s", TD_VID(pReader->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbReadSttBlockEx(SDataFReader *pReader, int32_t iStt, SSttBlk *pSttBlk, SBlockData *pBlockData) {
  int32_t code = 0;
  int32_t lino = 0;

  // alloc
  code = tRealloc(&pReader->aBuf[0], pSttBlk->bInfo.szBlock);
  TSDB_CHECK_CODE(code, lino, _exit);

  // read
  code = tsdbReadFile(pReader->aSttFD[iStt], pSttBlk->bInfo.offset, pReader->aBuf[0], pSttBlk->bInfo.szBlock);
  TSDB_CHECK_CODE(code, lino, _exit);

  // decmpr
  code = tDecmprBlockData(pReader->aBuf[0], pSttBlk->bInfo.szBlock, pBlockData, &pReader->aBuf[1]);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %d since %s", TD_VID(pReader->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

// SDelFWriter ====================================================
int32_t tsdbDelFWriterOpen(SDelFWriter **ppWriter, SDelFile *pFile, STsdb *pTsdb) {
  int32_t      code = 0;
  int32_t      lino = 0;
  char         fname[TSDB_FILENAME_LEN];
  uint8_t      hdr[TSDB_FHDR_SIZE] = {0};
  SDelFWriter *pDelFWriter = NULL;
  int64_t      n;

  // alloc
  pDelFWriter = (SDelFWriter *)taosMemoryCalloc(1, sizeof(*pDelFWriter));
  if (pDelFWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pDelFWriter->pTsdb = pTsdb;
  pDelFWriter->fDel = *pFile;

  tsdbDelFileName(pTsdb, pFile, fname);
  code = tsdbOpenFile(fname, pTsdb->pVnode->config.tsdbPageSize, TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE,
                      &pDelFWriter->pWriteH);
  TSDB_CHECK_CODE(code, lino, _exit);

  // update header
  code = tsdbWriteFile(pDelFWriter->pWriteH, 0, hdr, TSDB_FHDR_SIZE);
  TSDB_CHECK_CODE(code, lino, _exit);

  pDelFWriter->fDel.size = TSDB_FHDR_SIZE;
  pDelFWriter->fDel.offset = 0;

  *ppWriter = pDelFWriter;

_exit:
  if (code) {
    if (pDelFWriter) {
      tsdbCloseFile(&pDelFWriter->pWriteH);
      taosMemoryFree(pDelFWriter);
    }
    *ppWriter = NULL;
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(errno));
  } else {
    *ppWriter = pDelFWriter;
  }
  return code;
}

int32_t tsdbDelFWriterClose(SDelFWriter **ppWriter, int8_t sync) {
  int32_t      code = 0;
  SDelFWriter *pWriter = *ppWriter;
  STsdb       *pTsdb = pWriter->pTsdb;

  // sync
  if (sync) {
    code = tsdbFsyncFile(pWriter->pWriteH);
    if (code) goto _err;
  }

  // close
  tsdbCloseFile(&pWriter->pWriteH);

  for (int32_t iBuf = 0; iBuf < sizeof(pWriter->aBuf) / sizeof(uint8_t *); iBuf++) {
    tFree(pWriter->aBuf[iBuf]);
  }
  taosMemoryFree(pWriter);

  *ppWriter = NULL;
  return code;

_err:
  tsdbError("vgId:%d, failed to close del file writer since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteDelData(SDelFWriter *pWriter, SArray *aDelData, SDelIdx *pDelIdx) {
  int32_t code = 0;
  int64_t size;
  int64_t n;

  // prepare
  size = 0;
  for (int32_t iDelData = 0; iDelData < taosArrayGetSize(aDelData); iDelData++) {
    size += tPutDelData(NULL, taosArrayGet(aDelData, iDelData));
  }

  // alloc
  code = tRealloc(&pWriter->aBuf[0], size);
  if (code) goto _err;

  // build
  n = 0;
  for (int32_t iDelData = 0; iDelData < taosArrayGetSize(aDelData); iDelData++) {
    n += tPutDelData(pWriter->aBuf[0] + n, taosArrayGet(aDelData, iDelData));
  }
  ASSERT(n == size);

  // write
  code = tsdbWriteFile(pWriter->pWriteH, pWriter->fDel.size, pWriter->aBuf[0], size);
  if (code) goto _err;

  // update
  pDelIdx->offset = pWriter->fDel.size;
  pDelIdx->size = size;
  pWriter->fDel.size += size;

  return code;

_err:
  tsdbError("vgId:%d, failed to write del data since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteDelIdx(SDelFWriter *pWriter, SArray *aDelIdx) {
  int32_t  code = 0;
  int64_t  size;
  int64_t  n;
  SDelIdx *pDelIdx;

  // prepare
  size = 0;
  for (int32_t iDelIdx = 0; iDelIdx < taosArrayGetSize(aDelIdx); iDelIdx++) {
    size += tPutDelIdx(NULL, taosArrayGet(aDelIdx, iDelIdx));
  }

  // alloc
  code = tRealloc(&pWriter->aBuf[0], size);
  if (code) goto _err;

  // build
  n = 0;
  for (int32_t iDelIdx = 0; iDelIdx < taosArrayGetSize(aDelIdx); iDelIdx++) {
    n += tPutDelIdx(pWriter->aBuf[0] + n, taosArrayGet(aDelIdx, iDelIdx));
  }
  ASSERT(n == size);

  // write
  code = tsdbWriteFile(pWriter->pWriteH, pWriter->fDel.size, pWriter->aBuf[0], size);
  if (code) goto _err;

  // update
  pWriter->fDel.offset = pWriter->fDel.size;
  pWriter->fDel.size += size;

  return code;

_err:
  tsdbError("vgId:%d, write del idx failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbUpdateDelFileHdr(SDelFWriter *pWriter) {
  int32_t code = 0;
  char    hdr[TSDB_FHDR_SIZE] = {0};
  int64_t size = TSDB_FHDR_SIZE;
  int64_t n;

  // build
  tPutDelFile(hdr, &pWriter->fDel);

  // write
  code = tsdbWriteFile(pWriter->pWriteH, 0, hdr, size);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d, update del file hdr failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}
// SDelFReader ====================================================
struct SDelFReader {
  STsdb   *pTsdb;
  SDelFile fDel;
  STsdbFD *pReadH;
  uint8_t *aBuf[1];
};

int32_t tsdbDelFReaderOpen(SDelFReader **ppReader, SDelFile *pFile, STsdb *pTsdb) {
  int32_t      code = 0;
  int32_t      lino = 0;
  char         fname[TSDB_FILENAME_LEN];
  SDelFReader *pDelFReader = NULL;

  // alloc
  pDelFReader = (SDelFReader *)taosMemoryCalloc(1, sizeof(*pDelFReader));
  if (pDelFReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // open impl
  pDelFReader->pTsdb = pTsdb;
  pDelFReader->fDel = *pFile;

  tsdbDelFileName(pTsdb, pFile, fname);
  code = tsdbOpenFile(fname, pTsdb->pVnode->config.tsdbPageSize, TD_FILE_READ, &pDelFReader->pReadH);
  if (code) {
    taosMemoryFree(pDelFReader);
    goto _exit;
  }

_exit:
  if (code) {
    *ppReader = NULL;
    tsdbError("vgId:%d, %s failed at %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    *ppReader = pDelFReader;
  }
  return code;
}

int32_t tsdbDelFReaderClose(SDelFReader **ppReader) {
  int32_t      code = 0;
  SDelFReader *pReader = *ppReader;

  if (pReader) {
    tsdbCloseFile(&pReader->pReadH);
    for (int32_t iBuf = 0; iBuf < sizeof(pReader->aBuf) / sizeof(uint8_t *); iBuf++) {
      tFree(pReader->aBuf[iBuf]);
    }
    taosMemoryFree(pReader);
  }

  *ppReader = NULL;
  return code;
}

int32_t tsdbReadDelData(SDelFReader *pReader, SDelIdx *pDelIdx, SArray *aDelData) {
    return tsdbReadDelDatav1(pReader, pDelIdx, aDelData, INT64_MAX);
}

int32_t tsdbReadDelDatav1(SDelFReader *pReader, SDelIdx *pDelIdx, SArray *aDelData, int64_t maxVer) {
  int32_t code = 0;
  int64_t offset = pDelIdx->offset;
  int64_t size = pDelIdx->size;
  int64_t n;

  taosArrayClear(aDelData);

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  if (code) goto _err;

  // read
  code = tsdbReadFile(pReader->pReadH, offset, pReader->aBuf[0], size);
  if (code) goto _err;

  // // decode
  n = 0;
  while (n < size) {
    SDelData delData;
    n += tGetDelData(pReader->aBuf[0] + n, &delData);

    if (delData.version > maxVer) {
      continue;
    }
      if (taosArrayPush(aDelData, &delData) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _err;
      }
  }

  ASSERT(n == size);

  return code;

_err:
  tsdbError("vgId:%d, read del data failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadDelIdx(SDelFReader *pReader, SArray *aDelIdx) {
  int32_t code = 0;
  int32_t n;
  int64_t offset = pReader->fDel.offset;
  int64_t size = pReader->fDel.size - offset;

  taosArrayClear(aDelIdx);

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  if (code) goto _err;

  // read
  code = tsdbReadFile(pReader->pReadH, offset, pReader->aBuf[0], size);
  if (code) goto _err;

  // decode
  n = 0;
  while (n < size) {
    SDelIdx delIdx;

    n += tGetDelIdx(pReader->aBuf[0] + n, &delIdx);

    if (taosArrayPush(aDelIdx, &delIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  ASSERT(n == size);

  return code;

_err:
  tsdbError("vgId:%d, read del idx failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}
