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

#define TSDB_FILE_DLMT ((uint32_t)0xF00AFA0F)

// SDelFWriter ====================================================
int32_t tsdbDelFWriterOpen(SDelFWriter **ppWriter, SDelFile *pFile, STsdb *pTsdb) {
  int32_t      code = 0;
  char         fname[TSDB_FILENAME_LEN];
  char         hdr[TSDB_FHDR_SIZE] = {0};
  SDelFWriter *pDelFWriter;
  int64_t      n;

  // alloc
  pDelFWriter = (SDelFWriter *)taosMemoryCalloc(1, sizeof(*pDelFWriter));
  if (pDelFWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pDelFWriter->pTsdb = pTsdb;
  pDelFWriter->fDel = *pFile;

  tsdbDelFileName(pTsdb, pFile, fname);
  pDelFWriter->pWriteH = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE);
  if (pDelFWriter->pWriteH == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // update header
  n = taosWriteFile(pDelFWriter->pWriteH, &hdr, TSDB_FHDR_SIZE);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pDelFWriter->fDel.size = TSDB_FHDR_SIZE;
  pDelFWriter->fDel.size = 0;

  *ppWriter = pDelFWriter;
  return code;

_err:
  tsdbError("vgId:%d failed to open del file writer since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

int32_t tsdbDelFWriterClose(SDelFWriter *pWriter, int8_t sync) {
  int32_t code = 0;

  // sync
  if (sync && taosFsyncFile(pWriter->pWriteH) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // close
  if (taosCloseFile(&pWriter->pWriteH) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d failed to close del file writer since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteDelData(SDelFWriter *pWriter, SArray *aDelData, uint8_t **ppBuf, SDelIdx *pDelIdx) {
  int32_t       code = 0;
  uint8_t      *pBuf = NULL;
  int64_t       size;
  int64_t       n;
  SBlockDataHdr hdr = {.delimiter = TSDB_FILE_DLMT, .suid = pDelIdx->suid, .uid = pDelIdx->uid};

  if (!ppBuf) ppBuf = &pBuf;

  // prepare
  size = sizeof(hdr);
  for (int32_t iDelData = 0; iDelData < taosArrayGetSize(aDelData); iDelData++) {
    size += tPutDelData(NULL, taosArrayGet(aDelData, iDelData));
  }
  size += sizeof(TSCKSUM);

  // alloc
  code = tsdbRealloc(ppBuf, size);
  if (code) goto _err;

  // build
  n = 0;
  *(SBlockDataHdr *)(*ppBuf) = hdr;
  n += sizeof(hdr);
  for (int32_t iDelData = 0; iDelData < taosArrayGetSize(aDelData); iDelData++) {
    n += tPutDelData(*ppBuf + n, taosArrayGet(aDelData, iDelData));
  }
  taosCalcChecksumAppend(0, *ppBuf, size);

  ASSERT(n + sizeof(TSCKSUM) == size);

  // write
  n = taosWriteFile(pWriter->pWriteH, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  ASSERT(n == size);

  // update
  pDelIdx->offset = pWriter->fDel.size;
  pDelIdx->size = size;
  pWriter->fDel.size += size;

  tsdbFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d failed to write del data since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  tsdbFree(pBuf);
  return code;
}

int32_t tsdbWriteDelIdx(SDelFWriter *pWriter, SArray *aDelIdx, uint8_t **ppBuf) {
  int32_t  code = 0;
  int64_t  size;
  int64_t  n;
  uint8_t *pBuf = NULL;
  SDelIdx *pDelIdx;

  if (!ppBuf) ppBuf = &pBuf;

  // prepare
  size = 0;
  size += tPutU32(NULL, TSDB_FILE_DLMT);
  for (int32_t iDelIdx = 0; iDelIdx < taosArrayGetSize(aDelIdx); iDelIdx++) {
    size += tPutDelIdx(NULL, taosArrayGet(aDelIdx, iDelIdx));
  }
  size += sizeof(TSCKSUM);

  // alloc
  code = tsdbRealloc(ppBuf, size);
  if (code) goto _err;

  // build
  n = 0;
  n += tPutU32(*ppBuf + n, TSDB_FILE_DLMT);
  for (int32_t iDelIdx = 0; iDelIdx < taosArrayGetSize(aDelIdx); iDelIdx++) {
    n += tPutDelIdx(*ppBuf + n, taosArrayGet(aDelIdx, iDelIdx));
  }
  taosCalcChecksumAppend(0, *ppBuf, size);

  ASSERT(n + sizeof(TSCKSUM) == size);

  // write
  n = taosWriteFile(pWriter->pWriteH, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // update
  pWriter->fDel.offset = pWriter->fDel.size;
  pWriter->fDel.size += size;

  tsdbFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d write del idx failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  tsdbFree(pBuf);
  return code;
}

int32_t tsdbUpdateDelFileHdr(SDelFWriter *pWriter) {
  int32_t code = 0;
  char    hdr[TSDB_FHDR_SIZE];
  int64_t size = TSDB_FHDR_SIZE;
  int64_t n;

  // build
  memset(hdr, 0, size);
  tPutDelFile(hdr, &pWriter->fDel);
  taosCalcChecksumAppend(0, hdr, size);

  // seek
  if (taosLSeekFile(pWriter->pWriteH, 0, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // write
  n = taosWriteFile(pWriter->pWriteH, hdr, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d update del file hdr failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

// SDelFReader ====================================================
struct SDelFReader {
  STsdb    *pTsdb;
  SDelFile  fDel;
  TdFilePtr pReadH;
};

int32_t tsdbDelFReaderOpen(SDelFReader **ppReader, SDelFile *pFile, STsdb *pTsdb, uint8_t **ppBuf) {
  int32_t      code = 0;
  char         fname[TSDB_FILENAME_LEN];
  SDelFReader *pDelFReader;
  int64_t      n;

  // alloc
  pDelFReader = (SDelFReader *)taosMemoryCalloc(1, sizeof(*pDelFReader));
  if (pDelFReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // open impl
  pDelFReader->pTsdb = pTsdb;
  pDelFReader->fDel = *pFile;

  tsdbDelFileName(pTsdb, pFile, fname);
  pDelFReader->pReadH = taosOpenFile(fname, TD_FILE_READ);
  if (pDelFReader == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    taosMemoryFree(pDelFReader);
    goto _err;
  }

#if 0
  // load and check hdr if buffer is given
  if (ppBuf) {
    code = tsdbRealloc(ppBuf, TSDB_FHDR_SIZE);
    if (code) {
      goto _err;
    }

    n = taosReadFile(pDelFReader->pReadH, *ppBuf, TSDB_FHDR_SIZE);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    } else if (n < TSDB_FHDR_SIZE) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    }

    if (!taosCheckChecksumWhole(*ppBuf, TSDB_FHDR_SIZE)) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    }

    // TODO: check the content
  }
#endif

_exit:
  *ppReader = pDelFReader;
  return code;

_err:
  tsdbError("vgId:%d del file reader open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t tsdbDelFReaderClose(SDelFReader *pReader) {
  int32_t code = 0;

  if (pReader) {
    if (taosCloseFile(&pReader->pReadH) < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _exit;
    }
    taosMemoryFree(pReader);
  }

_exit:
  return code;
}

int32_t tsdbReadDelData(SDelFReader *pReader, SDelIdx *pDelIdx, SArray *aDelData, uint8_t **ppBuf) {
  int32_t        code = 0;
  int64_t        offset = pDelIdx->offset;
  int64_t        size = pDelIdx->size;
  int64_t        n;
  uint8_t       *pBuf = NULL;
  SBlockDataHdr *pHdr;
  SDelData      *pDelData = &(SDelData){0};

  if (!ppBuf) ppBuf = &pBuf;

  // seek
  if (taosLSeekFile(pReader->pReadH, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // alloc
  code = tsdbRealloc(ppBuf, size);
  if (code) goto _err;

  // read
  n = taosReadFile(pReader->pReadH, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // check
  if (!taosCheckChecksumWhole(*ppBuf, size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // // decode
  n = 0;
  pHdr = (SBlockDataHdr *)(*ppBuf + n);
  ASSERT(pHdr->delimiter == TSDB_FILE_DLMT);
  ASSERT(pHdr->suid == pDelIdx->suid);
  ASSERT(pHdr->uid == pDelIdx->uid);
  n += sizeof(*pHdr);
  while (n < size - sizeof(TSCKSUM)) {
    n += tGetDelData(*ppBuf + n, pDelData);
  }

  ASSERT(n == size - sizeof(TSCKSUM));

  tsdbFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d read del data failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  tsdbFree(pBuf);
  return code;
}

int32_t tsdbReadDelIdx(SDelFReader *pReader, SArray *aDelIdx, uint8_t **ppBuf) {
  int32_t  code = 0;
  int32_t  n;
  int64_t  offset = pReader->fDel.offset;
  int64_t  size = pReader->fDel.size - offset;
  uint32_t delimiter;
  uint8_t *pBuf = NULL;
  SDelIdx *pDelIdx = &(SDelIdx){};

  if (!ppBuf) ppBuf = &pBuf;

  // seek
  if (taosLSeekFile(pReader->pReadH, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // alloc
  code = tsdbRealloc(ppBuf, size);
  if (code) goto _err;

  // read
  n = taosReadFile(pReader->pReadH, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // check
  if (!taosCheckChecksumWhole(*ppBuf, size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // decode
  n = 0;
  n += tGetU32(*ppBuf + n, &delimiter);
  ASSERT(delimiter == TSDB_FILE_DLMT);

  taosArrayClear(aDelIdx);
  while (n < size - sizeof(TSCKSUM)) {
    n += tGetDelIdx(*ppBuf + n, pDelIdx);

    if (taosArrayPush(aDelIdx, pDelIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  ASSERT(n == size - sizeof(TSCKSUM));

  return code;

_err:
  tsdbError("vgId:%d read del idx failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

// SDataFReader ====================================================
struct SDataFReader {
  STsdb     *pTsdb;
  SDFileSet *pSet;
  TdFilePtr  pHeadFD;
  TdFilePtr  pDataFD;
  TdFilePtr  pLastFD;
  TdFilePtr  pSmaFD;
};

int32_t tsdbDataFReaderOpen(SDataFReader **ppReader, STsdb *pTsdb, SDFileSet *pSet) {
  int32_t       code = 0;
  SDataFReader *pReader;
  char          fname[TSDB_FILENAME_LEN];

  // alloc
  pReader = (SDataFReader *)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pTsdb = pTsdb;
  pReader->pSet = pSet;

  // open impl
  // head
  tsdbDataFileName(pTsdb, pSet, TSDB_HEAD_FILE, fname);
  pReader->pHeadFD = taosOpenFile(fname, TD_FILE_READ);
  if (pReader->pHeadFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // data
  tsdbDataFileName(pTsdb, pSet, TSDB_DATA_FILE, fname);
  pReader->pDataFD = taosOpenFile(fname, TD_FILE_READ);
  if (pReader->pDataFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // last
  tsdbDataFileName(pTsdb, pSet, TSDB_LAST_FILE, fname);
  pReader->pLastFD = taosOpenFile(fname, TD_FILE_READ);
  if (pReader->pLastFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // sma
  tsdbDataFileName(pTsdb, pSet, TSDB_SMA_FILE, fname);
  pReader->pSmaFD = taosOpenFile(fname, TD_FILE_READ);
  if (pReader->pSmaFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  *ppReader = pReader;
  return code;

_err:
  tsdbError("vgId:%d tsdb data file reader open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t tsdbDataFReaderClose(SDataFReader **ppReader) {
  int32_t code = 0;

  if (taosCloseFile(&(*ppReader)->pHeadFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosCloseFile(&(*ppReader)->pDataFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosCloseFile(&(*ppReader)->pLastFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosCloseFile(&(*ppReader)->pSmaFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  taosMemoryFree(*ppReader);
  *ppReader = NULL;
  return code;

_err:
  tsdbError("vgId:%d data file reader close failed since %s", TD_VID((*ppReader)->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadBlockIdx(SDataFReader *pReader, SMapData *mBlockIdx, uint8_t **ppBuf) {
  int32_t  code = 0;
  int64_t  offset = pReader->pSet->fHead.offset;
  int64_t  size = pReader->pSet->fHead.size - offset;
  int64_t  n;
  uint32_t delimiter;

  // alloc
  if (!ppBuf) ppBuf = &mBlockIdx->pBuf;
  code = tsdbRealloc(ppBuf, size);
  if (code) goto _err;

  // seek
  if (taosLSeekFile(pReader->pHeadFD, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // read
  n = taosReadFile(pReader->pHeadFD, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // check
  if (!taosCheckChecksumWhole(*ppBuf, size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // decode
  n = 0;
  n += tGetU32(*ppBuf + n, &delimiter);
  ASSERT(delimiter == TSDB_FILE_DLMT);
  n += tGetMapData(*ppBuf + n, mBlockIdx);
  ASSERT(n + sizeof(TSCKSUM) == size);

  return code;

_err:
  tsdbError("vgId:%d read block idx failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadBlock(SDataFReader *pReader, SBlockIdx *pBlockIdx, SMapData *mBlock, uint8_t **ppBuf) {
  int32_t       code = 0;
  int64_t       offset = pBlockIdx->offset;
  int64_t       size = pBlockIdx->size;
  int64_t       n;
  SBlockDataHdr hdr;

  // alloc
  if (!ppBuf) ppBuf = &mBlock->pBuf;
  code = tsdbRealloc(ppBuf, size);
  if (code) goto _err;

  // seek
  if (taosLSeekFile(pReader->pHeadFD, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // read
  n = taosReadFile(pReader->pHeadFD, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // check
  if (!taosCheckChecksumWhole(*ppBuf, size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // decode
  hdr = *(SBlockDataHdr *)(*ppBuf);
  ASSERT(hdr.delimiter == TSDB_FILE_DLMT);
  ASSERT(hdr.suid == pBlockIdx->suid);
  ASSERT(hdr.uid == pBlockIdx->uid);

  n = sizeof(hdr);
  n += tGetMapData(*ppBuf + n, mBlock);
  ASSERT(n + sizeof(TSCKSUM) == size);

  return code;

_err:
  tsdbError("vgId:%d read block failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbRecoverBlockDataKey(SBlockData *pBlockData, SSubBlock *pSubBlock, uint8_t *pBuf, uint8_t **ppBuf) {
  int32_t code = 0;
  int64_t size = pSubBlock->vsize + pSubBlock->ksize + sizeof(TSCKSUM);
  int64_t n;

  if (!taosCheckChecksumWhole(pBuf, size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  code = tsdbRealloc((uint8_t **)&pBlockData->aVersion, sizeof(int64_t) * pSubBlock->nRow);
  if (code) goto _err;
  code = tsdbRealloc((uint8_t **)&pBlockData->aTSKEY, sizeof(TSKEY) * pSubBlock->nRow);
  if (code) goto _err;

  if (pSubBlock->cmprAlg == NO_COMPRESSION) {
    ASSERT(pSubBlock->vsize == sizeof(int64_t) * pSubBlock->nRow);
    ASSERT(pSubBlock->ksize == sizeof(TSKEY) * pSubBlock->nRow);

    // VERSION
    memcpy(pBlockData->aVersion, pBuf, pSubBlock->vsize);

    // TSKEY
    pBuf = pBuf + pSubBlock->vsize;
    memcpy(pBlockData->aTSKEY, pBuf + pSubBlock->vsize, pSubBlock->ksize);
  } else {
    size = sizeof(int64_t) * pSubBlock->nRow + COMP_OVERFLOW_BYTES;
    if (pSubBlock->cmprAlg == TWO_STAGE_COMP) {
      code = tsdbRealloc(ppBuf, size);
      if (code) goto _err;
    }

    // VERSION
    n = tsDecompressBigint(pBuf, pSubBlock->vsize, pSubBlock->nRow, (char *)pBlockData->aVersion,
                           sizeof(int64_t) * pSubBlock->nRow, pSubBlock->cmprAlg, *ppBuf, size);
    if (n < 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }

    // TSKEY
    pBuf = pBuf + pSubBlock->vsize;
    n = tsDecompressTimestamp(pBuf, pSubBlock->ksize, pSubBlock->nRow, (char *)pBlockData->aTSKEY,
                              sizeof(TSKEY) * pSubBlock->nRow, pSubBlock->cmprAlg, *ppBuf, size);
    if (n < 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }
  }

  return code;

_err:
  return code;
}

static int32_t tsdbRecoverColData(SBlockData *pBlockData, SSubBlock *pSubBlock, SBlockCol *pBlockCol,
                                  SColData *pColData, uint8_t *pBuf, uint8_t **ppBuf) {
  int32_t code = 0;
  int64_t size;
  int64_t n;

  ASSERT(pBlockCol->flag != HAS_NULL);

  if (!taosCheckChecksumWhole(pBuf, pBlockCol->bsize + pBlockCol->csize + sizeof(TSCKSUM))) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  pColData->nVal = pSubBlock->nRow;
  pColData->flag = pBlockCol->flag;

  // bitmap
  if (pBlockCol->flag != HAS_VALUE) {
    size = BIT2_SIZE(pSubBlock->nRow);
    code = tsdbRealloc(&pColData->pBitMap, size);
    if (code) goto _err;

    ASSERT(pBlockCol->bsize == size);

    memcpy(pColData->pBitMap, pBuf, size);
  } else {
    ASSERT(pBlockCol->bsize == 0);
  }
  pBuf = pBuf + pBlockCol->bsize;

  // value
  if (IS_VAR_DATA_TYPE(pBlockCol->type)) {
    pColData->nData = pBlockCol->osize;
  } else {
    pColData->nData = tDataTypes[pBlockCol->type].bytes * pSubBlock->nRow;
  }
  code = tsdbRealloc(&pColData->pData, pColData->nData);
  if (code) goto _err;

  if (pSubBlock->cmprAlg == NO_COMPRESSION) {
    memcpy(pColData->pData, pBuf, pColData->nData);
  } else {
    size = pColData->nData + COMP_OVERFLOW_BYTES;
    if (pSubBlock->cmprAlg == TWO_STAGE_COMP) {
      code = tsdbRealloc(ppBuf, size);
      if (code) goto _err;
    }

    n = tDataTypes[pBlockCol->type].decompFunc(pBuf, pBlockCol->csize, pSubBlock->nRow, pColData->pData,
                                               pColData->nData, pSubBlock->cmprAlg, *ppBuf, size);
    if (n < 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }

    ASSERT(n == pColData->nData);
  }

  return code;

_err:
  return code;
}

int32_t tsdbReadColData(SDataFReader *pReader, SBlockIdx *pBlockIdx, SBlock *pBlock, int16_t *aColId, int32_t nCol,
                        SBlockData *pBlockData, uint8_t **ppBuf1, uint8_t **ppBuf2) {
  int32_t   code = 0;
  TdFilePtr pFD = pBlock->last ? pReader->pLastFD : pReader->pDataFD;
  uint8_t  *pBuf1 = NULL;
  uint8_t  *pBuf2 = NULL;

  ASSERT(nCol == 0 || aColId[0] != PRIMARYKEY_TIMESTAMP_COL_ID);

  if (!ppBuf1) ppBuf1 = &pBuf1;
  if (!ppBuf2) ppBuf2 = &pBuf2;

  for (int32_t iSubBlock = 0; iSubBlock < pBlock->nSubBlock; iSubBlock++) {
    SSubBlock *pSubBlock = &pBlock->aSubBlock[iSubBlock];
    int64_t    offset;
    int64_t    size;
    int64_t    n;

    tBlockDataReset(pBlockData);
    pBlockData->nRow = pSubBlock->nRow;

    // TSDBKEY
    offset = pSubBlock->offset + sizeof(SBlockDataHdr);
    size = pSubBlock->vsize + pSubBlock->ksize + sizeof(TSCKSUM);
    code = tsdbRealloc(ppBuf1, size);
    if (code) goto _err;

    n = taosLSeekFile(pFD, offset, SEEK_SET);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    n = taosReadFile(pFD, *ppBuf1, size);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    } else if (n < size) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    }

    code = tsdbRecoverBlockDataKey(pBlockData, pSubBlock, *ppBuf1, ppBuf2);
    if (code) goto _err;

    // OTHER
    SBlockCol  blockCol;
    SBlockCol *pBlockCol = &blockCol;
    SColData  *pColData;
    for (int32_t iCol = 0; iCol < nCol; iCol++) {
      int16_t cid = aColId[iCol];

      if (tMapDataSearch(&pSubBlock->mBlockCol, &(SBlockCol){.cid = cid}, tGetBlockCol, tBlockColCmprFn, pBlockCol) ==
          0) {
        code = tBlockDataAddColData(pBlockData, taosArrayGetSize(pBlockData->aColDataP), &pColData);
        if (code) goto _err;

        tColDataReset(pColData, pBlockCol->cid, pBlockCol->type);
        if (pBlockCol->flag == HAS_NULL) {
          for (int32_t iRow = 0; iRow < pSubBlock->nRow; iRow++) {
            code = tColDataAppendValue(pColData, &COL_VAL_NULL(pBlockCol->cid, pBlockCol->type));
            if (code) goto _err;
          }
        } else {
          offset = pSubBlock->offset + sizeof(SBlockDataHdr) + pSubBlock->vsize + pSubBlock->ksize + pBlockCol->offset;
          size = pBlockCol->bsize + pBlockCol->csize + sizeof(TSCKSUM);

          code = tsdbRealloc(ppBuf1, size);
          if (code) goto _err;

          // seek
          n = taosLSeekFile(pFD, offset, SEEK_SET);
          if (n < 0) {
            code = TAOS_SYSTEM_ERROR(errno);
            goto _err;
          }

          // read
          n = taosReadFile(pFD, *ppBuf1, size);
          if (n < 0) {
            code = TAOS_SYSTEM_ERROR(errno);
            goto _err;
          } else if (n < size) {
            code = TSDB_CODE_FILE_CORRUPTED;
            goto _err;
          }

          code = tsdbRecoverColData(pBlockData, pSubBlock, pBlockCol, pColData, *ppBuf1, ppBuf2);
          if (code) goto _err;
        }
      }
    }
  }

  tsdbFree(pBuf1);
  tsdbFree(pBuf2);
  return code;

_err:
  tsdbError("vgId:%d tsdb read col data failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  tsdbFree(pBuf1);
  tsdbFree(pBuf2);
  return code;
}

static int32_t tsdbReadSubBlockData(SDataFReader *pReader, SBlockIdx *pBlockIdx, SBlock *pBlock, int32_t iSubBlock,
                                    SBlockData *pBlockData, uint8_t **ppBuf1, uint8_t **ppBuf2) {
  int32_t    code = 0;
  uint8_t   *p;
  int64_t    size;
  int64_t    n;
  TdFilePtr  pFD = pBlock->last ? pReader->pLastFD : pReader->pDataFD;
  SSubBlock *pSubBlock = &pBlock->aSubBlock[iSubBlock];
  SBlockCol *pBlockCol = &(SBlockCol){0};

  tBlockDataReset(pBlockData);

  // realloc
  code = tsdbRealloc(ppBuf1, pSubBlock->bsize);
  if (code) goto _err;

  // seek
  n = taosLSeekFile(pFD, pSubBlock->offset, SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // read
  n = taosReadFile(pFD, *ppBuf1, pSubBlock->bsize);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < pSubBlock->bsize) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // // check
  // p = *ppBuf1;
  // SBlockDataHdr *pHdr = (SBlockDataHdr *)p;
  // ASSERT(pHdr->delimiter == TSDB_FILE_DLMT);
  // ASSERT(pHdr->suid == pBlockIdx->suid);
  // ASSERT(pHdr->uid == pBlockIdx->uid);
  // p += sizeof(*pHdr);

  // if (!taosCheckChecksumWhole(p, pSubBlock->vsize + pSubBlock->ksize + sizeof(TSCKSUM))) {
  //   code = TSDB_CODE_FILE_CORRUPTED;
  //   goto _err;
  // }
  // p += (pSubBlock->vsize + pSubBlock->ksize + sizeof(TSCKSUM));

  // for (int32_t iBlockCol = 0; iBlockCol < pSubBlock->mBlockCol.nItem; iBlockCol++) {
  //   tMapDataGetItemByIdx(&pSubBlock->mBlockCol, iBlockCol, pBlockCol, tGetBlockCol);

  //   ASSERT(pBlockCol->flag && pBlockCol->flag != HAS_NONE);

  //   if (pBlockCol->flag == HAS_NULL) continue;

  //   if (!taosCheckChecksumWhole(p, pBlockCol->bsize + pBlockCol->csize + sizeof(TSCKSUM))) {
  //     code = TSDB_CODE_FILE_CORRUPTED;
  //     goto _err;
  //   }
  //   p = p + pBlockCol->bsize + pBlockCol->csize + sizeof(TSCKSUM);
  // }

  // recover
  pBlockData->nRow = pSubBlock->nRow;
  p = *ppBuf1 + sizeof(SBlockDataHdr);

  code = tsdbRecoverBlockDataKey(pBlockData, pSubBlock, p, ppBuf2);
  if (code) goto _err;
  p = p + pSubBlock->vsize + pSubBlock->ksize + sizeof(TSCKSUM);

  for (int32_t iBlockCol = 0; iBlockCol < pSubBlock->mBlockCol.nItem; iBlockCol++) {
    SColData *pColData;

    tMapDataGetItemByIdx(&pSubBlock->mBlockCol, iBlockCol, pBlockCol, tGetBlockCol);
    ASSERT(pBlockCol->flag && pBlockCol->flag != HAS_NONE);

    code = tBlockDataAddColData(pBlockData, iBlockCol, &pColData);
    if (code) goto _err;

    tColDataReset(pColData, pBlockCol->cid, pBlockCol->type);
    if (pBlockCol->flag == HAS_NULL) {
      for (int32_t iRow = 0; iRow < pSubBlock->nRow; iRow++) {
        code = tColDataAppendValue(pColData, &COL_VAL_NULL(pBlockCol->cid, pBlockCol->type));
        if (code) goto _err;
      }
    } else {
      code = tsdbRecoverColData(pBlockData, pSubBlock, pBlockCol, pColData, p, ppBuf2);
      if (code) goto _err;

      p = p + pBlockCol->bsize + pBlockCol->csize + sizeof(TSCKSUM);
    }
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb read sub block data failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadBlockData(SDataFReader *pReader, SBlockIdx *pBlockIdx, SBlock *pBlock, SBlockData *pBlockData,
                          uint8_t **ppBuf1, uint8_t **ppBuf2) {
  int32_t   code = 0;
  TdFilePtr pFD = pBlock->last ? pReader->pLastFD : pReader->pDataFD;
  uint8_t  *pBuf1 = NULL;
  uint8_t  *pBuf2 = NULL;
  int32_t   iSubBlock;

  if (!ppBuf1) ppBuf1 = &pBuf1;
  if (!ppBuf2) ppBuf2 = &pBuf2;

  // read the first sub-block
  iSubBlock = 0;
  code = tsdbReadSubBlockData(pReader, pBlockIdx, pBlock, iSubBlock, pBlockData, ppBuf1, ppBuf2);
  if (code) goto _err;

  // read remain block data and do merg
  if (pBlock->nSubBlock > 1) {
    SBlockData *pBlockData1 = &(SBlockData){0};
    SBlockData *pBlockData2 = &(SBlockData){0};

    for (iSubBlock = 1; iSubBlock < pBlock->nSubBlock; iSubBlock++) {
      code = tsdbReadSubBlockData(pReader, pBlockIdx, pBlock, iSubBlock, pBlockData, ppBuf1, ppBuf2);
      if (code) {
        tBlockDataClear(pBlockData1);
        tBlockDataClear(pBlockData2);
        goto _err;
      }

      code = tBlockDataCopy(pBlockData, pBlockData2);
      if (code) {
        tBlockDataClear(pBlockData1);
        tBlockDataClear(pBlockData2);
        goto _err;
      }

      // merge two block data
      code = tBlockDataMerge(pBlockData1, pBlockData2, pBlockData);
      if (code) {
        tBlockDataClear(pBlockData1);
        tBlockDataClear(pBlockData2);
        goto _err;
      }
    }

    tBlockDataClear(pBlockData1);
    tBlockDataClear(pBlockData2);
  }

  ASSERT(pBlock->nRow == pBlockData->nRow);
  ASSERT(tsdbKeyCmprFn(&pBlock->minKey, &TSDBROW_KEY(&tBlockDataFirstRow(pBlockData))) == 0);
  ASSERT(tsdbKeyCmprFn(&pBlock->maxKey, &TSDBROW_KEY(&tBlockDataLastRow(pBlockData))) == 0);

  if (pBuf1) tsdbFree(pBuf1);
  if (pBuf2) tsdbFree(pBuf2);
  return code;

_err:
  tsdbError("vgId:%d tsdb read block data failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  if (pBuf1) tsdbFree(pBuf1);
  if (pBuf2) tsdbFree(pBuf2);
  return code;
}

int32_t tsdbReadBlockSMA(SDataFReader *pReader, SBlockSMA *pBlkSMA) {
  int32_t code = 0;
  // TODO
  return code;
}

// SDataFWriter ====================================================
struct SDataFWriter {
  STsdb    *pTsdb;
  SDFileSet wSet;
  TdFilePtr pHeadFD;
  TdFilePtr pDataFD;
  TdFilePtr pLastFD;
  TdFilePtr pSmaFD;
};

SDFileSet *tsdbDataFWriterGetWSet(SDataFWriter *pWriter) { return &pWriter->wSet; }

int32_t tsdbDataFWriterOpen(SDataFWriter **ppWriter, STsdb *pTsdb, SDFileSet *pSet) {
  int32_t       code = 0;
  int32_t       flag;
  int64_t       n;
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
  pWriter->wSet = *pSet;
  pSet = &pWriter->wSet;

  // head
  flag = TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  tsdbDataFileName(pTsdb, pSet, TSDB_HEAD_FILE, fname);
  pWriter->pHeadFD = taosOpenFile(fname, flag);
  if (pWriter->pHeadFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  n = taosWriteFile(pWriter->pHeadFD, hdr, TSDB_FHDR_SIZE);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  ASSERT(n == TSDB_FHDR_SIZE);

  pSet->fHead.size += TSDB_FHDR_SIZE;

  // data
  if (pSet->fData.size == 0) {
    flag = TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  } else {
    flag = TD_FILE_WRITE;
  }
  tsdbDataFileName(pTsdb, pSet, TSDB_DATA_FILE, fname);
  pWriter->pDataFD = taosOpenFile(fname, flag);
  if (pWriter->pDataFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  if (pSet->fData.size == 0) {
    n = taosWriteFile(pWriter->pDataFD, hdr, TSDB_FHDR_SIZE);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    pSet->fData.size += TSDB_FHDR_SIZE;
  } else {
    n = taosLSeekFile(pWriter->pDataFD, 0, SEEK_END);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    ASSERT(n == pSet->fData.size);
  }

  // last
  if (pSet->fLast.size == 0) {
    flag = TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  } else {
    flag = TD_FILE_WRITE;
  }
  tsdbDataFileName(pTsdb, pSet, TSDB_LAST_FILE, fname);
  pWriter->pLastFD = taosOpenFile(fname, flag);
  if (pWriter->pLastFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  if (pSet->fLast.size == 0) {
    n = taosWriteFile(pWriter->pLastFD, hdr, TSDB_FHDR_SIZE);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    pSet->fLast.size += TSDB_FHDR_SIZE;
  } else {
    n = taosLSeekFile(pWriter->pLastFD, 0, SEEK_END);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    ASSERT(n == pSet->fLast.size);
  }

  // sma
  if (pSet->fSma.size == 0) {
    flag = TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  } else {
    flag = TD_FILE_WRITE;
  }
  tsdbDataFileName(pTsdb, pSet, TSDB_SMA_FILE, fname);
  pWriter->pSmaFD = taosOpenFile(fname, flag);
  if (pWriter->pSmaFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  if (pSet->fSma.size == 0) {
    n = taosWriteFile(pWriter->pSmaFD, hdr, TSDB_FHDR_SIZE);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    pSet->fSma.size += TSDB_FHDR_SIZE;
  } else {
    n = taosLSeekFile(pWriter->pSmaFD, 0, SEEK_END);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    ASSERT(n == pSet->fSma.size);
  }

  *ppWriter = pWriter;
  return code;

_err:
  tsdbError("vgId:%d tsdb data file writer open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

int32_t tsdbDataFWriterClose(SDataFWriter **ppWriter, int8_t sync) {
  int32_t code = 0;
  STsdb  *pTsdb = (*ppWriter)->pTsdb;

  if (sync) {
    if (taosFsyncFile((*ppWriter)->pHeadFD) < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (taosFsyncFile((*ppWriter)->pDataFD) < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (taosFsyncFile((*ppWriter)->pLastFD) < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (taosFsyncFile((*ppWriter)->pSmaFD) < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
  }

  if (taosCloseFile(&(*ppWriter)->pHeadFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosCloseFile(&(*ppWriter)->pDataFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosCloseFile(&(*ppWriter)->pLastFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosCloseFile(&(*ppWriter)->pSmaFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  taosMemoryFree(*ppWriter);
  *ppWriter = NULL;
  return code;

_err:
  tsdbError("vgId:%d data file writer close failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbUpdateDFileSetHeader(SDataFWriter *pWriter) {
  int32_t    code = 0;
  int64_t    size = TSDB_FHDR_SIZE;
  int64_t    n;
  uint8_t    hdr[TSDB_FHDR_SIZE];
  SHeadFile *pHeadFile = &pWriter->wSet.fHead;
  SDataFile *pDataFile = &pWriter->wSet.fData;
  SLastFile *pLastFile = &pWriter->wSet.fLast;
  SSmaFile  *pSmaFile = &pWriter->wSet.fSma;

  // head ==============
  code = tsdbUpdateDFileHdr(pWriter->pHeadFD, &pWriter->wSet, TSDB_HEAD_FILE);
  if (code) goto _err;

  // data ==============
  code = tsdbUpdateDFileHdr(pWriter->pHeadFD, &pWriter->wSet, TSDB_DATA_FILE);
  if (code) goto _err;

  // last ==============
  code = tsdbUpdateDFileHdr(pWriter->pHeadFD, &pWriter->wSet, TSDB_LAST_FILE);
  if (code) goto _err;

  // sma ==============
  code = tsdbUpdateDFileHdr(pWriter->pHeadFD, &pWriter->wSet, TSDB_SMA_FILE);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d update DFileSet header failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteBlockIdx(SDataFWriter *pWriter, SMapData *mBlockIdx, uint8_t **ppBuf) {
  int32_t    code = 0;
  int64_t    size;
  SHeadFile *pHeadFile = &pWriter->wSet.fHead;
  int64_t    n;
  uint8_t   *pBuf = NULL;

  // prepare
  size = 0;
  size += tPutU32(NULL, TSDB_FILE_DLMT);
  size = size + tPutMapData(NULL, mBlockIdx) + sizeof(TSCKSUM);

  // alloc
  if (!ppBuf) ppBuf = &pBuf;
  code = tsdbRealloc(ppBuf, size);
  if (code) goto _err;

  // build
  n = 0;
  n += tPutU32(*ppBuf + n, TSDB_FILE_DLMT);
  n += tPutMapData(*ppBuf + n, mBlockIdx);
  taosCalcChecksumAppend(0, *ppBuf, size);

  ASSERT(n + sizeof(TSCKSUM) == size);

  // write
  n = taosWriteFile(pWriter->pHeadFD, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // update
  pHeadFile->offset = pHeadFile->size;
  pHeadFile->size += size;

  tsdbFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d write block idx failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  tsdbFree(pBuf);
  return code;
}

int32_t tsdbWriteBlock(SDataFWriter *pWriter, SMapData *mBlock, uint8_t **ppBuf, SBlockIdx *pBlockIdx) {
  int32_t       code = 0;
  SHeadFile    *pHeadFile = &pWriter->wSet.fHead;
  SBlockDataHdr hdr = {.delimiter = TSDB_FILE_DLMT, .suid = pBlockIdx->suid, .uid = pBlockIdx->uid};
  uint8_t      *pBuf = NULL;
  int64_t       size;
  int64_t       n;

  ASSERT(mBlock->nItem > 0);

  // prepare
  size = sizeof(SBlockDataHdr) + tPutMapData(NULL, mBlock) + sizeof(TSCKSUM);

  // alloc
  if (!ppBuf) ppBuf = &pBuf;
  code = tsdbRealloc(ppBuf, size);
  if (code) goto _err;

  // build
  n = 0;
  *(SBlockDataHdr *)(*ppBuf) = hdr;
  n += sizeof(hdr);
  n += tPutMapData(*ppBuf + n, mBlock);
  taosCalcChecksumAppend(0, *ppBuf, size);

  ASSERT(n + sizeof(TSCKSUM) == size);

  // write
  n = taosWriteFile(pWriter->pHeadFD, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // update
  pBlockIdx->offset = pHeadFile->size;
  pBlockIdx->size = size;
  pHeadFile->size += size;

  tsdbFree(pBuf);
  tsdbTrace("vgId:%d write block, offset:%" PRId64 " size:%" PRId64, TD_VID(pWriter->pTsdb->pVnode), pBlockIdx->offset,
            pBlockIdx->size);
  return code;

_err:
  tsdbFree(pBuf);
  tsdbError("vgId:%d write block failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteBlockData(SDataFWriter *pWriter, SBlockData *pBlockData, uint8_t **ppBuf1, uint8_t **ppBuf2,
                           SBlockIdx *pBlockIdx, SBlock *pBlock, int8_t cmprAlg) {
  int32_t       code = 0;
  SSubBlock    *pSubBlock = &pBlock->aSubBlock[pBlock->nSubBlock++];
  SBlockCol    *pBlockCol = &(SBlockCol){0};
  int64_t       size;
  int64_t       n;
  TdFilePtr     pFileFD = pBlock->last ? pWriter->pLastFD : pWriter->pDataFD;
  SBlockDataHdr hdr = {.delimiter = TSDB_FILE_DLMT, .suid = pBlockIdx->suid, .uid = pBlockIdx->uid};
  TSCKSUM       cksm;
  uint8_t      *p;
  int64_t       offset;
  uint8_t      *pBuf1 = NULL;
  uint8_t      *pBuf2 = NULL;

  if (!ppBuf1) ppBuf1 = &pBuf1;
  if (!ppBuf2) ppBuf2 = &pBuf2;

  TSKEY lastKey = TSKEY_MIN;
  for (int32_t iRow = 0; iRow < pBlockData->nRow; iRow++) {
    TSDBKEY key = TSDBROW_KEY(&tsdbRowFromBlockData(pBlockData, iRow));
    if (iRow == 0) {
      pBlock->minKey = MIN_TSDBKEY(pBlock->minKey, key);
    }

    if (iRow == pBlockData->nRow - 1) {
      pBlock->maxKey = MAX_TSDBKEY(pBlock->maxKey, key);
    }

    pBlock->minVersion = TMIN(pBlock->minVersion, key.version);
    pBlock->maxVersion = TMAX(pBlock->maxVersion, key.version);
    if (key.ts == lastKey) {
      pBlock->hasDup = 1;
    }
    lastKey = key.ts;
  }
  pBlock->nRow += pBlockData->nRow;

  pSubBlock->nRow = pBlockData->nRow;
  pSubBlock->cmprAlg = cmprAlg;
  if (pBlock->last) {
    pSubBlock->offset = pWriter->wSet.fLast.size;
  } else {
    pSubBlock->offset = pWriter->wSet.fData.size;
  }
  pSubBlock->bsize = 0;

  // HDR
  n = taosWriteFile(pFileFD, &hdr, sizeof(hdr));
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  pSubBlock->bsize += n;

  // TSDBKEY
  if (cmprAlg == NO_COMPRESSION) {
    cksm = 0;

    // version
    pSubBlock->vsize = sizeof(int64_t) * pBlockData->nRow;
    n = taosWriteFile(pFileFD, pBlockData->aVersion, pSubBlock->vsize);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    cksm = taosCalcChecksum(cksm, (uint8_t *)pBlockData->aVersion, pSubBlock->vsize);

    // TSKEY
    pSubBlock->ksize = sizeof(TSKEY) * pBlockData->nRow;
    n = taosWriteFile(pFileFD, pBlockData->aTSKEY, pSubBlock->ksize);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    cksm = taosCalcChecksum(cksm, (uint8_t *)pBlockData->aTSKEY, pSubBlock->ksize);

    // cksm
    size = sizeof(cksm);
    n = taosWriteFile(pFileFD, (uint8_t *)&cksm, size);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
  } else {
    ASSERT(cmprAlg == ONE_STAGE_COMP || cmprAlg == TWO_STAGE_COMP);

    size = (sizeof(int64_t) + sizeof(TSKEY)) * pBlockData->nRow + COMP_OVERFLOW_BYTES * 2 + sizeof(TSCKSUM);

    code = tsdbRealloc(ppBuf1, size);
    if (code) goto _err;

    if (cmprAlg == TWO_STAGE_COMP) {
      code = tsdbRealloc(ppBuf2, size);
      if (code) goto _err;
    }

    // version
    n = tsCompressBigint((char *)pBlockData->aVersion, sizeof(int64_t) * pBlockData->nRow, pBlockData->nRow, *ppBuf1,
                         size, cmprAlg, *ppBuf2, size);
    if (n <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }
    pSubBlock->vsize = n;

    // TSKEY
    n = tsCompressTimestamp((char *)pBlockData->aTSKEY, sizeof(TSKEY) * pBlockData->nRow, pBlockData->nRow,
                            *ppBuf1 + pSubBlock->vsize, size - pSubBlock->vsize, cmprAlg, *ppBuf2, size);
    if (n <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }
    pSubBlock->ksize = n;

    // cksm
    n = pSubBlock->vsize + pSubBlock->ksize + sizeof(TSCKSUM);
    ASSERT(n <= size);
    taosCalcChecksumAppend(0, *ppBuf1, n);

    // write
    n = taosWriteFile(pFileFD, *ppBuf1, n);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
  }
  pSubBlock->bsize += (pSubBlock->vsize + pSubBlock->ksize + sizeof(TSCKSUM));

  // other columns
  offset = 0;
  tMapDataReset(&pSubBlock->mBlockCol);
  for (int32_t iCol = 0; iCol < taosArrayGetSize(pBlockData->aColDataP); iCol++) {
    SColData *pColData = (SColData *)taosArrayGetP(pBlockData->aColDataP, iCol);

    ASSERT(pColData->flag);

    if (pColData->flag == HAS_NONE) continue;

    pBlockCol->cid = pColData->cid;
    pBlockCol->type = pColData->type;
    pBlockCol->flag = pColData->flag;

    if (pColData->flag != HAS_NULL) {
      cksm = 0;
      pBlockCol->offset = offset;

      // bitmap
      if (pColData->flag == HAS_VALUE) {
        pBlockCol->bsize = 0;
      } else {
        pBlockCol->bsize = BIT2_SIZE(pBlockData->nRow);
        n = taosWriteFile(pFileFD, pColData->pBitMap, pBlockCol->bsize);
        if (n < 0) {
          code = TAOS_SYSTEM_ERROR(errno);
          goto _err;
        }
        cksm = taosCalcChecksum(cksm, pColData->pBitMap, n);
      }

      // data
      if (cmprAlg == NO_COMPRESSION) {
        // data
        n = taosWriteFile(pFileFD, pColData->pData, pColData->nData);
        if (n < 0) {
          code = TAOS_SYSTEM_ERROR(errno);
          goto _err;
        }
        pBlockCol->csize = n;
        pBlockCol->osize = n;

        // checksum
        cksm = taosCalcChecksum(cksm, pColData->pData, pColData->nData);
        n = taosWriteFile(pFileFD, &cksm, sizeof(cksm));
        if (n < 0) {
          code = TAOS_SYSTEM_ERROR(errno);
          goto _err;
        }
      } else {
        size = pColData->nData + COMP_OVERFLOW_BYTES + sizeof(TSCKSUM);

        code = tsdbRealloc(ppBuf1, size);
        if (code) goto _err;

        if (cmprAlg == TWO_STAGE_COMP) {
          code = tsdbRealloc(ppBuf2, size);
          if (code) goto _err;
        }

        // data
        n = tDataTypes[pColData->type].compFunc(pColData->pData, pColData->nData, pBlockData->nRow, *ppBuf1, size,
                                                cmprAlg, *ppBuf2, size);
        if (n <= 0) {
          code = TSDB_CODE_COMPRESS_ERROR;
          goto _err;
        }
        pBlockCol->csize = n;
        pBlockCol->osize = pColData->nData;

        // cksm
        n += sizeof(TSCKSUM);
        ASSERT(n <= size);
        taosCalcChecksumAppend(cksm, *ppBuf1, n);

        // write
        n = taosWriteFile(pFileFD, *ppBuf1, n);
        if (n < 0) {
          code = TAOS_SYSTEM_ERROR(errno);
          goto _err;
        }
      }

      // state
      offset = offset + pBlockCol->bsize + pBlockCol->csize + sizeof(TSCKSUM);
      pSubBlock->bsize = pSubBlock->bsize + pBlockCol->bsize + pBlockCol->csize + sizeof(TSCKSUM);
    }

    code = tMapDataPutItem(&pSubBlock->mBlockCol, pBlockCol, tPutBlockCol);
    if (code) goto _err;
  }

  if (pBlock->last) {
    pWriter->wSet.fLast.size += pSubBlock->bsize;
  } else {
    pWriter->wSet.fData.size += pSubBlock->bsize;
  }

  tsdbFree(pBuf1);
  tsdbFree(pBuf2);
  return code;

_err:
  tsdbError("vgId:%d write block data failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  tsdbFree(pBuf1);
  tsdbFree(pBuf2);
  return code;
}

int32_t tsdbWriteBlockSMA(SDataFWriter *pWriter, SBlockSMA *pBlockSMA, int64_t *rOffset, int64_t *rSize) {
  int32_t code = 0;
  // TODO
  return code;
}
