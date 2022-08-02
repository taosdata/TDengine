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
  pDelFWriter->fDel.offset = 0;

  *ppWriter = pDelFWriter;
  return code;

_err:
  tsdbError("vgId:%d, failed to open del file writer since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

int32_t tsdbDelFWriterClose(SDelFWriter **ppWriter, int8_t sync) {
  int32_t      code = 0;
  SDelFWriter *pWriter = *ppWriter;

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

  *ppWriter = NULL;
  return code;

_err:
  tsdbError("vgId:%d, failed to close del file writer since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
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
  code = tRealloc(ppBuf, size);
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

  tFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d, failed to write del data since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  tFree(pBuf);
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
  code = tRealloc(ppBuf, size);
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

  tFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d, write del idx failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  tFree(pBuf);
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
  tsdbError("vgId:%d, update del file hdr failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
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
  if (pDelFReader->pReadH == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    taosMemoryFree(pDelFReader);
    goto _err;
  }

#if 0
  // load and check hdr if buffer is given
  if (ppBuf) {
    code = tRealloc(ppBuf, TSDB_FHDR_SIZE);
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
  tsdbError("vgId:%d, del file reader open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t tsdbDelFReaderClose(SDelFReader **ppReader) {
  int32_t      code = 0;
  SDelFReader *pReader = *ppReader;

  if (pReader) {
    if (taosCloseFile(&pReader->pReadH) < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _exit;
    }
    taosMemoryFree(pReader);
  }
  *ppReader = NULL;

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
  code = tRealloc(ppBuf, size);
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
  taosArrayClear(aDelData);
  while (n < size - sizeof(TSCKSUM)) {
    n += tGetDelData(*ppBuf + n, pDelData);

    if (taosArrayPush(aDelData, pDelData) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  ASSERT(n == size - sizeof(TSCKSUM));

  tFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d, read del data failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  tFree(pBuf);
  return code;
}

int32_t tsdbReadDelIdx(SDelFReader *pReader, SArray *aDelIdx, uint8_t **ppBuf) {
  int32_t  code = 0;
  int32_t  n;
  int64_t  offset = pReader->fDel.offset;
  int64_t  size = pReader->fDel.size - offset;
  uint32_t delimiter;
  uint8_t *pBuf = NULL;
  SDelIdx *pDelIdx = &(SDelIdx){0};

  if (!ppBuf) ppBuf = &pBuf;

  // seek
  if (taosLSeekFile(pReader->pReadH, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // alloc
  code = tRealloc(ppBuf, size);
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

  tFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d, read del idx failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  tFree(pBuf);
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
  tsdbHeadFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pHeadF, fname);
  pReader->pHeadFD = taosOpenFile(fname, TD_FILE_READ);
  if (pReader->pHeadFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // data
  tsdbDataFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pDataF, fname);
  pReader->pDataFD = taosOpenFile(fname, TD_FILE_READ);
  if (pReader->pDataFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // last
  tsdbLastFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pLastF, fname);
  pReader->pLastFD = taosOpenFile(fname, TD_FILE_READ);
  if (pReader->pLastFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // sma
  tsdbSmaFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pSmaF, fname);
  pReader->pSmaFD = taosOpenFile(fname, TD_FILE_READ);
  if (pReader->pSmaFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  *ppReader = pReader;
  return code;

_err:
  tsdbError("vgId:%d, tsdb data file reader open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t tsdbDataFReaderClose(SDataFReader **ppReader) {
  int32_t code = 0;
  if (*ppReader == NULL) goto _exit;

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

_exit:
  *ppReader = NULL;
  return code;

_err:
  tsdbError("vgId:%d, data file reader close failed since %s", TD_VID((*ppReader)->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadBlockIdx(SDataFReader *pReader, SArray *aBlockIdx, uint8_t **ppBuf) {
  int32_t   code = 0;
  int64_t   offset = pReader->pSet->pHeadF->offset;
  int64_t   size = pReader->pSet->pHeadF->size - offset;
  uint8_t  *pBuf = NULL;
  int64_t   n;
  uint32_t  delimiter;
  SBlockIdx blockIdx;

  if (!ppBuf) ppBuf = &pBuf;

  // alloc
  code = tRealloc(ppBuf, size);
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
  n = tGetU32(*ppBuf + n, &delimiter);
  ASSERT(delimiter == TSDB_FILE_DLMT);

  taosArrayClear(aBlockIdx);
  while (n < size - sizeof(TSCKSUM)) {
    n += tGetBlockIdx(*ppBuf + n, &blockIdx);

    if (taosArrayPush(aBlockIdx, &blockIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  ASSERT(n + sizeof(TSCKSUM) == size);

  tFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d, read block idx failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  tFree(pBuf);
  return code;
}

int32_t tsdbReadBlockL(SDataFReader *pReader, SArray *aBlockL, uint8_t **ppBuf) {
  int32_t  code = 0;
  int64_t  offset = pReader->pSet->pHeadF->loffset;
  int64_t  size = pReader->pSet->pHeadF->offset - offset;
  int64_t  n;
  uint32_t delimiter;
  uint8_t *pBuf = NULL;
  SBlockL  blockl;

  if (!ppBuf) ppBuf = &pBuf;

  // alloc
  code = tRealloc(ppBuf, size);
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
  n = tGetU32(*ppBuf + n, &delimiter);
  ASSERT(delimiter == TSDB_FILE_DLMT);

  taosArrayClear(aBlockL);
  while (n < size - sizeof(TSCKSUM)) {
    n += tGetBlockL(*ppBuf + n, &blockl);

    if (taosArrayPush(aBlockL, &blockl) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  ASSERT(n + sizeof(TSCKSUM) == size);

  tFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d read blockl failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadBlock(SDataFReader *pReader, SBlockIdx *pBlockIdx, SMapData *mBlock, uint8_t **ppBuf) {
  int32_t       code = 0;
  int64_t       offset = pBlockIdx->offset;
  int64_t       size = pBlockIdx->size;
  uint8_t      *pBuf = NULL;
  int64_t       n;
  int64_t       tn;
  SBlockDataHdr hdr;

  if (!ppBuf) ppBuf = &pBuf;

  // alloc
  code = tRealloc(ppBuf, size);
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
  tn = tGetMapData(*ppBuf + n, mBlock);
  if (tn < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  n += tn;
  ASSERT(n + sizeof(TSCKSUM) == size);

  tFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d, read block failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  tFree(pBuf);
  return code;
}

static int32_t tsdbReadBlockDataKey(SBlockData *pBlockData, SSubBlock *pSubBlock, uint8_t *pBuf, uint8_t **ppBuf) {
  int32_t code = 0;
  int64_t size = pSubBlock->szVersion + pSubBlock->szTSKEY + sizeof(TSCKSUM);
  int64_t n;

  if (!taosCheckChecksumWhole(pBuf, size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  code = tRealloc((uint8_t **)&pBlockData->aVersion, sizeof(int64_t) * pSubBlock->nRow);
  if (code) goto _err;
  code = tRealloc((uint8_t **)&pBlockData->aTSKEY, sizeof(TSKEY) * pSubBlock->nRow);
  if (code) goto _err;

  if (pSubBlock->cmprAlg == NO_COMPRESSION) {
    ASSERT(pSubBlock->szVersion == sizeof(int64_t) * pSubBlock->nRow);
    ASSERT(pSubBlock->szTSKEY == sizeof(TSKEY) * pSubBlock->nRow);

    // VERSION
    memcpy(pBlockData->aVersion, pBuf, pSubBlock->szVersion);

    // TSKEY
    memcpy(pBlockData->aTSKEY, pBuf + pSubBlock->szVersion, pSubBlock->szTSKEY);
  } else {
    size = sizeof(int64_t) * pSubBlock->nRow + COMP_OVERFLOW_BYTES;
    if (pSubBlock->cmprAlg == TWO_STAGE_COMP) {
      code = tRealloc(ppBuf, size);
      if (code) goto _err;
    }

    // VERSION
    n = tsDecompressBigint(pBuf, pSubBlock->szVersion, pSubBlock->nRow, (char *)pBlockData->aVersion,
                           sizeof(int64_t) * pSubBlock->nRow, pSubBlock->cmprAlg, *ppBuf, size);
    if (n < 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }

    // TSKEY
    n = tsDecompressTimestamp(pBuf + pSubBlock->szVersion, pSubBlock->szTSKEY, pSubBlock->nRow,
                              (char *)pBlockData->aTSKEY, sizeof(TSKEY) * pSubBlock->nRow, pSubBlock->cmprAlg, *ppBuf,
                              size);
    if (n < 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }
  }

  return code;

_err:
  return code;
}

static int32_t tsdbReadColDataImpl(SSubBlock *pSubBlock, SBlockCol *pBlockCol, SColData *pColData, uint8_t *pBuf,
                                   uint8_t **ppBuf) {
  int32_t code = 0;
  int64_t size;
  int64_t n;

  if (!taosCheckChecksumWhole(pBuf, pBlockCol->szBitmap + pBlockCol->szOffset + pBlockCol->szValue + sizeof(TSCKSUM))) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  pColData->nVal = pSubBlock->nRow;
  pColData->flag = pBlockCol->flag;

  // BITMAP
  if (pBlockCol->flag != HAS_VALUE) {
    ASSERT(pBlockCol->szBitmap);

    size = BIT2_SIZE(pColData->nVal);
    code = tRealloc(&pColData->pBitMap, size);
    if (code) goto _err;

    code = tRealloc(ppBuf, size + COMP_OVERFLOW_BYTES);
    if (code) goto _err;

    n = tsDecompressTinyint(pBuf, pBlockCol->szBitmap, size, pColData->pBitMap, size, TWO_STAGE_COMP, *ppBuf,
                            size + COMP_OVERFLOW_BYTES);
    if (n <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }

    ASSERT(n == size);
  } else {
    ASSERT(pBlockCol->szBitmap == 0);
  }
  pBuf = pBuf + pBlockCol->szBitmap;

  // OFFSET
  if (IS_VAR_DATA_TYPE(pColData->type)) {
    ASSERT(pBlockCol->szOffset);

    size = sizeof(int32_t) * pColData->nVal;
    code = tRealloc((uint8_t **)&pColData->aOffset, size);
    if (code) goto _err;

    code = tRealloc(ppBuf, size + COMP_OVERFLOW_BYTES);
    if (code) goto _err;

    n = tsDecompressInt(pBuf, pBlockCol->szOffset, pColData->nVal, (char *)pColData->aOffset, size, TWO_STAGE_COMP,
                        *ppBuf, size + COMP_OVERFLOW_BYTES);
    if (n <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }

    ASSERT(n == size);
  } else {
    ASSERT(pBlockCol->szOffset == 0);
  }
  pBuf = pBuf + pBlockCol->szOffset;

  // VALUE
  pColData->nData = pBlockCol->szOrigin;

  code = tRealloc(&pColData->pData, pColData->nData);
  if (code) goto _err;

  if (pSubBlock->cmprAlg == NO_COMPRESSION) {
    memcpy(pColData->pData, pBuf, pColData->nData);
  } else {
    if (pSubBlock->cmprAlg == TWO_STAGE_COMP) {
      code = tRealloc(ppBuf, pColData->nData + COMP_OVERFLOW_BYTES);
      if (code) goto _err;
    }

    n = tDataTypes[pBlockCol->type].decompFunc(pBuf, pBlockCol->szValue, pSubBlock->nRow, pColData->pData,
                                               pColData->nData, pSubBlock->cmprAlg, *ppBuf,
                                               pColData->nData + COMP_OVERFLOW_BYTES);
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

static int32_t tsdbReadBlockCol(SSubBlock *pSubBlock, uint8_t *p, SArray *aBlockCol) {
  int32_t    code = 0;
  int32_t    n = 0;
  SBlockCol  blockCol;
  SBlockCol *pBlockCol = &blockCol;

  if (!taosCheckChecksumWhole(p, pSubBlock->szBlockCol + sizeof(TSCKSUM))) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  n += sizeof(SBlockDataHdr);
  while (n < pSubBlock->szBlockCol) {
    n += tGetBlockCol(p + n, pBlockCol);

    if (taosArrayPush(aBlockCol, pBlockCol) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  ASSERT(n == pSubBlock->szBlockCol);

  return code;

_err:
  return code;
}

static int32_t tsdbReadSubColData(SDataFReader *pReader, SBlockIdx *pBlockIdx, SBlock *pBlock, int32_t iSubBlock,
                                  int16_t *aColId, int32_t nCol, SBlockData *pBlockData, uint8_t **ppBuf1,
                                  uint8_t **ppBuf2) {
  TdFilePtr  pFD = pBlock->last ? pReader->pLastFD : pReader->pDataFD;
  SSubBlock *pSubBlock = &pBlock->aSubBlock[iSubBlock];
  SArray    *aBlockCol = NULL;
  int32_t    code = 0;
  int64_t    offset;
  int64_t    size;
  int64_t    n;

  tBlockDataReset(pBlockData);
  pBlockData->nRow = pSubBlock->nRow;

  // TSDBKEY and SBlockCol
  if (nCol == 1) {
    offset = pSubBlock->offset + pSubBlock->szBlockCol + sizeof(TSCKSUM);
    size = pSubBlock->szVersion + pSubBlock->szTSKEY + sizeof(TSCKSUM);
  } else {
    offset = pSubBlock->offset;
    size = pSubBlock->szBlockCol + sizeof(TSCKSUM) + pSubBlock->szVersion + pSubBlock->szTSKEY + sizeof(TSCKSUM);
  }

  code = tRealloc(ppBuf1, size);
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

  if (nCol == 1) {
    code = tsdbReadBlockDataKey(pBlockData, pSubBlock, *ppBuf1, ppBuf2);
    if (code) goto _err;

    goto _exit;
  } else {
    aBlockCol = taosArrayInit(0, sizeof(SBlockCol));
    if (aBlockCol == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }

    code = tsdbReadBlockCol(pSubBlock, *ppBuf1, aBlockCol);
    if (code) goto _err;

    code = tsdbReadBlockDataKey(pBlockData, pSubBlock, *ppBuf1 + pSubBlock->szBlockCol + sizeof(TSCKSUM), ppBuf2);
    if (code) goto _err;
  }

  for (int32_t iCol = 1; iCol < nCol; iCol++) {
    void *p = taosArraySearch(aBlockCol, &(SBlockCol){.cid = aColId[iCol]}, tBlockColCmprFn, TD_EQ);

    if (p) {
      SBlockCol *pBlockCol = (SBlockCol *)p;
      SColData  *pColData;

      ASSERT(pBlockCol->flag && pBlockCol->flag != HAS_NONE);

      code = tBlockDataAddColData(pBlockData, taosArrayGetSize(pBlockData->aIdx), &pColData);
      if (code) goto _err;

      tColDataInit(pColData, pBlockCol->cid, pBlockCol->type, pBlockCol->smaOn);
      if (pBlockCol->flag == HAS_NULL) {
        for (int32_t iRow = 0; iRow < pSubBlock->nRow; iRow++) {
          code = tColDataAppendValue(pColData, &COL_VAL_NULL(pBlockCol->cid, pBlockCol->type));
          if (code) goto _err;
        }
      } else {
        offset = pSubBlock->offset + pSubBlock->szBlockCol + sizeof(TSCKSUM) + pSubBlock->szVersion +
                 pSubBlock->szTSKEY + sizeof(TSCKSUM) + pBlockCol->offset;
        size = pBlockCol->szBitmap + pBlockCol->szOffset + pBlockCol->szValue + sizeof(TSCKSUM);

        code = tRealloc(ppBuf1, size);
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

        code = tsdbReadColDataImpl(pSubBlock, pBlockCol, pColData, *ppBuf1, ppBuf2);
        if (code) goto _err;
      }
    }
  }

_exit:
  taosArrayDestroy(aBlockCol);
  return code;

_err:
  taosArrayDestroy(aBlockCol);
  return code;
}

int32_t tsdbReadColData(SDataFReader *pReader, SBlockIdx *pBlockIdx, SBlock *pBlock, int16_t *aColId, int32_t nCol,
                        SBlockData *pBlockData, uint8_t **ppBuf1, uint8_t **ppBuf2) {
  int32_t  code = 0;
  uint8_t *pBuf1 = NULL;
  uint8_t *pBuf2 = NULL;

  ASSERT(aColId[0] == PRIMARYKEY_TIMESTAMP_COL_ID);

  if (!ppBuf1) ppBuf1 = &pBuf1;
  if (!ppBuf2) ppBuf2 = &pBuf2;

  code = tsdbReadSubColData(pReader, pBlockIdx, pBlock, 0, aColId, nCol, pBlockData, ppBuf1, ppBuf2);
  if (code) goto _err;

  if (pBlock->nSubBlock > 1) {
    SBlockData *pBlockData1 = &(SBlockData){0};
    SBlockData *pBlockData2 = &(SBlockData){0};

    tBlockDataInit(pBlockData1);
    tBlockDataInit(pBlockData2);
    for (int32_t iSubBlock = 1; iSubBlock < pBlock->nSubBlock; iSubBlock++) {
      code = tsdbReadSubColData(pReader, pBlockIdx, pBlock, iSubBlock, aColId, nCol, pBlockData1, ppBuf1, ppBuf2);
      if (code) goto _err;

      code = tBlockDataCopy(pBlockData, pBlockData2);
      if (code) {
        tBlockDataClear(pBlockData1, 1);
        tBlockDataClear(pBlockData2, 1);
        goto _err;
      }

      code = tBlockDataMerge(pBlockData1, pBlockData2, pBlockData);
      if (code) {
        tBlockDataClear(pBlockData1, 1);
        tBlockDataClear(pBlockData2, 1);
        goto _err;
      }
    }

    tBlockDataClear(pBlockData1, 1);
    tBlockDataClear(pBlockData2, 1);
  }

  tFree(pBuf1);
  tFree(pBuf2);
  return code;

_err:
  tsdbError("vgId:%d, tsdb read col data failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  tFree(pBuf1);
  tFree(pBuf2);
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
  SArray    *aBlockCol = NULL;

  tBlockDataReset(pBlockData);

  // realloc
  code = tRealloc(ppBuf1, pSubBlock->szBlock);
  if (code) goto _err;

  // seek
  n = taosLSeekFile(pFD, pSubBlock->offset, SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // read
  n = taosReadFile(pFD, *ppBuf1, pSubBlock->szBlock);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < pSubBlock->szBlock) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  pBlockData->nRow = pSubBlock->nRow;

  // TSDBKEY
  p = *ppBuf1 + pSubBlock->szBlockCol + sizeof(TSCKSUM);
  code = tsdbReadBlockDataKey(pBlockData, pSubBlock, p, ppBuf2);
  if (code) goto _err;

  // COLUMNS
  aBlockCol = taosArrayInit(0, sizeof(SBlockCol));
  if (aBlockCol == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  code = tsdbReadBlockCol(pSubBlock, *ppBuf1, aBlockCol);
  if (code) goto _err;

  for (int32_t iBlockCol = 0; iBlockCol < taosArrayGetSize(aBlockCol); iBlockCol++) {
    SColData  *pColData;
    SBlockCol *pBlockCol = (SBlockCol *)taosArrayGet(aBlockCol, iBlockCol);

    ASSERT(pBlockCol->flag && pBlockCol->flag != HAS_NONE);

    code = tBlockDataAddColData(pBlockData, iBlockCol, &pColData);
    if (code) goto _err;

    tColDataInit(pColData, pBlockCol->cid, pBlockCol->type, pBlockCol->smaOn);
    if (pBlockCol->flag == HAS_NULL) {
      for (int32_t iRow = 0; iRow < pSubBlock->nRow; iRow++) {
        code = tColDataAppendValue(pColData, &COL_VAL_NULL(pBlockCol->cid, pBlockCol->type));
        if (code) goto _err;
      }
    } else {
      p = *ppBuf1 + pSubBlock->szBlockCol + sizeof(TSCKSUM) + pSubBlock->szVersion + pSubBlock->szTSKEY +
          sizeof(TSCKSUM) + pBlockCol->offset;
      code = tsdbReadColDataImpl(pSubBlock, pBlockCol, pColData, p, ppBuf2);
      if (code) goto _err;
    }
  }

  taosArrayDestroy(aBlockCol);
  return code;

_err:
  tsdbError("vgId:%d, tsdb read sub block data failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  taosArrayDestroy(aBlockCol);
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

    tBlockDataInit(pBlockData1);
    tBlockDataInit(pBlockData2);
    for (iSubBlock = 1; iSubBlock < pBlock->nSubBlock; iSubBlock++) {
      code = tsdbReadSubBlockData(pReader, pBlockIdx, pBlock, iSubBlock, pBlockData1, ppBuf1, ppBuf2);
      if (code) {
        tBlockDataClear(pBlockData1, 1);
        tBlockDataClear(pBlockData2, 1);
        goto _err;
      }

      code = tBlockDataCopy(pBlockData, pBlockData2);
      if (code) {
        tBlockDataClear(pBlockData1, 1);
        tBlockDataClear(pBlockData2, 1);
        goto _err;
      }

      // merge two block data
      code = tBlockDataMerge(pBlockData1, pBlockData2, pBlockData);
      if (code) {
        tBlockDataClear(pBlockData1, 1);
        tBlockDataClear(pBlockData2, 1);
        goto _err;
      }
    }

    tBlockDataClear(pBlockData1, 1);
    tBlockDataClear(pBlockData2, 1);
  }

  ASSERT(pBlock->nRow == pBlockData->nRow);
  ASSERT(tsdbKeyCmprFn(&pBlock->minKey, &TSDBROW_KEY(&tBlockDataFirstRow(pBlockData))) == 0);
  ASSERT(tsdbKeyCmprFn(&pBlock->maxKey, &TSDBROW_KEY(&tBlockDataLastRow(pBlockData))) == 0);

  if (pBuf1) tFree(pBuf1);
  if (pBuf2) tFree(pBuf2);
  return code;

_err:
  tsdbError("vgId:%d, tsdb read block data failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  if (pBuf1) tFree(pBuf1);
  if (pBuf2) tFree(pBuf2);
  return code;
}

int32_t tsdbReadBlockSma(SDataFReader *pReader, SBlock *pBlock, SArray *aColumnDataAgg, uint8_t **ppBuf) {
  int32_t   code = 0;
  TdFilePtr pFD = pReader->pSmaFD;
  int64_t   offset = pBlock->aSubBlock[0].sOffset;
  int64_t   size = pBlock->aSubBlock[0].nSma * sizeof(SColumnDataAgg) + sizeof(TSCKSUM);
  uint8_t  *pBuf = NULL;
  int64_t   n;

  ASSERT(tBlockHasSma(pBlock));

  if (!ppBuf) ppBuf = &pBuf;
  code = tRealloc(ppBuf, size);
  if (code) goto _err;

  // lseek
  n = taosLSeekFile(pFD, offset, SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // read
  n = taosReadFile(pFD, *ppBuf, size);
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
  taosArrayClear(aColumnDataAgg);
  for (int32_t iSma = 0; iSma < pBlock->aSubBlock[0].nSma; iSma++) {
    if (taosArrayPush(aColumnDataAgg, &((SColumnDataAgg *)(*ppBuf))[iSma]) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  tFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d, read block sma failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  tFree(pBuf);
  return code;
}

// SDataFWriter ====================================================
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
  pWriter->wSet = (SDFileSet){.diskId = pSet->diskId,
                              .fid = pSet->fid,
                              .pHeadF = &pWriter->fHead,
                              .pDataF = &pWriter->fData,
                              .pLastF = &pWriter->fLast,
                              .pSmaF = &pWriter->fSma};
  pWriter->fHead = *pSet->pHeadF;
  pWriter->fData = *pSet->pDataF;
  pWriter->fLast = *pSet->pLastF;
  pWriter->fSma = *pSet->pSmaF;

  // head
  flag = TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  tsdbHeadFileName(pTsdb, pWriter->wSet.diskId, pWriter->wSet.fid, &pWriter->fHead, fname);
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

  pWriter->fHead.size += TSDB_FHDR_SIZE;

  // data
  if (pWriter->fData.size == 0) {
    flag = TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  } else {
    flag = TD_FILE_WRITE;
  }
  tsdbDataFileName(pTsdb, pWriter->wSet.diskId, pWriter->wSet.fid, &pWriter->fData, fname);
  pWriter->pDataFD = taosOpenFile(fname, flag);
  if (pWriter->pDataFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  if (pWriter->fData.size == 0) {
    n = taosWriteFile(pWriter->pDataFD, hdr, TSDB_FHDR_SIZE);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    pWriter->fData.size += TSDB_FHDR_SIZE;
  } else {
    n = taosLSeekFile(pWriter->pDataFD, 0, SEEK_END);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    ASSERT(n == pWriter->fData.size);
  }

  // last
  if (pWriter->fLast.size == 0) {
    flag = TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  } else {
    flag = TD_FILE_WRITE;
  }
  tsdbLastFileName(pTsdb, pWriter->wSet.diskId, pWriter->wSet.fid, &pWriter->fLast, fname);
  pWriter->pLastFD = taosOpenFile(fname, flag);
  if (pWriter->pLastFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  if (pWriter->fLast.size == 0) {
    n = taosWriteFile(pWriter->pLastFD, hdr, TSDB_FHDR_SIZE);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    pWriter->fLast.size += TSDB_FHDR_SIZE;
  } else {
    n = taosLSeekFile(pWriter->pLastFD, 0, SEEK_END);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    ASSERT(n == pWriter->fLast.size);
  }

  // sma
  if (pWriter->fSma.size == 0) {
    flag = TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  } else {
    flag = TD_FILE_WRITE;
  }
  tsdbSmaFileName(pTsdb, pWriter->wSet.diskId, pWriter->wSet.fid, &pWriter->fSma, fname);
  pWriter->pSmaFD = taosOpenFile(fname, flag);
  if (pWriter->pSmaFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  if (pWriter->fSma.size == 0) {
    n = taosWriteFile(pWriter->pSmaFD, hdr, TSDB_FHDR_SIZE);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    pWriter->fSma.size += TSDB_FHDR_SIZE;
  } else {
    n = taosLSeekFile(pWriter->pSmaFD, 0, SEEK_END);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    ASSERT(n == pWriter->fSma.size);
  }

  *ppWriter = pWriter;
  return code;

_err:
  tsdbError("vgId:%d, tsdb data file writer open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

int32_t tsdbDataFWriterClose(SDataFWriter **ppWriter, int8_t sync) {
  int32_t code = 0;
  STsdb  *pTsdb = (*ppWriter)->pTsdb;

  if (*ppWriter == NULL) goto _exit;

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
  taosCalcChecksumAppend(0, hdr, TSDB_FHDR_SIZE);

  n = taosLSeekFile(pWriter->pHeadFD, 0, SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  n = taosWriteFile(pWriter->pHeadFD, hdr, TSDB_FHDR_SIZE);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // data ==============
  memset(hdr, 0, TSDB_FHDR_SIZE);
  tPutDataFile(hdr, &pWriter->fData);
  taosCalcChecksumAppend(0, hdr, TSDB_FHDR_SIZE);

  n = taosLSeekFile(pWriter->pDataFD, 0, SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  n = taosWriteFile(pWriter->pDataFD, hdr, TSDB_FHDR_SIZE);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // last ==============
  memset(hdr, 0, TSDB_FHDR_SIZE);
  tPutLastFile(hdr, &pWriter->fLast);
  taosCalcChecksumAppend(0, hdr, TSDB_FHDR_SIZE);

  n = taosLSeekFile(pWriter->pLastFD, 0, SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  n = taosWriteFile(pWriter->pLastFD, hdr, TSDB_FHDR_SIZE);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // sma ==============
  memset(hdr, 0, TSDB_FHDR_SIZE);
  tPutSmaFile(hdr, &pWriter->fSma);
  taosCalcChecksumAppend(0, hdr, TSDB_FHDR_SIZE);

  n = taosLSeekFile(pWriter->pSmaFD, 0, SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  n = taosWriteFile(pWriter->pSmaFD, hdr, TSDB_FHDR_SIZE);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d, update DFileSet header failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteBlockIdx(SDataFWriter *pWriter, SArray *aBlockIdx, uint8_t **ppBuf) {
  int32_t    code = 0;
  SHeadFile *pHeadFile = &pWriter->fHead;
  uint8_t   *pBuf = NULL;
  int64_t    size;
  int64_t    n;

  if (!ppBuf) ppBuf = &pBuf;

  // prepare
  size = tPutU32(NULL, TSDB_FILE_DLMT);
  for (int32_t iBlockIdx = 0; iBlockIdx < taosArrayGetSize(aBlockIdx); iBlockIdx++) {
    size += tPutBlockIdx(NULL, taosArrayGet(aBlockIdx, iBlockIdx));
  }
  size += sizeof(TSCKSUM);

  // alloc
  code = tRealloc(ppBuf, size);
  if (code) goto _err;

  // build
  n = 0;
  n = tPutU32(*ppBuf + n, TSDB_FILE_DLMT);
  for (int32_t iBlockIdx = 0; iBlockIdx < taosArrayGetSize(aBlockIdx); iBlockIdx++) {
    n += tPutBlockIdx(*ppBuf + n, taosArrayGet(aBlockIdx, iBlockIdx));
  }
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

  tFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d, write block idx failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  tFree(pBuf);
  return code;
}

int32_t tsdbWriteBlock(SDataFWriter *pWriter, SMapData *mBlock, uint8_t **ppBuf, SBlockIdx *pBlockIdx) {
  int32_t       code = 0;
  SHeadFile    *pHeadFile = &pWriter->fHead;
  SBlockDataHdr hdr = {.delimiter = TSDB_FILE_DLMT, .suid = pBlockIdx->suid, .uid = pBlockIdx->uid};
  uint8_t      *pBuf = NULL;
  int64_t       size;
  int64_t       n;

  ASSERT(mBlock->nItem > 0);

  // prepare
  size = sizeof(SBlockDataHdr) + tPutMapData(NULL, mBlock) + sizeof(TSCKSUM);

  // alloc
  if (!ppBuf) ppBuf = &pBuf;
  code = tRealloc(ppBuf, size);
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

  tFree(pBuf);
  tsdbTrace("vgId:%d, write block, offset:%" PRId64 " size:%" PRId64, TD_VID(pWriter->pTsdb->pVnode), pBlockIdx->offset,
            pBlockIdx->size);
  return code;

_err:
  tFree(pBuf);
  tsdbError("vgId:%d, write block failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteBlockL(SDataFWriter *pWriter, SArray *aBlockL, uint8_t **ppBuf) {
  int32_t    code = 0;
  SHeadFile *pHeadFile = &pWriter->fHead;
  uint8_t   *pBuf = NULL;
  int64_t    size;
  int64_t    n;

  // size
  size = sizeof(uint32_t);
  for (int32_t iBlockL = 0; iBlockL < taosArrayGetSize(aBlockL); iBlockL++) {
    size += tPutBlockL(NULL, taosArrayGet(aBlockL, iBlockL));
  }
  size += sizeof(TSCKSUM);

  // alloc
  if (!ppBuf) ppBuf = &pBuf;
  code = tRealloc(ppBuf, size);
  if (code) goto _err;

  // encode
  n = 0;
  n += tPutU32(*ppBuf + n, TSDB_FILE_DLMT);
  for (int32_t iBlockL = 0; iBlockL < taosArrayGetSize(aBlockL); iBlockL++) {
    n += tPutBlockL(*ppBuf + n, taosArrayGet(aBlockL, iBlockL));
  }
  taosCalcChecksumAppend(0, *ppBuf, size);

  ASSERT(n + sizeof(TSCKSUM) == size);

  // write
  n = taosWriteFile(pWriter->pHeadFD, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // update
  pHeadFile->loffset = pHeadFile->size;
  pHeadFile->size += size;

  return code;

_err:
  tsdbError("vgId:%d tsdb write blockl failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static void tsdbUpdateBlockInfo(SBlockData *pBlockData, SBlock *pBlock) {
  for (int32_t iRow = 0; iRow < pBlockData->nRow; iRow++) {
    TSDBKEY key = {.ts = pBlockData->aTSKEY[iRow], .version = pBlockData->aVersion[iRow]};

    if (iRow == 0) {
      if (tsdbKeyCmprFn(&pBlock->minKey, &key) > 0) {
        pBlock->minKey = key;
      }
    } else {
      if (pBlockData->aTSKEY[iRow] == pBlockData->aTSKEY[iRow - 1]) {
        pBlock->hasDup = 1;
      }
    }

    if (iRow == pBlockData->nRow - 1 && tsdbKeyCmprFn(&pBlock->maxKey, &key) < 0) {
      pBlock->maxKey = key;
    }

    pBlock->minVersion = TMIN(pBlock->minVersion, key.version);
    pBlock->maxVersion = TMAX(pBlock->maxVersion, key.version);
  }
  pBlock->nRow += pBlockData->nRow;
}

static int32_t tsdbWriteBlockDataKey(SSubBlock *pSubBlock, SBlockData *pBlockData, uint8_t **ppBuf1, int64_t *nDataP,
                                     uint8_t **ppBuf2) {
  int32_t code = 0;
  int64_t size;
  int64_t tsize;

  if (pSubBlock->cmprAlg == NO_COMPRESSION) {
    pSubBlock->szVersion = sizeof(int64_t) * pSubBlock->nRow;
    pSubBlock->szTSKEY = sizeof(TSKEY) * pSubBlock->nRow;

    code = tRealloc(ppBuf1, *nDataP + pSubBlock->szVersion + pSubBlock->szTSKEY + sizeof(TSCKSUM));
    if (code) goto _err;

    // VERSION
    memcpy(*ppBuf1 + *nDataP, pBlockData->aVersion, pSubBlock->szVersion);

    // TSKEY
    memcpy(*ppBuf1 + *nDataP + pSubBlock->szVersion, pBlockData->aTSKEY, pSubBlock->szTSKEY);
  } else {
    size = (sizeof(int64_t) + sizeof(TSKEY)) * pSubBlock->nRow + COMP_OVERFLOW_BYTES * 2;

    code = tRealloc(ppBuf1, *nDataP + size + sizeof(TSCKSUM));
    if (code) goto _err;

    tsize = sizeof(int64_t) * pSubBlock->nRow + COMP_OVERFLOW_BYTES;
    if (pSubBlock->cmprAlg == TWO_STAGE_COMP) {
      code = tRealloc(ppBuf2, tsize);
      if (code) goto _err;
    }

    // VERSION
    pSubBlock->szVersion =
        tsCompressBigint((char *)pBlockData->aVersion, sizeof(int64_t) * pBlockData->nRow, pBlockData->nRow,
                         *ppBuf1 + *nDataP, size, pSubBlock->cmprAlg, *ppBuf2, tsize);
    if (pSubBlock->szVersion <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }

    // TSKEY
    pSubBlock->szTSKEY = tsCompressTimestamp((char *)pBlockData->aTSKEY, sizeof(TSKEY) * pBlockData->nRow,
                                             pBlockData->nRow, *ppBuf1 + *nDataP + pSubBlock->szVersion,
                                             size - pSubBlock->szVersion, pSubBlock->cmprAlg, *ppBuf2, tsize);
    if (pSubBlock->szTSKEY <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }

    ASSERT(pSubBlock->szVersion + pSubBlock->szTSKEY <= size);
  }

  // checksum
  size = pSubBlock->szVersion + pSubBlock->szTSKEY + sizeof(TSCKSUM);
  taosCalcChecksumAppend(0, *ppBuf1 + *nDataP, size);

  *nDataP += size;
  return code;

_err:
  return code;
}

static int32_t tsdbWriteColData(SColData *pColData, SBlockCol *pBlockCol, SSubBlock *pSubBlock, uint8_t **ppBuf1,
                                int64_t *nDataP, uint8_t **ppBuf2) {
  int32_t code = 0;
  int64_t size;
  int64_t n = 0;

  // BITMAP
  if (pColData->flag != HAS_VALUE) {
    size = BIT2_SIZE(pColData->nVal) + COMP_OVERFLOW_BYTES;

    code = tRealloc(ppBuf1, *nDataP + n + size);
    if (code) goto _err;

    code = tRealloc(ppBuf2, size);
    if (code) goto _err;

    pBlockCol->szBitmap =
        tsCompressTinyint((char *)pColData->pBitMap, BIT2_SIZE(pColData->nVal), BIT2_SIZE(pColData->nVal),
                          *ppBuf1 + *nDataP + n, size, TWO_STAGE_COMP, *ppBuf2, size);
    if (pBlockCol->szBitmap <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }
  } else {
    pBlockCol->szBitmap = 0;
  }
  n += pBlockCol->szBitmap;

  // OFFSET
  if (IS_VAR_DATA_TYPE(pColData->type)) {
    size = sizeof(int32_t) * pColData->nVal + COMP_OVERFLOW_BYTES;

    code = tRealloc(ppBuf1, *nDataP + n + size);
    if (code) goto _err;

    code = tRealloc(ppBuf2, size);
    if (code) goto _err;

    pBlockCol->szOffset = tsCompressInt((char *)pColData->aOffset, sizeof(int32_t) * pColData->nVal, pColData->nVal,
                                        *ppBuf1 + *nDataP + n, size, TWO_STAGE_COMP, *ppBuf2, size);
    if (pBlockCol->szOffset <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }
  } else {
    pBlockCol->szOffset = 0;
  }
  n += pBlockCol->szOffset;

  // VALUE
  if (pSubBlock->cmprAlg == NO_COMPRESSION) {
    pBlockCol->szValue = pColData->nData;

    code = tRealloc(ppBuf1, *nDataP + n + pBlockCol->szValue + sizeof(TSCKSUM));
    if (code) goto _err;

    memcpy(*ppBuf1 + *nDataP + n, pColData->pData, pBlockCol->szValue);
  } else {
    size = pColData->nData + COMP_OVERFLOW_BYTES;

    code = tRealloc(ppBuf1, *nDataP + n + size + sizeof(TSCKSUM));
    if (code) goto _err;

    if (pSubBlock->cmprAlg == TWO_STAGE_COMP) {
      code = tRealloc(ppBuf2, size);
      if (code) goto _err;
    }

    pBlockCol->szValue =
        tDataTypes[pColData->type].compFunc((char *)pColData->pData, pColData->nData, pColData->nVal,
                                            *ppBuf1 + *nDataP + n, size, pSubBlock->cmprAlg, *ppBuf2, size);
    if (pBlockCol->szValue <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }
  }
  n += pBlockCol->szValue;
  pBlockCol->szOrigin = pColData->nData;

  // checksum
  n += sizeof(TSCKSUM);
  taosCalcChecksumAppend(0, *ppBuf1 + *nDataP, n);

  *nDataP += n;

  return code;

_err:
  return code;
}

static int32_t tsdbWriteBlockDataImpl(TdFilePtr pFD, SSubBlock *pSubBlock, SBlockDataHdr hdr, SArray *aBlockCol,
                                      uint8_t *pData, int64_t nData, uint8_t **ppBuf) {
  int32_t code = 0;
  int32_t nBlockCol = taosArrayGetSize(aBlockCol);
  int64_t size;
  int64_t n;

  // HDR + SArray<SBlockCol>
  pSubBlock->szBlockCol = sizeof(hdr);
  for (int32_t iBlockCol = 0; iBlockCol < nBlockCol; iBlockCol++) {
    pSubBlock->szBlockCol += tPutBlockCol(NULL, taosArrayGet(aBlockCol, iBlockCol));
  }

  code = tRealloc(ppBuf, pSubBlock->szBlockCol + sizeof(TSCKSUM));
  if (code) goto _err;

  n = 0;
  memcpy(*ppBuf, &hdr, sizeof(hdr));
  n += sizeof(hdr);
  for (int32_t iBlockCol = 0; iBlockCol < nBlockCol; iBlockCol++) {
    n += tPutBlockCol(*ppBuf + n, taosArrayGet(aBlockCol, iBlockCol));
  }
  taosCalcChecksumAppend(0, *ppBuf, pSubBlock->szBlockCol + sizeof(TSCKSUM));

  ASSERT(n == pSubBlock->szBlockCol);

  n = taosWriteFile(pFD, *ppBuf, pSubBlock->szBlockCol + sizeof(TSCKSUM));
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // SBlockData
  n = taosWriteFile(pFD, pData, nData);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  return code;

_err:
  return code;
}

static int32_t tsdbWriteBlockSma(TdFilePtr pFD, SBlockData *pBlockData, SSubBlock *pSubBlock, uint8_t **ppBuf) {
  int32_t   code = 0;
  int64_t   n;
  SColData *pColData;

  // prepare
  pSubBlock->nSma = 0;
  for (int32_t iColData = 0; iColData < taosArrayGetSize(pBlockData->aIdx); iColData++) {
    pColData = tBlockDataGetColDataByIdx(pBlockData, iColData);

    if (IS_VAR_DATA_TYPE(pColData->type) || (!pColData->smaOn)) continue;

    pSubBlock->nSma++;
  }
  if (pSubBlock->nSma == 0) goto _exit;

  // calc
  code = tRealloc(ppBuf, sizeof(SColumnDataAgg) * pSubBlock->nSma + sizeof(TSCKSUM));
  if (code) goto _err;
  n = 0;
  for (int32_t iColData = 0; iColData < taosArrayGetSize(pBlockData->aIdx); iColData++) {
    pColData = tBlockDataGetColDataByIdx(pBlockData, iColData);

    if (IS_VAR_DATA_TYPE(pColData->type) || (!pColData->smaOn)) continue;

    tsdbCalcColDataSMA(pColData, &((SColumnDataAgg *)(*ppBuf))[n]);
    n++;
  }
  taosCalcChecksumAppend(0, *ppBuf, sizeof(SColumnDataAgg) * pSubBlock->nSma + sizeof(TSCKSUM));

  // write
  n = taosWriteFile(pFD, *ppBuf, sizeof(SColumnDataAgg) * pSubBlock->nSma + sizeof(TSCKSUM));
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

_exit:
  return code;

_err:
  return code;
}

int32_t tsdbWriteBlockData(SDataFWriter *pWriter, SBlockData *pBlockData, uint8_t **ppBuf1, uint8_t **ppBuf2,
                           SBlockIdx *pBlockIdx, SBlock *pBlock, int8_t cmprAlg) {
  int32_t       code = 0;
  SSubBlock    *pSubBlock = &pBlock->aSubBlock[pBlock->nSubBlock++];
  SBlockCol     blockCol;
  SBlockCol    *pBlockCol = &blockCol;
  int64_t       n;
  TdFilePtr     pFileFD = pBlock->last ? pWriter->pLastFD : pWriter->pDataFD;
  SBlockDataHdr hdr = {.delimiter = TSDB_FILE_DLMT, .suid = pBlockIdx->suid, .uid = pBlockIdx->uid};
  uint8_t      *p;
  int64_t       nData;
  uint8_t      *pBuf1 = NULL;
  uint8_t      *pBuf2 = NULL;
  SArray       *aBlockCol = NULL;

  if (!ppBuf1) ppBuf1 = &pBuf1;
  if (!ppBuf2) ppBuf2 = &pBuf2;

  tsdbUpdateBlockInfo(pBlockData, pBlock);

  pSubBlock->nRow = pBlockData->nRow;
  pSubBlock->cmprAlg = cmprAlg;
  if (pBlock->last) {
    pSubBlock->offset = pWriter->fLast.size;
  } else {
    pSubBlock->offset = pWriter->fData.size;
  }

  // ======================= BLOCK DATA =======================
  // TSDBKEY
  nData = 0;
  code = tsdbWriteBlockDataKey(pSubBlock, pBlockData, ppBuf1, &nData, ppBuf2);
  if (code) goto _err;

  // COLUMNS
  aBlockCol = taosArrayInit(taosArrayGetSize(pBlockData->aIdx), sizeof(SBlockCol));
  if (aBlockCol == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  int32_t offset = 0;
  for (int32_t iCol = 0; iCol < taosArrayGetSize(pBlockData->aIdx); iCol++) {
    SColData *pColData = tBlockDataGetColDataByIdx(pBlockData, iCol);

    ASSERT(pColData->flag);

    if (pColData->flag == HAS_NONE) continue;

    pBlockCol->cid = pColData->cid;
    pBlockCol->type = pColData->type;
    pBlockCol->smaOn = pColData->smaOn;
    pBlockCol->flag = pColData->flag;

    if (pColData->flag != HAS_NULL) {
      code = tsdbWriteColData(pColData, pBlockCol, pSubBlock, ppBuf1, &nData, ppBuf2);
      if (code) goto _err;

      pBlockCol->offset = offset;
      offset = offset + pBlockCol->szBitmap + pBlockCol->szOffset + pBlockCol->szValue + sizeof(TSCKSUM);
    }

    if (taosArrayPush(aBlockCol, pBlockCol) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  // write
  code = tsdbWriteBlockDataImpl(pFileFD, pSubBlock, hdr, aBlockCol, *ppBuf1, nData, ppBuf2);
  if (code) goto _err;

  pSubBlock->szBlock = pSubBlock->szBlockCol + sizeof(TSCKSUM) + nData;
  if (pBlock->last) {
    pWriter->fLast.size += pSubBlock->szBlock;
  } else {
    pWriter->fData.size += pSubBlock->szBlock;
  }

  // ======================= BLOCK SMA =======================
  pSubBlock->sOffset = 0;
  pSubBlock->nSma = 0;

  if (pBlock->nSubBlock > 1 || pBlock->last || pBlock->hasDup) goto _exit;

  code = tsdbWriteBlockSma(pWriter->pSmaFD, pBlockData, pSubBlock, ppBuf1);
  if (code) goto _err;

  if (pSubBlock->nSma > 0) {
    pSubBlock->sOffset = pWriter->fSma.size;
    pWriter->fSma.size += (sizeof(SColumnDataAgg) * pSubBlock->nSma + sizeof(TSCKSUM));
  }

_exit:
  tFree(pBuf1);
  tFree(pBuf2);
  taosArrayDestroy(aBlockCol);
  return code;

_err:
  tsdbError("vgId:%d, write block data failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  tFree(pBuf1);
  tFree(pBuf2);
  taosArrayDestroy(aBlockCol);
  return code;
}

int32_t tsdbDFileSetCopy(STsdb *pTsdb, SDFileSet *pSetFrom, SDFileSet *pSetTo) {
  int32_t   code = 0;
  int64_t   n;
  int64_t   size;
  TdFilePtr pOutFD = NULL;  // TODO
  TdFilePtr PInFD = NULL;   // TODO
  char      fNameFrom[TSDB_FILENAME_LEN];
  char      fNameTo[TSDB_FILENAME_LEN];

  // head
  tsdbHeadFileName(pTsdb, pSetFrom->diskId, pSetFrom->fid, pSetFrom->pHeadF, fNameFrom);
  tsdbHeadFileName(pTsdb, pSetTo->diskId, pSetTo->fid, pSetTo->pHeadF, fNameTo);

  pOutFD = taosOpenFile(fNameTo, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (pOutFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  PInFD = taosOpenFile(fNameFrom, TD_FILE_READ);
  if (PInFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  n = taosFSendFile(pOutFD, PInFD, 0, pSetFrom->pHeadF->size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  taosCloseFile(&pOutFD);
  taosCloseFile(&PInFD);

  // data
  tsdbDataFileName(pTsdb, pSetFrom->diskId, pSetFrom->fid, pSetFrom->pDataF, fNameFrom);
  tsdbDataFileName(pTsdb, pSetTo->diskId, pSetTo->fid, pSetTo->pDataF, fNameTo);

  pOutFD = taosOpenFile(fNameTo, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (pOutFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  PInFD = taosOpenFile(fNameFrom, TD_FILE_READ);
  if (PInFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  n = taosFSendFile(pOutFD, PInFD, 0, pSetFrom->pDataF->size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  taosCloseFile(&pOutFD);
  taosCloseFile(&PInFD);

  // last
  tsdbLastFileName(pTsdb, pSetFrom->diskId, pSetFrom->fid, pSetFrom->pLastF, fNameFrom);
  tsdbLastFileName(pTsdb, pSetTo->diskId, pSetTo->fid, pSetTo->pLastF, fNameTo);

  pOutFD = taosOpenFile(fNameTo, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (pOutFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  PInFD = taosOpenFile(fNameFrom, TD_FILE_READ);
  if (PInFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  n = taosFSendFile(pOutFD, PInFD, 0, pSetFrom->pLastF->size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  taosCloseFile(&pOutFD);
  taosCloseFile(&PInFD);

  // sma
  tsdbSmaFileName(pTsdb, pSetFrom->diskId, pSetFrom->fid, pSetFrom->pSmaF, fNameFrom);
  tsdbSmaFileName(pTsdb, pSetTo->diskId, pSetTo->fid, pSetTo->pSmaF, fNameTo);

  pOutFD = taosOpenFile(fNameTo, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (pOutFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  PInFD = taosOpenFile(fNameFrom, TD_FILE_READ);
  if (PInFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  n = taosFSendFile(pOutFD, PInFD, 0, pSetFrom->pSmaF->size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  taosCloseFile(&pOutFD);
  taosCloseFile(&PInFD);

  return code;

_err:
  tsdbError("vgId:%d, tsdb DFileSet copy failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}