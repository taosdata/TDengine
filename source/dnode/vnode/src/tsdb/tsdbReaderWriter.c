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
  STsdb       *pTsdb = pWriter->pTsdb;

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
  size = sizeof(uint32_t);
  for (int32_t iDelData = 0; iDelData < taosArrayGetSize(aDelData); iDelData++) {
    size += tPutDelData(NULL, taosArrayGet(aDelData, iDelData));
  }
  size += sizeof(TSCKSUM);

  // alloc
  code = tRealloc(&pWriter->aBuf[0], size);
  if (code) goto _err;

  // build
  n = 0;
  n += tPutU32(pWriter->aBuf[0] + n, TSDB_FILE_DLMT);
  for (int32_t iDelData = 0; iDelData < taosArrayGetSize(aDelData); iDelData++) {
    n += tPutDelData(pWriter->aBuf[0] + n, taosArrayGet(aDelData, iDelData));
  }
  taosCalcChecksumAppend(0, pWriter->aBuf[0], size);

  ASSERT(n + sizeof(TSCKSUM) == size);

  // write
  n = taosWriteFile(pWriter->pWriteH, pWriter->aBuf[0], size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  ASSERT(n == size);

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
  size = sizeof(uint32_t);
  for (int32_t iDelIdx = 0; iDelIdx < taosArrayGetSize(aDelIdx); iDelIdx++) {
    size += tPutDelIdx(NULL, taosArrayGet(aDelIdx, iDelIdx));
  }
  size += sizeof(TSCKSUM);

  // alloc
  code = tRealloc(&pWriter->aBuf[0], size);
  if (code) goto _err;

  // build
  n = 0;
  n += tPutU32(pWriter->aBuf[0] + n, TSDB_FILE_DLMT);
  for (int32_t iDelIdx = 0; iDelIdx < taosArrayGetSize(aDelIdx); iDelIdx++) {
    n += tPutDelIdx(pWriter->aBuf[0] + n, taosArrayGet(aDelIdx, iDelIdx));
  }
  taosCalcChecksumAppend(0, pWriter->aBuf[0], size);

  ASSERT(n + sizeof(TSCKSUM) == size);

  // write
  n = taosWriteFile(pWriter->pWriteH, pWriter->aBuf[0], size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

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

  uint8_t *aBuf[1];
};

int32_t tsdbDelFReaderOpen(SDelFReader **ppReader, SDelFile *pFile, STsdb *pTsdb) {
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
    for (int32_t iBuf = 0; iBuf < sizeof(pReader->aBuf) / sizeof(uint8_t *); iBuf++) {
      tFree(pReader->aBuf[iBuf]);
    }
    taosMemoryFree(pReader);
  }
  *ppReader = NULL;

_exit:
  return code;
}

int32_t tsdbReadDelData(SDelFReader *pReader, SDelIdx *pDelIdx, SArray *aDelData) {
  int32_t code = 0;
  int64_t offset = pDelIdx->offset;
  int64_t size = pDelIdx->size;
  int64_t n;

  taosArrayClear(aDelData);

  // seek
  if (taosLSeekFile(pReader->pReadH, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  if (code) goto _err;

  // read
  n = taosReadFile(pReader->pReadH, pReader->aBuf[0], size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // check
  if (!taosCheckChecksumWhole(pReader->aBuf[0], size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // // decode
  n = 0;

  uint32_t delimiter;
  n += tGetU32(pReader->aBuf[0] + n, &delimiter);
  while (n < size - sizeof(TSCKSUM)) {
    SDelData delData;
    n += tGetDelData(pReader->aBuf[0] + n, &delData);

    if (taosArrayPush(aDelData, &delData) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  ASSERT(n == size - sizeof(TSCKSUM));

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

  // seek
  if (taosLSeekFile(pReader->pReadH, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  if (code) goto _err;

  // read
  n = taosReadFile(pReader->pReadH, pReader->aBuf[0], size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // check
  if (!taosCheckChecksumWhole(pReader->aBuf[0], size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // decode
  n = 0;
  uint32_t delimiter;
  n += tGetU32(pReader->aBuf[0] + n, &delimiter);
  ASSERT(delimiter == TSDB_FILE_DLMT);

  while (n < size - sizeof(TSCKSUM)) {
    SDelIdx delIdx;

    n += tGetDelIdx(pReader->aBuf[0] + n, &delIdx);

    if (taosArrayPush(aDelIdx, &delIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  ASSERT(n == size - sizeof(TSCKSUM));

  return code;

_err:
  tsdbError("vgId:%d, read del idx failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
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

  uint8_t *aBuf[3];
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

  for (int32_t iBuf = 0; iBuf < sizeof((*ppReader)->aBuf) / sizeof(uint8_t *); iBuf++) {
    tFree((*ppReader)->aBuf[iBuf]);
  }

  taosMemoryFree(*ppReader);

_exit:
  *ppReader = NULL;
  return code;

_err:
  tsdbError("vgId:%d, data file reader close failed since %s", TD_VID((*ppReader)->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadBlockIdx(SDataFReader *pReader, SArray *aBlockIdx) {
  int32_t  code = 0;
  int64_t  offset = pReader->pSet->pHeadF->offset;
  int64_t  size = pReader->pSet->pHeadF->size - offset;
  int64_t  n;
  uint32_t delimiter;

  taosArrayClear(aBlockIdx);
  if (size == 0) {
    goto _exit;
  }

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  if (code) goto _err;

  // seek
  if (taosLSeekFile(pReader->pHeadFD, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // read
  n = taosReadFile(pReader->pHeadFD, pReader->aBuf[0], size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // check
  if (!taosCheckChecksumWhole(pReader->aBuf[0], size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // decode
  n = 0;
  n = tGetU32(pReader->aBuf[0] + n, &delimiter);
  ASSERT(delimiter == TSDB_FILE_DLMT);

  while (n < size - sizeof(TSCKSUM)) {
    SBlockIdx blockIdx;
    n += tGetBlockIdx(pReader->aBuf[0] + n, &blockIdx);

    if (taosArrayPush(aBlockIdx, &blockIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  ASSERT(n + sizeof(TSCKSUM) == size);

_exit:
  return code;

_err:
  tsdbError("vgId:%d, read block idx failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadBlockL(SDataFReader *pReader, SArray *aBlockL) {
  int32_t  code = 0;
  int64_t  offset = pReader->pSet->pLastF->offset;
  int64_t  size = pReader->pSet->pLastF->size - offset;
  int64_t  n;
  uint32_t delimiter;

  taosArrayClear(aBlockL);
  if (size == 0) {
    goto _exit;
  }

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  if (code) goto _err;

  // seek
  if (taosLSeekFile(pReader->pLastFD, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // read
  n = taosReadFile(pReader->pLastFD, pReader->aBuf[0], size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // check
  if (!taosCheckChecksumWhole(pReader->aBuf[0], size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // decode
  n = 0;
  n = tGetU32(pReader->aBuf[0] + n, &delimiter);
  ASSERT(delimiter == TSDB_FILE_DLMT);

  while (n < size - sizeof(TSCKSUM)) {
    SBlockL blockl;
    n += tGetBlockL(pReader->aBuf[0] + n, &blockl);

    if (taosArrayPush(aBlockL, &blockl) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  ASSERT(n + sizeof(TSCKSUM) == size);

_exit:
  return code;

_err:
  tsdbError("vgId:%d read blockl failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadBlock(SDataFReader *pReader, SBlockIdx *pBlockIdx, SMapData *mBlock) {
  int32_t code = 0;
  int64_t offset = pBlockIdx->offset;
  int64_t size = pBlockIdx->size;
  int64_t n;
  int64_t tn;

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  if (code) goto _err;

  // seek
  if (taosLSeekFile(pReader->pHeadFD, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // read
  n = taosReadFile(pReader->pHeadFD, pReader->aBuf[0], size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // check
  if (!taosCheckChecksumWhole(pReader->aBuf[0], size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // decode
  n = 0;

  uint32_t delimiter;
  n += tGetU32(pReader->aBuf[0] + n, &delimiter);
  ASSERT(delimiter == TSDB_FILE_DLMT);

  tn = tGetMapData(pReader->aBuf[0] + n, mBlock);
  if (tn < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  n += tn;
  ASSERT(n + sizeof(TSCKSUM) == size);

  return code;

_err:
  tsdbError("vgId:%d, read block failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadBlockSma(SDataFReader *pReader, SBlock *pBlock, SArray *aColumnDataAgg) {
  int32_t   code = 0;
  SSmaInfo *pSmaInfo = &pBlock->smaInfo;

  ASSERT(pSmaInfo->size > 0);

  taosArrayClear(aColumnDataAgg);

  // alloc
  int32_t size = pSmaInfo->size + sizeof(TSCKSUM);
  code = tRealloc(&pReader->aBuf[0], size);
  if (code) goto _err;

  // seek
  int64_t n = taosLSeekFile(pReader->pSmaFD, pSmaInfo->offset, SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < pSmaInfo->offset) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // read
  n = taosReadFile(pReader->pSmaFD, pReader->aBuf[0], size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // check
  if (!taosCheckChecksumWhole(pReader->aBuf[0], size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // decode
  n = 0;
  while (n < pSmaInfo->size) {
    SColumnDataAgg sma;

    n += tGetColumnDataAgg(pReader->aBuf[0] + n, &sma);
    if (taosArrayPush(aColumnDataAgg, &sma) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb read block sma failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbReadBlockDataImpl(SDataFReader *pReader, SBlockInfo *pBlkInfo, int8_t fromLast,
                                     SBlockData *pBlockData) {
  int32_t code = 0;

  tBlockDataClear(pBlockData);

  TdFilePtr pFD = fromLast ? pReader->pLastFD : pReader->pDataFD;

  // uid + version + tskey
  code = tsdbReadAndCheck(pFD, pBlkInfo->offset, &pReader->aBuf[0], pBlkInfo->szKey, 1);
  if (code) goto _err;
  SDiskDataHdr hdr;
  uint8_t     *p = pReader->aBuf[0] + tGetDiskDataHdr(pReader->aBuf[0], &hdr);

  ASSERT(hdr.delimiter == TSDB_FILE_DLMT);
  ASSERT(pBlockData->suid == hdr.suid);
  ASSERT(pBlockData->uid == hdr.uid);

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

  ASSERT(p - pReader->aBuf[0] == pBlkInfo->szKey - sizeof(TSCKSUM));

  // read and decode columns
  if (taosArrayGetSize(pBlockData->aIdx) == 0) goto _exit;

  if (hdr.szBlkCol > 0) {
    int64_t offset = pBlkInfo->offset + pBlkInfo->szKey;
    code = tsdbReadAndCheck(pFD, offset, &pReader->aBuf[0], hdr.szBlkCol + sizeof(TSCKSUM), 1);
    if (code) goto _err;
  }

  SBlockCol  blockCol = {.cid = 0};
  SBlockCol *pBlockCol = &blockCol;
  int32_t    n = 0;

  for (int32_t iColData = 0; iColData < taosArrayGetSize(pBlockData->aIdx); iColData++) {
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
        int64_t offset = pBlkInfo->offset + pBlkInfo->szKey + hdr.szBlkCol + sizeof(TSCKSUM) + pBlockCol->offset;
        int32_t size = pBlockCol->szBitmap + pBlockCol->szOffset + pBlockCol->szValue + sizeof(TSCKSUM);

        code = tsdbReadAndCheck(pFD, offset, &pReader->aBuf[1], size, 0);
        if (code) goto _err;

        code = tsdbDecmprColData(pReader->aBuf[1], pBlockCol, hdr.cmprAlg, hdr.nRow, pColData, &pReader->aBuf[2]);
        if (code) goto _err;
      }
    }
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d tsdb read block data impl failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadDataBlock(SDataFReader *pReader, SBlock *pBlock, SBlockData *pBlockData) {
  int32_t code = 0;

  code = tsdbReadBlockDataImpl(pReader, &pBlock->aSubBlock[0], 0, pBlockData);
  if (code) goto _err;

  if (pBlock->nSubBlock > 1) {
    SBlockData bData1;
    SBlockData bData2;

    // create
    code = tBlockDataCreate(&bData1);
    if (code) goto _err;
    code = tBlockDataCreate(&bData2);
    if (code) goto _err;

    // init
    tBlockDataInitEx(&bData1, pBlockData);
    tBlockDataInitEx(&bData2, pBlockData);

    for (int32_t iSubBlock = 1; iSubBlock < pBlock->nSubBlock; iSubBlock++) {
      code = tsdbReadBlockDataImpl(pReader, &pBlock->aSubBlock[iSubBlock], 0, &bData1);
      if (code) {
        tBlockDataDestroy(&bData1, 1);
        tBlockDataDestroy(&bData2, 1);
        goto _err;
      }

      code = tBlockDataCopy(pBlockData, &bData2);
      if (code) {
        tBlockDataDestroy(&bData1, 1);
        tBlockDataDestroy(&bData2, 1);
        goto _err;
      }

      code = tBlockDataMerge(&bData1, &bData2, pBlockData);
      if (code) {
        tBlockDataDestroy(&bData1, 1);
        tBlockDataDestroy(&bData2, 1);
        goto _err;
      }
    }

    tBlockDataDestroy(&bData1, 1);
    tBlockDataDestroy(&bData2, 1);
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb read data block failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadLastBlock(SDataFReader *pReader, SBlockL *pBlockL, SBlockData *pBlockData) {
  int32_t code = 0;

  code = tsdbReadBlockDataImpl(pReader, &pBlockL->bInfo, 1, pBlockData);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d tsdb read last block failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
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
  if (code) goto _err;
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
  STsdb  *pTsdb = NULL;

  if (*ppWriter == NULL) goto _exit;

  pTsdb = (*ppWriter)->pTsdb;
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

int32_t tsdbWriteBlockIdx(SDataFWriter *pWriter, SArray *aBlockIdx) {
  int32_t    code = 0;
  SHeadFile *pHeadFile = &pWriter->fHead;
  int64_t    size = 0;
  int64_t    n;

  // check
  if (taosArrayGetSize(aBlockIdx) == 0) {
    pHeadFile->offset = pHeadFile->size;
    goto _exit;
  }

  // prepare
  size = sizeof(uint32_t);
  for (int32_t iBlockIdx = 0; iBlockIdx < taosArrayGetSize(aBlockIdx); iBlockIdx++) {
    size += tPutBlockIdx(NULL, taosArrayGet(aBlockIdx, iBlockIdx));
  }
  size += sizeof(TSCKSUM);

  // alloc
  code = tRealloc(&pWriter->aBuf[0], size);
  if (code) goto _err;

  // build
  n = 0;
  n = tPutU32(pWriter->aBuf[0] + n, TSDB_FILE_DLMT);
  for (int32_t iBlockIdx = 0; iBlockIdx < taosArrayGetSize(aBlockIdx); iBlockIdx++) {
    n += tPutBlockIdx(pWriter->aBuf[0] + n, taosArrayGet(aBlockIdx, iBlockIdx));
  }
  taosCalcChecksumAppend(0, pWriter->aBuf[0], size);

  ASSERT(n + sizeof(TSCKSUM) == size);

  // write
  n = taosWriteFile(pWriter->pHeadFD, pWriter->aBuf[0], size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // update
  pHeadFile->offset = pHeadFile->size;
  pHeadFile->size += size;

_exit:
  tsdbTrace("vgId:%d write block idx, offset:%" PRId64 " size:%" PRId64 " nBlockIdx:%d", TD_VID(pWriter->pTsdb->pVnode),
            pHeadFile->offset, size, taosArrayGetSize(aBlockIdx));
  return code;

_err:
  tsdbError("vgId:%d, write block idx failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteBlock(SDataFWriter *pWriter, SMapData *mBlock, SBlockIdx *pBlockIdx) {
  int32_t    code = 0;
  SHeadFile *pHeadFile = &pWriter->fHead;
  int64_t    size;
  int64_t    n;

  ASSERT(mBlock->nItem > 0);

  // alloc
  size = sizeof(uint32_t) + tPutMapData(NULL, mBlock) + sizeof(TSCKSUM);
  code = tRealloc(&pWriter->aBuf[0], size);
  if (code) goto _err;

  // build
  n = 0;
  n += tPutU32(pWriter->aBuf[0] + n, TSDB_FILE_DLMT);
  n += tPutMapData(pWriter->aBuf[0] + n, mBlock);
  taosCalcChecksumAppend(0, pWriter->aBuf[0], size);

  ASSERT(n + sizeof(TSCKSUM) == size);

  // write
  n = taosWriteFile(pWriter->pHeadFD, pWriter->aBuf[0], size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // update
  pBlockIdx->offset = pHeadFile->size;
  pBlockIdx->size = size;
  pHeadFile->size += size;

  tsdbTrace("vgId:%d, write block, file ID:%d commit ID:%d suid:%" PRId64 " uid:%" PRId64 " offset:%" PRId64
            " size:%" PRId64 " nItem:%d",
            TD_VID(pWriter->pTsdb->pVnode), pWriter->wSet.fid, pHeadFile->commitID, pBlockIdx->suid, pBlockIdx->uid,
            pBlockIdx->offset, pBlockIdx->size, mBlock->nItem);
  return code;

_err:
  tsdbError("vgId:%d, write block failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteBlockL(SDataFWriter *pWriter, SArray *aBlockL) {
  int32_t    code = 0;
  SLastFile *pLastFile = &pWriter->fLast;
  int64_t    size;
  int64_t    n;

  // check
  if (taosArrayGetSize(aBlockL) == 0) {
    pLastFile->offset = pLastFile->size;
    goto _exit;
  }

  // size
  size = sizeof(uint32_t);  // TSDB_FILE_DLMT
  for (int32_t iBlockL = 0; iBlockL < taosArrayGetSize(aBlockL); iBlockL++) {
    size += tPutBlockL(NULL, taosArrayGet(aBlockL, iBlockL));
  }
  size += sizeof(TSCKSUM);

  // alloc
  code = tRealloc(&pWriter->aBuf[0], size);
  if (code) goto _err;

  // encode
  n = 0;
  n += tPutU32(pWriter->aBuf[0] + n, TSDB_FILE_DLMT);
  for (int32_t iBlockL = 0; iBlockL < taosArrayGetSize(aBlockL); iBlockL++) {
    n += tPutBlockL(pWriter->aBuf[0] + n, taosArrayGet(aBlockL, iBlockL));
  }
  taosCalcChecksumAppend(0, pWriter->aBuf[0], size);

  ASSERT(n + sizeof(TSCKSUM) == size);

  // write
  n = taosWriteFile(pWriter->pLastFD, pWriter->aBuf[0], size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // update
  pLastFile->offset = pLastFile->size;
  pLastFile->size += size;

_exit:
  tsdbTrace("vgId:%d tsdb write blockl, loffset:%" PRId64 " size:%" PRId64, TD_VID(pWriter->pTsdb->pVnode),
            pLastFile->offset, size);
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

    pBlock->minVer = TMIN(pBlock->minVer, key.version);
    pBlock->maxVer = TMAX(pBlock->maxVer, key.version);
  }
  pBlock->nRow += pBlockData->nRow;
}

static int32_t tsdbWriteBlockSma(SDataFWriter *pWriter, SBlockData *pBlockData, SSmaInfo *pSmaInfo) {
  int32_t code = 0;

  pSmaInfo->offset = 0;
  pSmaInfo->size = 0;

  // encode
  for (int32_t iColData = 0; iColData < taosArrayGetSize(pBlockData->aIdx); iColData++) {
    SColData *pColData = tBlockDataGetColDataByIdx(pBlockData, iColData);

    if ((!pColData->smaOn) || IS_VAR_DATA_TYPE(pColData->type)) continue;

    SColumnDataAgg sma;
    tsdbCalcColDataSMA(pColData, &sma);

    code = tRealloc(&pWriter->aBuf[0], pSmaInfo->size + tPutColumnDataAgg(NULL, &sma));
    if (code) goto _err;
    pSmaInfo->size += tPutColumnDataAgg(pWriter->aBuf[0] + pSmaInfo->size, &sma);
  }

  // write
  if (pSmaInfo->size) {
    int32_t size = pSmaInfo->size + sizeof(TSCKSUM);

    code = tRealloc(&pWriter->aBuf[0], size);
    if (code) goto _err;

    taosCalcChecksumAppend(0, pWriter->aBuf[0], size);

    int64_t n = taosWriteFile(pWriter->pSmaFD, pWriter->aBuf[0], size);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    pSmaInfo->offset = pWriter->fSma.size;
    pWriter->fSma.size += size;
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb write block sma failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbWriteBlockData(SDataFWriter *pWriter, SBlockData *pBlockData, SBlockInfo *pBlkInfo, SSmaInfo *pSmaInfo,
                           int8_t cmprAlg, int8_t toLast) {
  int32_t code = 0;

  ASSERT(pBlockData->nRow > 0);

  pBlkInfo->offset = toLast ? pWriter->fLast.size : pWriter->fData.size;
  pBlkInfo->szBlock = 0;
  pBlkInfo->szKey = 0;

  int32_t aBufN[4] = {0};
  code = tCmprBlockData(pBlockData, cmprAlg, NULL, NULL, pWriter->aBuf, aBufN);
  if (code) goto _err;

  // write =================
  TdFilePtr pFD = toLast ? pWriter->pLastFD : pWriter->pDataFD;

  pBlkInfo->szKey = aBufN[3] + aBufN[2];
  pBlkInfo->szBlock = aBufN[0] + aBufN[1] + aBufN[2] + aBufN[3];

  int64_t n = taosWriteFile(pFD, pWriter->aBuf[3], aBufN[3]);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  n = taosWriteFile(pFD, pWriter->aBuf[2], aBufN[2]);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (aBufN[1]) {
    n = taosWriteFile(pFD, pWriter->aBuf[1], aBufN[1]);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
  }

  if (aBufN[0]) {
    n = taosWriteFile(pFD, pWriter->aBuf[0], aBufN[0]);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
  }

  // update info
  if (toLast) {
    pWriter->fLast.size += pBlkInfo->szBlock;
  } else {
    pWriter->fData.size += pBlkInfo->szBlock;
  }

  // ================= SMA ====================
  if (pSmaInfo) {
    code = tsdbWriteBlockSma(pWriter, pBlockData, pSmaInfo);
    if (code) goto _err;
  }

_exit:
  tsdbTrace("vgId:%d tsdb write block data, suid:%" PRId64 " uid:%" PRId64 " nRow:%d, offset:%" PRId64 " size:%d",
            TD_VID(pWriter->pTsdb->pVnode), pBlockData->suid, pBlockData->uid, pBlockData->nRow, pBlkInfo->offset,
            pBlkInfo->szBlock);
  return code;

_err:
  tsdbError("vgId:%d tsdb write block data failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
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
