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
struct SDelFWriter {
  STsdb    *pTsdb;
  SDelFile *pFile;
  TdFilePtr pWriteH;
};

int32_t tsdbDelFWriterOpen(SDelFWriter **ppWriter, SDelFile *pFile, STsdb *pTsdb) {
  int32_t      code = 0;
  char        *fname = NULL;  // TODO
  SDelFWriter *pDelFWriter;

  pDelFWriter = (SDelFWriter *)taosMemoryCalloc(1, sizeof(*pDelFWriter));
  if (pDelFWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pDelFWriter->pTsdb = pTsdb;
  pDelFWriter->pFile = pFile;
  pDelFWriter->pWriteH = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE);
  if (pDelFWriter->pWriteH == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosLSeekFile(pDelFWriter->pWriteH, TSDB_FHDR_SIZE, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pDelFWriter->pFile->size = TSDB_FHDR_SIZE;
  pDelFWriter->pFile->offset = 0;

  return code;

_err:
  tsdbError("vgId:%d failed to open del file writer since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
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

int32_t tsdbWriteDelData(SDelFWriter *pWriter, SMapData *pDelDataMap, uint8_t **ppBuf, SDelIdx *pDelIdx) {
  int32_t  code = 0;
  uint8_t *pBuf = NULL;
  int64_t  size = 0;
  int64_t  n = 0;

  // prepare
  size += tPutU32(NULL, TSDB_FILE_DLMT);
  size += tPutI64(NULL, pDelIdx->suid);
  size += tPutI64(NULL, pDelIdx->uid);
  size = size + tPutMapData(NULL, pDelDataMap) + sizeof(TSCKSUM);

  // alloc
  if (!ppBuf) ppBuf = &pBuf;
  code = tsdbRealloc(ppBuf, size);
  if (code) goto _err;

  // build
  n += tPutU32(*ppBuf + n, TSDB_FILE_DLMT);
  n += tPutI64(*ppBuf + n, pDelIdx->suid);
  n += tPutI64(*ppBuf + n, pDelIdx->uid);
  n += tPutMapData(*ppBuf + n, pDelDataMap);
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
  pDelIdx->offset = pWriter->pFile->size;
  pDelIdx->size = size;
  pWriter->pFile->offset = pWriter->pFile->size;
  pWriter->pFile->size += size;

  tsdbFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d failed to write del data since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  tsdbFree(pBuf);
  return code;
}

int32_t tsdbWriteDelIdx(SDelFWriter *pWriter, SMapData *pDelIdxMap, uint8_t **ppBuf) {
  int32_t  code = 0;
  int64_t  size = 0;
  int64_t  n = 0;
  uint8_t *pBuf = NULL;

  // prepare
  size += tPutU32(NULL, TSDB_FILE_DLMT);
  size = size + tPutMapData(NULL, pDelIdxMap) + sizeof(TSCKSUM);

  // alloc
  if (!ppBuf) ppBuf = &pBuf;
  code = tsdbRealloc(ppBuf, size);
  if (code) goto _err;

  // build
  n += tPutU32(*ppBuf + n, TSDB_FILE_DLMT);
  n += tPutMapData(*ppBuf + n, pDelIdxMap);
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
  pWriter->pFile->offset = pWriter->pFile->size;
  pWriter->pFile->size += size;

  tsdbFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d write del idx failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  tsdbFree(pBuf);
  return code;
}

int32_t tsdbUpdateDelFileHdr(SDelFWriter *pWriter, uint8_t **ppBuf) {
  int32_t  code = 0;
  uint8_t *pBuf = NULL;
  int64_t  size = TSDB_FHDR_SIZE;
  int64_t  n;

  // alloc
  if (!ppBuf) ppBuf = &pBuf;
  code = tsdbRealloc(ppBuf, size);
  if (code) goto _err;

  // build
  memset(*ppBuf, 0, size);
  n = tPutDelFile(*ppBuf, pWriter->pFile);
  taosCalcChecksumAppend(0, *ppBuf, size);

  ASSERT(n <= size - sizeof(TSCKSUM));

  // seek
  if (taosLSeekFile(pWriter->pWriteH, 0, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // write
  if (taosWriteFile(pWriter->pWriteH, *ppBuf, size) < size) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  tsdbFree(pBuf);
  return code;

_err:
  tsdbError("vgId:%d update del file hdr failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  tsdbFree(pBuf);
  return code;
}

// SDelFReader ====================================================
struct SDelFReader {
  STsdb    *pTsdb;
  SDelFile *pFile;
  TdFilePtr pReadH;
};

int32_t tsdbDelFReaderOpen(SDelFReader **ppReader, SDelFile *pFile, STsdb *pTsdb, uint8_t **ppBuf) {
  int32_t      code = 0;
  char        *fname = NULL;  // todo
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
  pDelFReader->pFile = pFile;
  pDelFReader->pReadH = taosOpenFile(fname, TD_FILE_READ);
  if (pDelFReader == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    taosMemoryFree(pDelFReader);
    goto _err;
  }

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

int32_t tsdbReadDelData(SDelFReader *pReader, SDelIdx *pDelIdx, SMapData *pDelDataMap, uint8_t **ppBuf) {
  int32_t  code = 0;
  int64_t  n;
  uint32_t delimiter;
  tb_uid_t suid;
  tb_uid_t uid;

  // seek
  if (taosLSeekFile(pReader->pReadH, pDelIdx->offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // alloc
  if (!ppBuf) ppBuf = &pDelDataMap->pBuf;
  code = tsdbRealloc(ppBuf, pDelIdx->size);
  if (code) goto _err;

  // read
  n = taosReadFile(pReader->pReadH, *ppBuf, pDelIdx->size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n < pDelIdx->size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // check
  if (!taosCheckChecksumWhole(*ppBuf, pDelIdx->size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // // decode
  n = 0;
  n += tGetU32(*ppBuf + n, &delimiter);
  ASSERT(delimiter == TSDB_FILE_DLMT);
  n += tGetI64(*ppBuf + n, &suid);
  ASSERT(suid == pDelIdx->suid);
  n += tGetI64(*ppBuf + n, &uid);
  ASSERT(uid == pDelIdx->uid);
  n += tGetMapData(*ppBuf + n, pDelDataMap);
  ASSERT(n + sizeof(TSCKSUM) == pDelIdx->size);

  return code;

_err:
  tsdbError("vgId:%d read del data failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbReadDelIdx(SDelFReader *pReader, SMapData *pDelIdxMap, uint8_t **ppBuf) {
  int32_t  code = 0;
  int32_t  n;
  int64_t  offset = pReader->pFile->offset;
  int64_t  size = pReader->pFile->size - offset;
  uint32_t delimiter;

  ASSERT(ppBuf && *ppBuf);

  // seek
  if (taosLSeekFile(pReader->pReadH, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // alloc
  if (!ppBuf) ppBuf = &pDelIdxMap->pBuf;
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
  n += tGetMapData(*ppBuf + n, pDelIdxMap);
  ASSERT(n + sizeof(TSCKSUM) == size);

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

int32_t tsdbReadBlockData(SDataFReader *pReader, SBlockIdx *pBlockIdx, SBlock *pBlock, SBlockData *pBlockData,
                          int16_t *aColId, int32_t nCol, uint8_t **ppBuf1, uint8_t **ppBuf2) {
  int32_t code = 0;
  // TODO
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

int32_t tsdbUpdateDFileSetHeader(SDataFWriter *pWriter, uint8_t **ppBuf) {
  int32_t    code = 0;
  int64_t    size = TSDB_FHDR_SIZE;
  int64_t    n;
  uint8_t   *pBuf = NULL;
  SHeadFile *pHeadFile = &pWriter->wSet.fHead;
  SDataFile *pDataFile = &pWriter->wSet.fData;
  SLastFile *pLastFile = &pWriter->wSet.fLast;
  SSmaFile  *pSmaFile = &pWriter->wSet.fSma;

  // alloc
  if (!ppBuf) ppBuf = &pBuf;
  code = tsdbRealloc(ppBuf, size);
  if (code) goto _err;

  // head ==============
  // build
  memset(*ppBuf, 0, size);
  tPutDataFileHdr(*ppBuf, &pWriter->wSet, TSDB_HEAD_FILE);
  taosCalcChecksumAppend(0, *ppBuf, size);

  // seek
  if (taosLSeekFile(pWriter->pHeadFD, 0, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // write
  n = taosWriteFile(pWriter->pHeadFD, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // data ==============
  memset(*ppBuf, 0, size);
  tPutDataFileHdr(*ppBuf, &pWriter->wSet, TSDB_DATA_FILE);
  taosCalcChecksumAppend(0, *ppBuf, size);

  // seek
  if (taosLSeekFile(pWriter->pDataFD, 0, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // write
  n = taosWriteFile(pWriter->pDataFD, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // last ==============
  memset(*ppBuf, 0, size);
  tPutDataFileHdr(*ppBuf, &pWriter->wSet, TSDB_LAST_FILE);
  taosCalcChecksumAppend(0, *ppBuf, size);

  // seek
  if (taosLSeekFile(pWriter->pLastFD, 0, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // write
  n = taosWriteFile(pWriter->pLastFD, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // sma ==============
  memset(*ppBuf, 0, size);
  tPutDataFileHdr(*ppBuf, &pWriter->wSet, TSDB_SMA_FILE);
  taosCalcChecksumAppend(0, *ppBuf, size);

  // seek
  if (taosLSeekFile(pWriter->pSmaFD, 0, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // write
  n = taosWriteFile(pWriter->pSmaFD, *ppBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  tsdbFree(pBuf);
  return code;

_err:
  tsdbFree(pBuf);
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
  n += tPutMapData(*ppBuf, mBlockIdx);
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
                           SBlockIdx *pBlockIdx, SBlock *pBlock) {
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
  pSubBlock->ksize = 0;
  if (pBlock->cmprAlg == NO_COMPRESSION) {
    // TSKEY
    size = sizeof(TSKEY) * pBlockData->nRow;
    n = taosWriteFile(pFileFD, pBlockData->aTSKEY, size);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    pSubBlock->ksize += size;
    cksm = taosCalcChecksum(0, (uint8_t *)pBlockData->aTSKEY, size);

    // version
    size = sizeof(int64_t) * pBlockData->nRow;
    n = taosWriteFile(pFileFD, pBlockData->aVersion, size);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    pSubBlock->ksize += size;
    cksm = taosCalcChecksum(cksm, (uint8_t *)pBlockData->aVersion, size);

    // cksm
    size = sizeof(cksm);
    n = taosWriteFile(pFileFD, (uint8_t *)&cksm, size);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    pSubBlock->ksize += size;
  } else {
    ASSERT(pBlock->cmprAlg == ONE_STAGE_COMP || pBlock->cmprAlg == TWO_STAGE_COMP);

    size = (sizeof(TSKEY) + sizeof(int64_t)) * pBlockData->nRow + COMP_OVERFLOW_BYTES * 2 + sizeof(TSCKSUM);

    code = tsdbRealloc(ppBuf1, size);
    if (code) goto _err;

    if (pBlock->cmprAlg == TWO_STAGE_COMP) {
      code = tsdbRealloc(ppBuf2, size);
      if (code) goto _err;
    }

    // TSKEY
    n = tsCompressTimestamp((char *)pBlockData->aTSKEY, sizeof(TSKEY) * pBlockData->nRow, pBlockData->nRow, *ppBuf1,
                            size, pBlock->cmprAlg, *ppBuf2, size);
    if (n <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }
    pSubBlock->ksize += n;

    // version
    n = tsCompressBigint((char *)pBlockData->aVersion, sizeof(int64_t) * pBlockData->nRow, pBlockData->nRow,
                         *ppBuf1 + pSubBlock->ksize, size - pSubBlock->ksize, pBlock->cmprAlg, *ppBuf2, size);
    if (n <= 0) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _err;
    }
    pSubBlock->ksize += n;

    // cksm
    pSubBlock->ksize += sizeof(TSCKSUM);
    ASSERT(pSubBlock->ksize <= size);
    taosCalcChecksumAppend(0, *ppBuf1, pSubBlock->ksize);

    // write
    n = taosWriteFile(pFileFD, *ppBuf1, pSubBlock->ksize);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
  }
  pSubBlock->bsize += pSubBlock->ksize;

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
      pBlockCol->size = 0;

      // bitmap
      if (pColData->flag != HAS_VALUE) {
        // optimize bitmap storage (todo)
        n = taosWriteFile(pFileFD, pColData->pBitMap, BIT2_SIZE(pBlockData->nRow));
        if (n < 0) {
          code = TAOS_SYSTEM_ERROR(errno);
          goto _err;
        }

        cksm = taosCalcChecksum(cksm, pColData->pBitMap, n);
        pBlockCol->size += n;
      }

      // data
      if (pBlock->cmprAlg == NO_COMPRESSION) {
        // data
        n = taosWriteFile(pFileFD, pColData->pData, pColData->nData);
        if (n < 0) {
          code = TAOS_SYSTEM_ERROR(errno);
          goto _err;
        }
        pBlockCol->size += n;

        // checksum
        cksm = taosCalcChecksum(cksm, pColData->pData, pColData->nData);
        n = taosWriteFile(pFileFD, &cksm, sizeof(cksm));
        if (n < 0) {
          code = TAOS_SYSTEM_ERROR(errno);
          goto _err;
        }
        pBlockCol->size += n;
      } else {
        size = pColData->nData + COMP_OVERFLOW_BYTES + sizeof(TSCKSUM);

        code = tsdbRealloc(ppBuf1, size);
        if (code) goto _err;

        if (pBlock->cmprAlg == TWO_STAGE_COMP) {
          code = tsdbRealloc(ppBuf2, size);
          if (code) goto _err;
        }

        // data
        n = tDataTypes[pColData->type].compFunc(pColData->pData, pColData->nData, pBlockData->nRow, *ppBuf1, size,
                                                pBlock->cmprAlg, *ppBuf2, size);
        if (n <= 0) {
          code = TSDB_CODE_COMPRESS_ERROR;
          goto _err;
        }

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

        pBlockCol->size += n;
      }

      // state
      offset += pBlockCol->size;
      pSubBlock->bsize += pBlockCol->size;
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
