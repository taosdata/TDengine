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

#define TSDB_FHDR_SIZE 512
#define TSDB_FILE_DLMT ((uint32_t)0xF00AFA0F)

// SDFileSetWritter ====================================================
struct SDFileSetWritter {
  STsdb   *pTsdb;
  int32_t  szBuf1;
  uint8_t *pBuf1;
  int32_t  szBuf2;
  uint8_t *pBuf2;
};

// SDFileSetReader ====================================================
struct SDFileSetReader {
  STsdb   *pTsdb;
  int32_t  szBuf1;
  uint8_t *pBuf1;
  int32_t  szBuf2;
  uint8_t *pBuf2;
};

int32_t tsdbDFileSetReaderOpen(SDFileSetReader *pReader, STsdb *pTsdb, SDFileSet *pSet) {
  int32_t code = 0;

  memset(pReader, 0, sizeof(*pReader));
  pReader->pTsdb = pTsdb;

  return code;

_err:
  tsdbError("vgId:%d failed to open SDFileSetReader since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbDFileSetReaderClose(SDFileSetReader *pReader) {
  int32_t code = 0;

  taosMemoryFreeClear(pReader->pBuf1);
  taosMemoryFreeClear(pReader->pBuf2);

  return code;
}

int32_t tsdbLoadSBlockIdx(SDFileSetReader *pReader, SArray *pArray) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbLoadSBlockInfo(SDFileSetReader *pReader, SBlockIdx *pBlockIdx, SBlockInfo *pBlockInfo) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbLoadSBlockStatis(SDFileSetReader *pReader, SBlock *pBlock, SBlockStatis *pBlockStatis) {
  int32_t code = 0;
  // TODO
  return code;
}

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

  return code;

_err:
  tsdbError("vgId:%d failed to open del file writer since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbDelFWriterClose(SDelFWriter *pWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbWriteDelData(SDelFWriter *pWriter, SDelData *pDelData, uint8_t **ppBuf, SDelIdxItem *pItem) {
  int32_t code = 0;
  int64_t size;

  // TODO
  return code;
}

int32_t tsdbWriteDelIdx(SDelFWriter *pWriter, SDelIdx *pDelIdx, uint8_t **ppBuf) {
  int32_t code = 0;
  int64_t size;

  size = tPutDelIdx(NULL, pDelIdx) + sizeof(TSCKSUM);

  // alloc
  code = tsdbRealloc(ppBuf, size);
  if (code) {
    goto _err;
  }

  // encode
  tPutDelIdx(*ppBuf, pDelIdx);

  // checksum
  taosCalcChecksumAppend(0, *ppBuf, size);

  // write
  if (taosWriteFile(pWriter->pWriteH, *ppBuf, size) < size) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pWriter->pFile->offset = pWriter->pFile->size;
  pWriter->pFile->size += size;

  return code;

_err:
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

    if (taosReadFile(pDelFReader->pReadH, *ppBuf, TSDB_FHDR_SIZE) < TSDB_FHDR_SIZE) {
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
  *ppReader = NULL;
  return code;
}

int32_t tsdbDelFReaderClose(SDelFReader *pReader) {
  int32_t code = 0;

  if (pReader) {
    taosCloseFile(&pReader->pReadH);
    taosMemoryFree(pReader);
  }

  return code;
}

int32_t tsdbReadDelData(SDelFReader *pReader, SDelData *pDelData, uint8_t **ppBuf) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbReadDelIdx(SDelFReader *pReader, SDelIdx *pDelIdx, uint8_t **ppBuf) {
  int32_t code = 0;
  int64_t offset = pReader->pFile->offset;
  int64_t size = pReader->pFile->size - offset;

  // seek
  if (taosLSeekFile(pReader->pReadH, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // alloc
  code = tsdbRealloc(ppBuf, size);
  if (code) {
    goto _err;
  }

  // read
  if (taosReadFile(pReader->pReadH, *ppBuf, size) < size) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // check
  if (!taosCheckChecksumWhole(*ppBuf, size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  // decode
  int32_t n = tGetDelIdx(*ppBuf, pDelIdx);
  ASSERT(n == size - sizeof(TSCKSUM));
  ASSERT(pDelIdx->delimiter == TSDB_FILE_DLMT);
  ASSERT(pDelIdx->nOffset > 0 && pDelIdx->nData > 0);

  return code;

_err:
  tsdbError("vgId:%d failed to read del idx since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}