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

// STombstoneFileWriter ====================================================
struct STombstoneFileWriter {
  STsdb *pTsdb;
};

// STombstoneFileReader ====================================================
struct STombstoneFileReader {
  STsdb *pTsdb;
};
