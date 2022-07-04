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

struct STsdbSnapReader {
  STsdb*  pTsdb;
  int64_t sver;
  int64_t ever;
  // for data file
  SDataFReader* pDataFReader;
  // for del file
  SDelFReader* pDelFReader;
};

typedef struct STsdbSnapWriter {
  STsdb*  pTsdb;
  int64_t sver;
  int64_t ever;
  // for data file
  int32_t       iDFileSet;
  SDataFWriter* pDataFWriter;
  // for del file
  SDelFWriter* pDelFWriter;
} STsdbSnapWriter;

int32_t tsdbSnapReaderOpen(STsdb* pTsdb, STsdbSnapReader** ppReader, int64_t sver, int64_t ever) {
  int32_t          code = 0;
  STsdbSnapReader* pReader = NULL;

  // alloc
  pReader = (STsdbSnapReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pTsdb = pTsdb;
  pReader->sver = sver;
  pReader->ever = ever;

  *ppReader = pReader;
  return code;

_err:
  tsdbError("vgId:%d snapshot reader open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t tsdbSnapRead(STsdbSnapReader* pReader, void** ppData, uint32_t* nData) {
  int32_t code = 0;

  // read data file

  // read del file

  return code;

_err:
  tsdbError("vgId:%d snapshot read failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbSnapReaderClose(STsdbSnapReader* pReader) {
  int32_t code = 0;
  taosMemoryFree(pReader);
  return code;
}
