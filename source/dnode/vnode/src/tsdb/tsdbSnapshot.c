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

struct STsdbSnapshotReader {
  STsdb*  pTsdb;
  int64_t sver;
  int64_t ever;
  // for data file
  SDataFReader* pDataFReader;
  // for del file
  SDelFReader* pDelFReader;
};

typedef struct STsdbSnapshotWriter {
  STsdb*  pTsdb;
  int64_t sver;
  int64_t ever;
  // for data file
  SDataFWriter* pDataFWriter;
  // for del file
  SDelFWriter* pDelFWriter;
} STsdbSnapshotWriter;

int32_t tsdbSnapshotReaderOpen(STsdb* pTsdb, STsdbSnapshotReader** ppReader, int64_t sver, int64_t ever) {
  int32_t              code = 0;
  STsdbSnapshotReader* pReader = NULL;

  // alloc
  pReader = (STsdbSnapshotReader*)taosMemoryCalloc(1, sizeof(*pReader));
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

int32_t tsdbSnapshotRead(STsdbSnapshotReader* pReader, void** ppData, uint32_t* nData) {
  int32_t code = 0;
  // TODO
  return code;

_err:
  tsdbError("vgId:%d snapshot read failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbSnapshotReaderClose(STsdbSnapshotReader* pReader) {
  int32_t code = 0;
  taosMemoryFree(pReader);
  return code;
}
