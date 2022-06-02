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

#include "vnodeInt.h"

struct SVSnapshotReader {
  SVnode              *pVnode;
  int64_t              sver;
  int64_t              ever;
  int8_t               isMetaEnd;
  int8_t               isTsdbEnd;
  SMetaSnapshotReader *pMetaReader;
  STsdbSnapshotReader *pTsdbReader;
  void                *pData;
  int32_t              nData;
};

int32_t vnodeSnapshotReaderOpen(SVnode *pVnode, SVSnapshotReader **ppReader, int64_t sver, int64_t ever) {
  SVSnapshotReader *pReader = NULL;

  pReader = (SVSnapshotReader *)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pVnode = pVnode;
  pReader->sver = sver;
  pReader->ever = ever;
  pReader->isMetaEnd = 0;
  pReader->isTsdbEnd = 0;

  if (metaSnapshotReaderOpen(pVnode->pMeta, &pReader->pMetaReader, sver, ever) < 0) {
    taosMemoryFree(pReader);
    goto _err;
  }

  if (tsdbSnapshotReaderOpen(pVnode->pTsdb, &pReader->pTsdbReader, sver, ever) < 0) {
    metaSnapshotReaderClose(pReader->pMetaReader);
    taosMemoryFree(pReader);
    goto _err;
  }

_exit:
  *ppReader = pReader;
  return 0;

_err:
  *ppReader = NULL;
  return -1;
}

int32_t vnodeSnapshotReaderClose(SVSnapshotReader *pReader) {
  if (pReader) {
    vnodeFree(pReader->pData);
    tsdbSnapshotReaderClose(pReader->pTsdbReader);
    metaSnapshotReaderClose(pReader->pMetaReader);
    taosMemoryFree(pReader);
  }
  return 0;
}

int32_t vnodeSnapshotRead(SVSnapshotReader *pReader, const void **ppData, uint32_t *nData) {
  int32_t code = 0;

  if (!pReader->isMetaEnd) {
    code = metaSnapshotRead(pReader->pMetaReader, &pReader->pData, &pReader->nData);
    if (code) {
      if (code == TSDB_CODE_VND_READ_END) {
        pReader->isMetaEnd = 1;
      } else {
        return code;
      }
    } else {
      *ppData = pReader->pData;
      *nData = pReader->nData;
      return code;
    }
  }

  if (!pReader->isTsdbEnd) {
    code = tsdbSnapshotRead(pReader->pTsdbReader, &pReader->pData, &pReader->nData);
    if (code) {
      if (code == TSDB_CODE_VND_READ_END) {
        pReader->isTsdbEnd = 1;
      } else {
        return code;
      }
    } else {
      *ppData = pReader->pData;
      *nData = pReader->nData;
      return code;
    }
  }

  code = TSDB_CODE_VND_READ_END;
  return code;
}