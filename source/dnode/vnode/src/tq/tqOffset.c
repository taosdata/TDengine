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
#define _DEFAULT_SOURCE

#include "tq.h"

static char* tqOffsetBuildFName(const char* path, int32_t fVer) {
  int32_t len = strlen(path);
  char*   fname = taosMemoryCalloc(1, len + 40);
  if (fname == NULL) {
    return NULL;
  }
  snprintf(fname, len + 40, "%s/offset-ver%d", path, fVer);
  return fname;
}

int32_t tqOffsetRestoreFromFile(STQ * pTq) {
  int32_t   code = -1;
  void*     pMemBuf = NULL;
  SDecoder  decoder = {0};
  char*     fname   = tqOffsetBuildFName(pTq->path, 0);
  TdFilePtr pFile   = taosOpenFile(fname, TD_FILE_READ);
  if (pFile == NULL || fname == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }

  int64_t ret = 0;
  int32_t size = 0;
  while (1) {
    if ((ret = taosReadFile(pFile, &size, INT_BYTES)) != INT_BYTES) {
      if (ret != 0) {
        terrno = TSDB_CODE_INVALID_MSG;
      }else {
        code = 0;
      }
      goto END;
    }

    size = htonl(size);
    pMemBuf = taosMemoryCalloc(1, size);
    if (pMemBuf == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }

    if (taosReadFile(pFile, pMemBuf, size) != size) {
      terrno = TSDB_CODE_INVALID_MSG;
      goto END;
    }

    STqOffset offset;
    tDecoderInit(&decoder, pMemBuf, size);
    if (tDecodeSTqOffset(&decoder, &offset) < 0) {
      terrno = TSDB_CODE_INVALID_MSG;
      goto END;
    }

    if (taosHashPut(pTq->pOffset, offset.subKey, strlen(offset.subKey), &offset, sizeof(STqOffset)) < 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }

    tDecoderClear(&decoder);
    taosMemoryFree(pMemBuf);
    pMemBuf = NULL;
  }

END:
  taosCloseFile(&pFile);
  taosMemoryFree(fname);
  taosMemoryFree(pMemBuf);
  tDecoderClear(&decoder);

  return code;
}
