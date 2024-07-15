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

int32_t tqBuildFName(char** data, const char* path, char* name) {
  int32_t len = strlen(path) + strlen(name) + 2;
  char*   fname = taosMemoryCalloc(1, len);
  if(fname == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = snprintf(fname, len, "%s%s%s", path, TD_DIRSEP, name);
  if (code < 0){
    code = TAOS_SYSTEM_ERROR(errno);
    taosMemoryFree(fname);
    return code;
  }
  *data = fname;
  return TDB_CODE_SUCCESS;
}

int32_t tqOffsetRestoreFromFile(SHashObj* pOffset, char* name) {
  int32_t   code = TDB_CODE_SUCCESS;
  void*     pMemBuf = NULL;
  SDecoder  decoder = {0};

  TdFilePtr pFile = taosOpenFile(name, TD_FILE_READ);
  if (pFile == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto END;
  }

  int64_t ret = 0;
  int32_t size = 0;
  while (1) {
    if ((ret = taosReadFile(pFile, &size, INT_BYTES)) != INT_BYTES) {
      if (ret != 0) {
        code = TSDB_CODE_INVALID_MSG;
      }
      goto END;
    }

    size = htonl(size);
    pMemBuf = taosMemoryCalloc(1, size);
    if (pMemBuf == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }

    if (taosReadFile(pFile, pMemBuf, size) != size) {
      terrno = TSDB_CODE_INVALID_MSG;
      goto END;
    }

    STqOffset offset;
    tDecoderInit(&decoder, pMemBuf, size);
    if (tDecodeSTqOffset(&decoder, &offset) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto END;
    }

    if (taosHashPut(pOffset, offset.subKey, strlen(offset.subKey), &offset, sizeof(STqOffset)) < 0) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }

    tDecoderClear(&decoder);
    taosMemoryFree(pMemBuf);
    pMemBuf = NULL;
  }

END:
  taosCloseFile(&pFile);
  taosMemoryFree(pMemBuf);
  tDecoderClear(&decoder);

  return code;
}
