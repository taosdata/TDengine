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
  if (data == NULL || path == NULL || name == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  int32_t len = strlen(path) + strlen(name) + 2;
  char*   fname = taosMemoryCalloc(1, len);
  if(fname == NULL) {
    return terrno;
  }
  int32_t code = tsnprintf(fname, len, "%s%s%s", path, TD_DIRSEP, name);
  if (code < 0){
    code = TAOS_SYSTEM_ERROR(errno);
    taosMemoryFree(fname);
    return code;
  }
  *data = fname;
  return TDB_CODE_SUCCESS;
}

int32_t tqOffsetRestoreFromFile(STQ* pTq, char* name) {
  if (pTq == NULL || name == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  int32_t   code = TDB_CODE_SUCCESS;
  int32_t   lino = 0;
  void*     pMemBuf = NULL;

  TdFilePtr pFile = taosOpenFile(name, TD_FILE_READ);
  TSDB_CHECK_NULL(pFile, code, lino, END, TDB_CODE_SUCCESS);

  int64_t ret = 0;
  int32_t size = 0;
  int32_t total = 0;
  while (1) {
    if ((ret = taosReadFile(pFile, &size, INT_BYTES)) != INT_BYTES) {
      if (ret != 0) {
        code = TSDB_CODE_INVALID_MSG;
      }
      break;
    }
    total += INT_BYTES;
    size = htonl(size);
    TSDB_CHECK_CONDITION(size > 0, code, lino, END, TSDB_CODE_INVALID_MSG);

    pMemBuf = taosMemoryCalloc(1, size);
    TSDB_CHECK_NULL(pMemBuf, code, lino, END, terrno);
    TSDB_CHECK_CONDITION(taosReadFile(pFile, pMemBuf, size) == size, code, lino, END, TSDB_CODE_INVALID_MSG);

    total += size;
    STqOffset offset = {0};
    code = tqMetaDecodeOffsetInfo(&offset, pMemBuf, size);
    TSDB_CHECK_CODE(code, lino, END);
    code = taosHashPut(pTq->pOffset, offset.subKey, strlen(offset.subKey), &offset, sizeof(STqOffset));
    if (code != TDB_CODE_SUCCESS) {
      tDeleteSTqOffset(&offset);
      goto END;
    }

    tqInfo("tq: offset restore from file to tdb, size:%d, hash size:%d subkey:%s", total, taosHashGetSize(pTq->pOffset), offset.subKey);
    taosMemoryFree(pMemBuf);
    pMemBuf = NULL;
  }

  void *pIter = NULL;
  while ((pIter = taosHashIterate(pTq->pOffset, pIter))) {
    STqOffset* pOffset = (STqOffset*)pIter;
    code = tqMetaSaveOffset(pTq, pOffset);
    if(code != 0){
      taosHashCancelIterate(pTq->pOffset, pIter);
      goto END;
    }
  }

END:
  if (code != 0){
    tqError("%s failed at %d since %s", __func__, lino, tstrerror(code));
  }
  taosCloseFile(&pFile);
  taosMemoryFree(pMemBuf);

  return code;
}
