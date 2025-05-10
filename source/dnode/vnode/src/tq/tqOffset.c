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
  int32_t code = 0;
  int32_t lino = 0;
  char*   fname = NULL;
  TSDB_CHECK_NULL(data, code, lino, END, TSDB_CODE_INVALID_MSG);
  TSDB_CHECK_NULL(path, code, lino, END, TSDB_CODE_INVALID_MSG);
  TSDB_CHECK_NULL(name, code, lino, END, TSDB_CODE_INVALID_MSG);
  int32_t len = strlen(path) + strlen(name) + 2;
  fname = taosMemoryCalloc(1, len);
  TSDB_CHECK_NULL(fname, code, lino, END, terrno);
  (void)tsnprintf(fname, len, "%s%s%s", path, TD_DIRSEP, name);

  *data = fname;
  fname = NULL;

END:
  if (code != 0) {
    tqError("%s failed at %d since %s", __func__, lino, tstrerror(code));
  }
  taosMemoryFree(fname);
  return code;
}

int32_t tqCommitOffset(void* p) {
  STQ*    pTq = (STQ*)p;
  int32_t code = TDB_CODE_SUCCESS;
  void*   pIter = NULL;
  int32_t vgId = pTq->pVnode != NULL ? pTq->pVnode->config.vgId : -1;
  while ((pIter = taosHashIterate(pTq->pOffset, pIter))) {
    STqOffset* offset = (STqOffset*)pIter;
    int32_t    ret = tqMetaSaveOffset(pTq, offset);
    if (ret != TDB_CODE_SUCCESS) {
      code = ret;
      tqError("tq commit offset error subkey:%s, vgId:%d", offset->subKey, vgId);
    } else {
      if (offset->val.type == TMQ_OFFSET__LOG) {
        tqInfo("tq commit offset success subkey:%s vgId:%d, offset(type:log) version:%" PRId64, offset->subKey, vgId,
               offset->val.version);
      }
    }
  }
  return code;
}

int32_t tqOffsetRestoreFromFile(STQ* pTq, char* name) {
  int32_t    code = TDB_CODE_SUCCESS;
  int32_t    lino = 0;
  void*      pMemBuf = NULL;
  TdFilePtr  pFile = NULL;
  STqOffset* pOffset = NULL;
  void*      pIter = NULL;

  TSDB_CHECK_NULL(pTq, code, lino, END, TSDB_CODE_INVALID_MSG);
  TSDB_CHECK_NULL(name, code, lino, END, TSDB_CODE_INVALID_MSG);

  pFile = taosOpenFile(name, TD_FILE_READ);
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
    pOffset = &offset;
    code = taosHashPut(pTq->pOffset, pOffset->subKey, strlen(pOffset->subKey), pOffset, sizeof(STqOffset));
    TSDB_CHECK_CODE(code, lino, END);
    pOffset = NULL;

    tqInfo("tq: offset restore from file to tdb, size:%d, hash size:%d subkey:%s", total, taosHashGetSize(pTq->pOffset),
           offset.subKey);
    taosMemoryFree(pMemBuf);
    pMemBuf = NULL;
  }

  code = tqCommitOffset(pTq);
  TSDB_CHECK_CODE(code, lino, END);

END:
  if (code != 0) {
    tqError("%s failed at %d since %s", __func__, lino, tstrerror(code));
  }
  (void)taosCloseFile(&pFile);
  taosMemoryFree(pMemBuf);

  tDeleteSTqOffset(pOffset);
  taosHashCancelIterate(pTq->pOffset, pIter);

  return code;
}
