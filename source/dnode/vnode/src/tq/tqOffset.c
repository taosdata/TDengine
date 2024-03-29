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

struct STqOffsetStore {
  STQ*      pTq;
  SHashObj* pHash;  // SHashObj<subscribeKey, offset>
  int8_t    needCommit;
};

char* tqOffsetBuildFName(const char* path, int32_t fVer) {
  int32_t len = strlen(path);
  char*   fname = taosMemoryCalloc(1, len + 40);
  snprintf(fname, len + 40, "%s/offset-ver%d", path, fVer);
  return fname;
}

int32_t tqOffsetRestoreFromFile(STqOffsetStore* pStore, const char* fname) {
  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_READ);
  if (pFile == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t vgId = TD_VID(pStore->pTq->pVnode);
  int64_t code = 0;
  int32_t size = 0;
  while (1) {
    if ((code = taosReadFile(pFile, &size, INT_BYTES)) != INT_BYTES) {
      if (code == 0) {
        break;
      } else {
        return -1;
      }
    }

    size = htonl(size);
    void*   pMemBuf = taosMemoryCalloc(1, size);
    if (pMemBuf == NULL) {
      tqError("vgId:%d failed to restore offset from file, since out of memory, malloc size:%d", vgId, size);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    if ((code = taosReadFile(pFile, pMemBuf, size)) != size) {
      taosMemoryFree(pMemBuf);
      return -1;
    }

    STqOffset offset;
    SDecoder  decoder;
    tDecoderInit(&decoder, pMemBuf, size);
    if (tDecodeSTqOffset(&decoder, &offset) < 0) {
      taosMemoryFree(pMemBuf);
      tDecoderClear(&decoder);
      return code;
    }

    tDecoderClear(&decoder);
    if (taosHashPut(pStore->pHash, offset.subKey, strlen(offset.subKey), &offset, sizeof(STqOffset)) < 0) {
      return -1;
    }

    // todo remove this
    if (offset.val.type == TMQ_OFFSET__LOG) {
      STqHandle* pHandle = taosHashGet(pStore->pTq->pHandle, offset.subKey, strlen(offset.subKey));
      if (pHandle) {
        if (walSetRefVer(pHandle->pRef, offset.val.version) < 0) {
//          tqError("vgId: %d, tq handle %s ref ver %" PRId64 "error", pStore->pTq->pVnode->config.vgId, pHandle->subKey,
//                  offset.val.version);
        }
      }
    }

    taosMemoryFree(pMemBuf);
  }

  taosCloseFile(&pFile);
  return TSDB_CODE_SUCCESS;
}

STqOffsetStore* tqOffsetOpen(STQ* pTq) {
  STqOffsetStore* pStore = taosMemoryCalloc(1, sizeof(STqOffsetStore));
  if (pStore == NULL) {
    return NULL;
  }

  pStore->pTq = pTq;
  pStore->needCommit = 0;
  pTq->pOffsetStore = pStore;

  pStore->pHash = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  if (pStore->pHash == NULL) {
    taosMemoryFree(pStore);
    return NULL;
  }

  char* fname = tqOffsetBuildFName(pStore->pTq->path, 0);
  if (tqOffsetRestoreFromFile(pStore, fname) < 0) {
    taosMemoryFree(fname);
    taosMemoryFree(pStore);
    return NULL;
  }

  taosMemoryFree(fname);
  return pStore;
}

void tqOffsetClose(STqOffsetStore* pStore) {
  if(pStore == NULL) return;
  tqOffsetCommitFile(pStore);
  taosHashCleanup(pStore->pHash);
  taosMemoryFree(pStore);
}

STqOffset* tqOffsetRead(STqOffsetStore* pStore, const char* subscribeKey) {
  return (STqOffset*)taosHashGet(pStore->pHash, subscribeKey, strlen(subscribeKey));
}

int32_t tqOffsetWrite(STqOffsetStore* pStore, const STqOffset* pOffset) {
  pStore->needCommit = 1;
  return taosHashPut(pStore->pHash, pOffset->subKey, strlen(pOffset->subKey), pOffset, sizeof(STqOffset));
}

int32_t tqOffsetDelete(STqOffsetStore* pStore, const char* subscribeKey) {
  return taosHashRemove(pStore->pHash, subscribeKey, strlen(subscribeKey));
}

int32_t tqOffsetCommitFile(STqOffsetStore* pStore) {
  if (!pStore->needCommit) {
    return 0;
  }

  // TODO file name should be with a newer version
  char*     fname = tqOffsetBuildFName(pStore->pTq->path, 0);
  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    const char* err = strerror(errno);
    tqError("vgId:%d, failed to open offset file %s, since %s", TD_VID(pStore->pTq->pVnode), fname, err);
    taosMemoryFree(fname);
    return -1;
  }

  taosMemoryFree(fname);

  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pStore->pHash, pIter);
    if (pIter == NULL) {
      break;
    }

    STqOffset* pOffset = (STqOffset*)pIter;
    int32_t    bodyLen;
    int32_t    code;
    tEncodeSize(tEncodeSTqOffset, pOffset, bodyLen, code);

    if (code < 0) {
      taosHashCancelIterate(pStore->pHash, pIter);
      return -1;
    }

    int32_t totLen = INT_BYTES + bodyLen;
    void*   buf = taosMemoryCalloc(1, totLen);
    void*   abuf = POINTER_SHIFT(buf, INT_BYTES);

    *(int32_t*)buf = htonl(bodyLen);
    SEncoder encoder;
    tEncoderInit(&encoder, abuf, bodyLen);
    tEncodeSTqOffset(&encoder, pOffset);

    // write file
    int64_t writeLen;
    if ((writeLen = taosWriteFile(pFile, buf, totLen)) != totLen) {
      tqError("write offset incomplete, len %d, write len %" PRId64, bodyLen, writeLen);
      taosHashCancelIterate(pStore->pHash, pIter);
      taosMemoryFree(buf);
      return -1;
    }

    taosMemoryFree(buf);
  }

  // close and rename file
  taosCloseFile(&pFile);
  pStore->needCommit = 0;
  return 0;
}
