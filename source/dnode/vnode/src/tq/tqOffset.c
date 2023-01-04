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
  if (pFile != NULL) {
    STqOffsetHead head = {0};
    int64_t       code;

    while (1) {
      if ((code = taosReadFile(pFile, &head, sizeof(STqOffsetHead))) != sizeof(STqOffsetHead)) {
        if (code == 0) {
          break;
        } else {
          ASSERT(0);
          // TODO handle error
        }
      }
      int32_t size = htonl(head.size);
      void*   memBuf = taosMemoryCalloc(1, size);
      if (memBuf == NULL) {
        return -1;
      }
      if ((code = taosReadFile(pFile, memBuf, size)) != size) {
        taosMemoryFree(memBuf);
        return -1;
      }
      STqOffset offset;
      SDecoder  decoder;
      tDecoderInit(&decoder, memBuf, size);
      if (tDecodeSTqOffset(&decoder, &offset) < 0) {
        taosMemoryFree(memBuf);
        tDecoderClear(&decoder);
        return -1;
      }

      tDecoderClear(&decoder);
      if (taosHashPut(pStore->pHash, offset.subKey, strlen(offset.subKey), &offset, sizeof(STqOffset)) < 0) {
        return -1;
      }

      if (offset.val.type == TMQ_OFFSET__LOG) {
        STqHandle* pHandle = taosHashGet(pStore->pTq->pHandle, offset.subKey, strlen(offset.subKey));
        if (pHandle) {
          if (walRefVer(pHandle->pRef, offset.val.version) < 0) {
            tqError("vgId: %d, tq handle %s ref ver %" PRId64 "error", pStore->pTq->pVnode->config.vgId,
                    pHandle->subKey, offset.val.version);
          }
        }
      }

      taosMemoryFree(memBuf);
    }

    taosCloseFile(&pFile);
  }
  return 0;
}

STqOffsetStore* tqOffsetOpen(STQ* pTq) {
  STqOffsetStore* pStore = taosMemoryCalloc(1, sizeof(STqOffsetStore));
  if (pStore == NULL) {
    return NULL;
  }
  pStore->pTq = pTq;
  pStore->needCommit = 0;
  pTq->pOffsetStore = pStore;

  pStore->pHash = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
  if (pStore->pHash == NULL) {
    taosMemoryFree(pStore);
    return NULL;
  }
  char* fname = tqOffsetBuildFName(pStore->pTq->path, 0);
  if (tqOffsetRestoreFromFile(pStore, fname) < 0) {
    ASSERT(0);
  }
  taosMemoryFree(fname);
  return pStore;
}

void tqOffsetClose(STqOffsetStore* pStore) {
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
  if (!pStore->needCommit) return 0;
  // TODO file name should be with a newer version
  char*     fname = tqOffsetBuildFName(pStore->pTq->path, 0);
  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);

    int32_t     err = terrno;
    const char* errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char* sysErrStr = strerror(errno);
    tqError("vgId:%d, cannot open file %s when commit offset since %s", pStore->pTq->pVnode->config.vgId, fname,
            sysErrStr);
    taosMemoryFree(fname);
    return -1;
  }
  taosMemoryFree(fname);
  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pStore->pHash, pIter);
    if (pIter == NULL) break;
    STqOffset* pOffset = (STqOffset*)pIter;
    int32_t    bodyLen;
    int32_t    code;
    tEncodeSize(tEncodeSTqOffset, pOffset, bodyLen, code);
    ASSERT(code == 0);
    if (code < 0) {
      ASSERT(0);
      taosHashCancelIterate(pStore->pHash, pIter);
      return -1;
    }

    int32_t totLen = sizeof(STqOffsetHead) + bodyLen;
    void*   buf = taosMemoryCalloc(1, totLen);
    void*   abuf = POINTER_SHIFT(buf, sizeof(STqOffsetHead));

    ((STqOffsetHead*)buf)->size = htonl(bodyLen);
    SEncoder encoder;
    tEncoderInit(&encoder, abuf, bodyLen);
    tEncodeSTqOffset(&encoder, pOffset);
    // write file
    int64_t writeLen;
    if ((writeLen = taosWriteFile(pFile, buf, totLen)) != totLen) {
      ASSERT(0);
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
