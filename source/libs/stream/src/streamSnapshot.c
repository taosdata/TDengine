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

#include "streamSnapshot.h"
#include "rocksdb/c.h"
#include "tcommon.h"

struct SStreamSnapHandle {
  void*   handle;
  SArray* fileList;
};

struct SStreamSnapReader {
  void*   pMeta;
  int64_t sver;
  int64_t ever;
};

// SMetaSnapWriter ========================================
struct StreamSnapWriter {
  void*   pMeta;
  int64_t sver;
  int64_t ever;
};

void streamSnapHandleInit(SStreamSnapHandle* handle) {
  // impl later
  handle->fileList = taosArrayInit(32, sizeof(void*));
  return;
}
void streamSnapHandleDestroy(SStreamSnapHandle* handle) {
  for (int i = 0; handle && i < taosArrayGetSize(handle->fileList); i++) {
    taosMemoryFree(taosArrayGetP(handle->fileList, i));
  }
  return;
}

int32_t streamSnapReaderOpen(void* pMeta, int64_t sver, int64_t ever, void** ppReader) {
  // impl later
  rocksdb_t* db = NULL;

  return 0;
}
int32_t streamSnapReaderClose(void** ppReader) {
  // impl later
  return 0;
}
int32_t streamSnapRead(void* pReader, uint8_t** ppData) {
  // impl later

  return 0;
}
// SMetaSnapWriter ========================================
int32_t streamSnapWriterOpen(void* pMeta, int64_t sver, int64_t ever, void** ppWriter) {
  // impl later
  return 0;
}
int32_t streamSnapWrite(void* pWriter, uint8_t* pData, uint32_t nData) {
  // impl later
  return 0;
}
int32_t streamSnapWriterClose(void** ppWriter, int8_t rollback) { return 0; }
