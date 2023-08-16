/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "indexFstFile.h"
#include "indexComm.h"
#include "indexFstUtil.h"
#include "indexInt.h"
#include "indexUtil.h"
#include "os.h"
#include "osDef.h"
#include "tutil.h"

static int32_t kBlockSize = 4096;

typedef struct {
  int32_t blockId;
  int32_t nread;
  char    buf[0];
} SDataBlock;

static void deleteDataBlockFromLRU(const void* key, size_t keyLen, void* value, void* ud) {
  (void)ud;
  taosMemoryFree(value);
}

static FORCE_INLINE void idxGenLRUKey(char* buf, const char* path, int32_t blockId) {
  char* p = buf;
  SERIALIZE_STR_VAR_TO_BUF(p, path, strlen(path));
  SERIALIZE_VAR_TO_BUF(p, '_', char);
  idxInt2str(blockId, p, 0);
  return;
}
static FORCE_INLINE int idxFileCtxDoWrite(IFileCtx* ctx, uint8_t* buf, int len) {
  int tlen = len;
  if (ctx->type == TFILE) {
    int32_t cap = ctx->file.wBufCap;
    if (len + ctx->file.wBufOffset >= cap) {
      int32_t nw = cap - ctx->file.wBufOffset;
      memcpy(ctx->file.wBuf + ctx->file.wBufOffset, buf, nw);
      taosWriteFile(ctx->file.pFile, ctx->file.wBuf, cap);

      memset(ctx->file.wBuf, 0, cap);
      ctx->file.wBufOffset = 0;

      len -= nw;
      buf += nw;

      nw = (len / cap) * cap;
      if (nw != 0) {
        taosWriteFile(ctx->file.pFile, buf, nw);
      }

      len -= nw;
      buf += nw;
      if (len != 0) {
        memcpy(ctx->file.wBuf, buf, len);
      }
      ctx->file.wBufOffset += len;
    } else {
      memcpy(ctx->file.wBuf + ctx->file.wBufOffset, buf, len);
      ctx->file.wBufOffset += len;
    }

  } else {
    memcpy(ctx->mem.buf + ctx->offset, buf, len);
  }
  ctx->offset += tlen;
  return tlen;
}
static FORCE_INLINE int idxFileCtxDoRead(IFileCtx* ctx, uint8_t* buf, int len) {
  int nRead = 0;
  if (ctx->type == TFILE) {
#ifdef USE_MMAP
    nRead = len < ctx->file.size ? len : ctx->file.size;
    memcpy(buf, ctx->file.ptr, nRead);
#else
    nRead = taosReadFile(ctx->file.pFile, buf, len);
#endif
  } else {
    memcpy(buf, ctx->mem.buf + ctx->offset, len);
  }
  ctx->offset += nRead;

  return nRead;
}
static int idxFileCtxDoReadFrom(IFileCtx* ctx, uint8_t* buf, int len, int32_t offset) {
  int32_t total = 0, nread = 0;
  int32_t blkId = offset / kBlockSize;
  int32_t blkOffset = offset % kBlockSize;
  int32_t blkLeft = kBlockSize - blkOffset;

  if (offset >= ctx->file.size) return 0;

  do {
    char key[1024] = {0};
    ASSERT(strlen(ctx->file.buf) + 1 + 64 < sizeof(key));
    idxGenLRUKey(key, ctx->file.buf, blkId);
    LRUHandle* h = taosLRUCacheLookup(ctx->lru, key, strlen(key));

    if (h) {
      SDataBlock* blk = taosLRUCacheValue(ctx->lru, h);
      nread = TMIN(blkLeft, len);
      memcpy(buf + total, blk->buf + blkOffset, nread);
      taosLRUCacheRelease(ctx->lru, h, false);
    } else {
      int32_t left = ctx->file.size - offset;
      if (left < kBlockSize) {
        nread = TMIN(left, len);
        int32_t bytes = taosPReadFile(ctx->file.pFile, buf + total, nread, offset);
        ASSERTS(bytes == nread, "index read incomplete data");
        if (bytes != nread) break;

        total += bytes;
        return total;
      } else {
        int32_t cacheMemSize = sizeof(SDataBlock) + kBlockSize;

        SDataBlock* blk = taosMemoryCalloc(1, cacheMemSize);
        blk->blockId = blkId;
        blk->nread = taosPReadFile(ctx->file.pFile, blk->buf, kBlockSize, blkId * kBlockSize);
        ASSERTS(blk->nread <= kBlockSize, "index read incomplete data");
        if (blk->nread < kBlockSize && blk->nread < len) {
          taosMemoryFree(blk);
          break;
        }

        nread = TMIN(blkLeft, len);
        memcpy(buf + total, blk->buf + blkOffset, nread);

        LRUStatus s = taosLRUCacheInsert(ctx->lru, key, strlen(key), blk, cacheMemSize, deleteDataBlockFromLRU, NULL,
                                         TAOS_LRU_PRIORITY_LOW, NULL);
        if (s != TAOS_LRU_STATUS_OK) {
          return -1;
        }
      }
    }
    total += nread;
    len -= nread;
    offset += nread;

    blkId = offset / kBlockSize;
    blkOffset = offset % kBlockSize;
    blkLeft = kBlockSize - blkOffset;

  } while (len > 0);
  return total;
}
static FORCE_INLINE int idxFileCtxGetSize(IFileCtx* ctx) {
  if (ctx->type == TFILE) {
    if (ctx->file.readOnly == false) {
      return ctx->offset;
    } else {
      int64_t file_size = 0;
      taosStatFile(ctx->file.buf, &file_size, NULL, NULL);
      return (int)file_size;
    }
  }
  return 0;
}
static FORCE_INLINE int idxFileCtxDoFlush(IFileCtx* ctx) {
  if (ctx->type == TFILE) {
    if (ctx->file.wBufOffset > 0) {
      int32_t nw = taosWriteFile(ctx->file.pFile, ctx->file.wBuf, ctx->file.wBufOffset);
      ctx->file.wBufOffset = 0;
    }
    int ret = taosFsyncFile(ctx->file.pFile);
    UNUSED(ret);
  } else {
    // do nothing
  }
  return 1;
}

IFileCtx* idxFileCtxCreate(WriterType type, const char* path, bool readOnly, int32_t capacity) {
  int       code = 0;
  IFileCtx* ctx = taosMemoryCalloc(1, sizeof(IFileCtx));
  if (ctx == NULL) {
    return NULL;
  }
  ctx->type = type;
  if (ctx->type == TFILE) {
    // ugly code, refactor later
    ctx->file.readOnly = readOnly;
    memcpy(ctx->file.buf, path, strlen(path));
    if (readOnly == false) {
      ctx->file.pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);

      code = taosFtruncateFile(ctx->file.pFile, 0);
      UNUSED(code);

      code = taosStatFile(path, &ctx->file.size, NULL, NULL);
      UNUSED(code);

      ctx->file.wBufOffset = 0;
      ctx->file.wBufCap = kBlockSize * 4;
      ctx->file.wBuf = taosMemoryCalloc(1, ctx->file.wBufCap);
    } else {
      ctx->file.pFile = taosOpenFile(path, TD_FILE_READ);
      code = taosFStatFile(ctx->file.pFile, &ctx->file.size, NULL);
      UNUSED(code);

      ctx->file.wBufOffset = 0;

#ifdef USE_MMAP
      ctx->file.ptr = (char*)tfMmapReadOnly(ctx->file.pFile, ctx->file.size);
#endif
    }
    if (ctx->file.pFile == NULL) {
      indexError("failed to open file, error %d", errno);
      goto END;
    }
  } else if (ctx->type == TMEMORY) {
    ctx->mem.buf = taosMemoryCalloc(1, sizeof(char) * capacity);
    ctx->mem.cap = capacity;
  }

  ctx->write = idxFileCtxDoWrite;
  ctx->read = idxFileCtxDoRead;
  ctx->flush = idxFileCtxDoFlush;
  ctx->readFrom = idxFileCtxDoReadFrom;
  ctx->size = idxFileCtxGetSize;

  ctx->offset = 0;
  ctx->limit = capacity;

  return ctx;
END:
  if (ctx->type == TMEMORY) {
    taosMemoryFree(ctx->mem.buf);
  }
  taosMemoryFree(ctx);
  return NULL;
}
void idxFileCtxDestroy(IFileCtx* ctx, bool remove) {
  if (ctx->type == TMEMORY) {
    taosMemoryFree(ctx->mem.buf);
  } else {
    if (ctx->file.wBufOffset > 0) {
      int32_t nw = taosWriteFile(ctx->file.pFile, ctx->file.wBuf, ctx->file.wBufOffset);
      ctx->file.wBufOffset = 0;
    }
    ctx->flush(ctx);
    taosMemoryFreeClear(ctx->file.wBuf);
    taosCloseFile(&ctx->file.pFile);
    if (ctx->file.readOnly) {
#ifdef USE_MMAP
      munmap(ctx->file.ptr, ctx->file.size);
#endif
    }
    if (remove) {
      unlink(ctx->file.buf);
    }
  }
  taosMemoryFree(ctx);
}

IdxFstFile* idxFileCreate(void* wrt) {
  IdxFstFile* cw = taosMemoryCalloc(1, sizeof(IdxFstFile));
  if (cw == NULL) {
    return NULL;
  }

  cw->wrt = wrt;
  return cw;
}
void idxFileDestroy(IdxFstFile* cw) {
  idxFileFlush(cw);
  taosMemoryFree(cw);
}

int idxFileWrite(IdxFstFile* write, uint8_t* buf, uint32_t len) {
  if (write == NULL) {
    return 0;
  }
  // update checksum
  IFileCtx* ctx = write->wrt;
  int       nWrite = ctx->write(ctx, buf, len);
  ASSERTS(nWrite == len, "index write incomplete data");
  if (nWrite != len) {
    return -1;
  }
  write->count += len;

  write->summer = taosCalcChecksum(write->summer, buf, len);
  return len;
}

int idxFileRead(IdxFstFile* write, uint8_t* buf, uint32_t len) {
  if (write == NULL) {
    return 0;
  }
  IFileCtx* ctx = write->wrt;
  return ctx->read(ctx, buf, len);
}

uint32_t idxFileMaskedCheckSum(IdxFstFile* write) {
  //////
  return write->summer;
}

int idxFileFlush(IdxFstFile* write) {
  IFileCtx* ctx = write->wrt;
  ctx->flush(ctx);
  return 1;
}

void idxFilePackUintIn(IdxFstFile* writer, uint64_t n, uint8_t nBytes) {
  uint8_t* buf = taosMemoryCalloc(8, sizeof(uint8_t));
  for (uint8_t i = 0; i < nBytes; i++) {
    buf[i] = (uint8_t)n;
    n = n >> 8;
  }
  idxFileWrite(writer, buf, nBytes);
  taosMemoryFree(buf);
  return;
}

uint8_t idxFilePackUint(IdxFstFile* writer, uint64_t n) {
  uint8_t nBytes = packSize(n);
  idxFilePackUintIn(writer, n, nBytes);
  return nBytes;
}
