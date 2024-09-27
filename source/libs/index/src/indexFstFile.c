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
  TAOS_UNUSED(ud);
  TAOS_UNUSED(key);
  TAOS_UNUSED(keyLen);
  taosMemoryFree(value);
}

static FORCE_INLINE void idxGenLRUKey(char* buf, const char* path, int32_t blockId) {
  char* p = buf;
  SERIALIZE_STR_VAR_TO_BUF(p, path, strlen(path));
  SERIALIZE_VAR_TO_BUF(p, '_', char);
  if (idxInt2str(blockId, p, 0) == NULL) {
    indexError("failed to generate lru key");
  }
  return;
}
static FORCE_INLINE int idxFileCtxDoWrite(IFileCtx* ctx, uint8_t* buf, int len) {
  int tlen = len;
  if (ctx->type == TFILE) {
    int32_t cap = ctx->file.wBufCap;
    if (len + ctx->file.wBufOffset >= cap) {
      int32_t nw = cap - ctx->file.wBufOffset;
      memcpy(ctx->file.wBuf + ctx->file.wBufOffset, buf, nw);
      if (taosWriteFile(ctx->file.pFile, ctx->file.wBuf, cap) < 0) {
        indexError("failed to write file:%s", ctx->file.buf);
      }

      memset(ctx->file.wBuf, 0, cap);
      ctx->file.wBufOffset = 0;

      len -= nw;
      buf += nw;

      nw = (len / cap) * cap;
      if (nw != 0) {
        if (taosWriteFile(ctx->file.pFile, buf, nw) < 0) {
          indexError("failed to write file:%s", ctx->file.buf);
        }
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
    if (strlen(ctx->file.buf) + 1 + 64 >= sizeof(key)) {
      return TSDB_CODE_INDEX_INVALID_FILE;
    }

    idxGenLRUKey(key, ctx->file.buf, blkId);
    LRUHandle* h = taosLRUCacheLookup(ctx->lru, key, strlen(key));

    if (h) {
      SDataBlock* blk = taosLRUCacheValue(ctx->lru, h);
      nread = TMIN(blkLeft, len);
      memcpy(buf + total, blk->buf + blkOffset, nread);
      if (taosLRUCacheRelease(ctx->lru, h, false)) {
        indexDebug("succ to release lru cache");
      }
    } else {
      int32_t left = ctx->file.size - offset;
      if (left < kBlockSize) {
        nread = TMIN(left, len);
        int32_t bytes = taosPReadFile(ctx->file.pFile, buf + total, nread, offset);
        if (bytes != nread) {
          total = TSDB_CODE_INDEX_INVALID_FILE;
          break;
        }

        total += bytes;
        return total;
      } else {
        int32_t cacheMemSize = sizeof(SDataBlock) + kBlockSize;

        SDataBlock* blk = taosMemoryCalloc(1, cacheMemSize);
        if (blk == NULL) {
          return terrno;
        }
        blk->blockId = blkId;
        blk->nread = taosPReadFile(ctx->file.pFile, blk->buf, kBlockSize, blkId * kBlockSize);
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
      if (taosStatFile(ctx->file.buf, &file_size, NULL, NULL) < 0) {
        indexError("failed to get file size since %s", tstrerror(terrno));
      }
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
      if (ctx->file.wBuf == NULL) {
        indexError("failed to allocate memory for write buffer");
        goto END;
      }
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
    if (ctx->mem.buf == NULL) {
      indexError("failed to allocate memory for memory buffer");
      goto END;
    }

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
    if ((ctx->flush(ctx)) < 0) {
      indexError("failed to flush file since %s", tstrerror(terrno));
    }
    taosMemoryFreeClear(ctx->file.wBuf);
    if ((taosCloseFile(&ctx->file.pFile)) < 0) {
      indexError("failed to close file since %s", tstrerror(terrno));
    }
    if (ctx->file.readOnly) {
#ifdef USE_MMAP
      munmap(ctx->file.ptr, ctx->file.size);
#endif
    }
    if (remove) {
      if ((unlink(ctx->file.buf)) < 0) {
        indexError("failed to unlink file since %s", tstrerror(terrno));
      }
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
  if (idxFileFlush(cw) < 0) {
    indexError("failed to flush file since %s", tstrerror(terrno));
  }
  taosMemoryFree(cw);
}

int32_t idxFileWrite(IdxFstFile* write, uint8_t* buf, uint32_t len) {
  int32_t code = 0;

  if (write == NULL) {
    return 0;
  }
  // update checksum
  IFileCtx* ctx = write->wrt;
  int       nWrite = ctx->write(ctx, buf, len);
  if (nWrite != len) {
    code = TAOS_SYSTEM_ERROR(errno);
    return code;
  }
  write->count += len;

  write->summer = taosCalcChecksum(write->summer, buf, len);
  return len;
}

int32_t idxFileRead(IdxFstFile* write, uint8_t* buf, uint32_t len) {
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
  if ((ctx->flush(ctx)) < 0) {
    indexError("failed to flush file since %s", tstrerror(terrno));
  }
  return 1;
}

void idxFilePackUintIn(IdxFstFile* writer, uint64_t n, uint8_t nBytes) {
  uint8_t* buf = taosMemoryCalloc(8, sizeof(uint8_t));
  if (buf == NULL) {
    indexError("failed to allocate memory for packing uint");
    return;
  }
  for (uint8_t i = 0; i < nBytes; i++) {
    buf[i] = (uint8_t)n;
    n = n >> 8;
  }
  if (idxFileWrite(writer, buf, nBytes) < 0) {
    indexError("failed to write file since %s", tstrerror(TSDB_CODE_INDEX_INVALID_FILE));
  }
  taosMemoryFree(buf);
  return;
}

uint8_t idxFilePackUint(IdxFstFile* writer, uint64_t n) {
  uint8_t nBytes = packSize(n);
  idxFilePackUintIn(writer, n, nBytes);
  return nBytes;
}
