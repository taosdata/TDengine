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

#include "indexFstFile.h"
#include "indexFstUtil.h"
#include "indexInt.h"
#include "os.h"
#include "tutil.h"

static int idxFileCtxDoWrite(IFileCtx* ctx, uint8_t* buf, int len) {
  if (ctx->type == TFile) {
    assert(len == taosWriteFile(ctx->file.pFile, buf, len));
  } else {
    memcpy(ctx->mem.buf + ctx->offset, buf, len);
  }
  ctx->offset += len;
  return len;
}
static int idxFileCtxDoRead(IFileCtx* ctx, uint8_t* buf, int len) {
  int nRead = 0;
  if (ctx->type == TFile) {
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
  int nRead = 0;
  if (ctx->type == TFile) {
    // tfLseek(ctx->file.pFile, offset, 0);
#ifdef USE_MMAP
    int32_t last = ctx->file.size - offset;
    nRead = last >= len ? len : last;
    memcpy(buf, ctx->file.ptr + offset, nRead);
#else
    nRead = taosPReadFile(ctx->file.pFile, buf, len, offset);
#endif
  } else {
    // refactor later
    assert(0);
  }
  return nRead;
}
static int idxFileCtxGetSize(IFileCtx* ctx) {
  if (ctx->type == TFile) {
    int64_t file_size = 0;
    taosStatFile(ctx->file.buf, &file_size, NULL);
    return (int)file_size;
  }
  return 0;
}
static int idxFileCtxDoFlush(IFileCtx* ctx) {
  if (ctx->type == TFile) {
    // taosFsyncFile(ctx->file.pFile);
    taosFsyncFile(ctx->file.pFile);
    // tfFlush(ctx->file.pFile);
  } else {
    // do nothing
  }
  return 1;
}

IFileCtx* idxFileCtxCreate(WriterType type, const char* path, bool readOnly, int32_t capacity) {
  IFileCtx* ctx = taosMemoryCalloc(1, sizeof(IFileCtx));
  if (ctx == NULL) {
    return NULL;
  }

  ctx->type = type;
  if (ctx->type == TFile) {
    // ugly code, refactor later
    ctx->file.readOnly = readOnly;
    memcpy(ctx->file.buf, path, strlen(path));
    if (readOnly == false) {
      ctx->file.pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
      taosFtruncateFile(ctx->file.pFile, 0);
      taosStatFile(path, &ctx->file.size, NULL);
      // ctx->file.size = (int)size;

    } else {
      ctx->file.pFile = taosOpenFile(path, TD_FILE_READ);

      int64_t size = 0;
      taosFStatFile(ctx->file.pFile, &ctx->file.size, NULL);
      ctx->file.size = (int)size;
#ifdef USE_MMAP
      ctx->file.ptr = (char*)tfMmapReadOnly(ctx->file.pFile, ctx->file.size);
#endif
    }
    if (ctx->file.pFile == NULL) {
      indexError("failed to open file, error %d", errno);
      goto END;
    }
  } else if (ctx->type == TMemory) {
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
  if (ctx->type == TMemory) {
    taosMemoryFree(ctx->mem.buf);
  }
  taosMemoryFree(ctx);
  return NULL;
}
void idxFileCtxDestroy(IFileCtx* ctx, bool remove) {
  if (ctx->type == TMemory) {
    taosMemoryFree(ctx->mem.buf);
  } else {
    ctx->flush(ctx);
    taosCloseFile(&ctx->file.pFile);
    if (ctx->file.readOnly) {
#ifdef USE_MMAP
      munmap(ctx->file.ptr, ctx->file.size);
#endif
    }
    if (ctx->file.readOnly == false) {
      int64_t file_size = 0;
      taosStatFile(ctx->file.buf, &file_size, NULL);
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
  // free wrt object: close fd or free mem
  idxFileFlush(cw);
  // idxFileCtxDestroy((IFileCtx *)(cw->wrt));
  taosMemoryFree(cw);
}

int idxFileWrite(IdxFstFile* write, uint8_t* buf, uint32_t len) {
  if (write == NULL) {
    return 0;
  }
  // update checksum
  // write data to file/socket or mem
  IFileCtx* ctx = write->wrt;

  int nWrite = ctx->write(ctx, buf, len);
  assert(nWrite == len);
  write->count += len;

  write->summer = taosCalcChecksum(write->summer, buf, len);
  return len;
}
int idxFileRead(IdxFstFile* write, uint8_t* buf, uint32_t len) {
  if (write == NULL) {
    return 0;
  }
  IFileCtx* ctx = write->wrt;
  int       nRead = ctx->read(ctx, buf, len);
  // assert(nRead == len);
  return nRead;
}

uint32_t idxFileMaskedCheckSum(IdxFstFile* write) {
  // opt
  return write->summer;
}

int idxFileFlush(IdxFstFile* write) {
  IFileCtx* ctx = write->wrt;
  ctx->flush(ctx);
  return 1;
}

void idxFilePackUintIn(IdxFstFile* writer, uint64_t n, uint8_t nBytes) {
  assert(1 <= nBytes && nBytes <= 8);
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
