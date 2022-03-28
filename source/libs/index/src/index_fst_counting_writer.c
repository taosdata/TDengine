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

#include "os.h"
#include "index_fst_counting_writer.h"
#include "indexInt.h"
#include "index_fst_util.h"
#include "tutil.h"

static int writeCtxDoWrite(WriterCtx* ctx, uint8_t* buf, int len) {
  if (ctx->type == TFile) {
    assert(len == taosWriteFile(ctx->file.pFile, buf, len));
  } else {
    memcpy(ctx->mem.buf + ctx->offset, buf, len);
  }
  ctx->offset += len;
  return len;
}
static int writeCtxDoRead(WriterCtx* ctx, uint8_t* buf, int len) {
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
static int writeCtxDoReadFrom(WriterCtx* ctx, uint8_t* buf, int len, int32_t offset) {
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
static int writeCtxGetSize(WriterCtx* ctx) {
  if (ctx->type == TFile) {
    int64_t file_size = 0;
    taosStatFile(ctx->file.buf, &file_size, NULL);
    return (int)file_size;
  }
  return 0;
}
static int writeCtxDoFlush(WriterCtx* ctx) {
  if (ctx->type == TFile) {
    // taosFsyncFile(ctx->file.pFile);
    taosFsyncFile(ctx->file.pFile);
    // tfFlush(ctx->file.pFile);
  } else {
    // do nothing
  }
  return 1;
}

WriterCtx* writerCtxCreate(WriterType type, const char* path, bool readOnly, int32_t capacity) {
  WriterCtx* ctx = taosMemoryCalloc(1, sizeof(WriterCtx));
  if (ctx == NULL) { return NULL; }

  ctx->type = type;
  if (ctx->type == TFile) {
    // ugly code, refactor later
    ctx->file.readOnly = readOnly;
    if (readOnly == false) {
      // ctx->file.pFile = open(path, O_WRONLY | O_CREAT | O_APPEND, S_IRWXU | S_IRWXG | S_IRWXO);
      ctx->file.pFile = taosOpenFile(path, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_APPEND);
      taosFtruncateFile(ctx->file.pFile, 0);
      int64_t file_size;
      taosStatFile(path, &file_size, NULL);
      ctx->file.size = (int)file_size;
    } else {
      // ctx->file.pFile = open(path, O_RDONLY, S_IRWXU | S_IRWXG | S_IRWXO);
      ctx->file.pFile = taosOpenFile(path, TD_FILE_READ);

      int64_t file_size = 0;
      taosFStatFile(ctx->file.pFile, &file_size, NULL);
      ctx->file.size = (int)file_size;
#ifdef USE_MMAP
      ctx->file.ptr = (char*)tfMmapReadOnly(ctx->file.pFile, ctx->file.size);
#endif
    }
    memcpy(ctx->file.buf, path, strlen(path));
    if (ctx->file.pFile == NULL) {
      indexError("failed to open file, error %d", errno);
      goto END;
    }
  } else if (ctx->type == TMemory) {
    ctx->mem.buf = taosMemoryCalloc(1, sizeof(char) * capacity);
    ctx->mem.capa = capacity;
  }
  ctx->write = writeCtxDoWrite;
  ctx->read = writeCtxDoRead;
  ctx->flush = writeCtxDoFlush;
  ctx->readFrom = writeCtxDoReadFrom;
  ctx->size = writeCtxGetSize;

  ctx->offset = 0;
  ctx->limit = capacity;

  return ctx;
END:
  if (ctx->type == TMemory) { taosMemoryFree(ctx->mem.buf); }
  taosMemoryFree(ctx);
  return NULL;
}
void writerCtxDestroy(WriterCtx* ctx, bool remove) {
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
      // struct stat fstat;
      // stat(ctx->file.buf, &fstat);
      // indexError("write file size: %d", (int)(fstat.st_size));
    }
    if (remove) { unlink(ctx->file.buf); }
  }
  taosMemoryFree(ctx);
}

FstCountingWriter* fstCountingWriterCreate(void* wrt) {
  FstCountingWriter* cw = taosMemoryCalloc(1, sizeof(FstCountingWriter));
  if (cw == NULL) { return NULL; }

  cw->wrt = wrt;
  //(void *)(writerCtxCreate(TFile, readOnly));
  return cw;
}
void fstCountingWriterDestroy(FstCountingWriter* cw) {
  // free wrt object: close fd or free mem
  fstCountingWriterFlush(cw);
  // writerCtxDestroy((WriterCtx *)(cw->wrt));
  taosMemoryFree(cw);
}

int fstCountingWriterWrite(FstCountingWriter* write, uint8_t* buf, uint32_t len) {
  if (write == NULL) { return 0; }
  // update checksum
  // write data to file/socket or mem
  WriterCtx* ctx = write->wrt;

  int nWrite = ctx->write(ctx, buf, len);
  assert(nWrite == len);
  write->count += len;

  write->summer = taosCalcChecksum(write->summer, buf, len);
  return len;
}
int fstCountingWriterRead(FstCountingWriter* write, uint8_t* buf, uint32_t len) {
  if (write == NULL) { return 0; }
  WriterCtx* ctx = write->wrt;
  int        nRead = ctx->read(ctx, buf, len);
  // assert(nRead == len);
  return nRead;
}

uint32_t fstCountingWriterMaskedCheckSum(FstCountingWriter* write) {
  // opt
  return write->summer;
}

int fstCountingWriterFlush(FstCountingWriter* write) {
  WriterCtx* ctx = write->wrt;
  ctx->flush(ctx);
  // write->wtr->flush
  return 1;
}

void fstCountingWriterPackUintIn(FstCountingWriter* writer, uint64_t n, uint8_t nBytes) {
  assert(1 <= nBytes && nBytes <= 8);
  uint8_t* buf = taosMemoryCalloc(8, sizeof(uint8_t));
  for (uint8_t i = 0; i < nBytes; i++) {
    buf[i] = (uint8_t)n;
    n = n >> 8;
  }
  fstCountingWriterWrite(writer, buf, nBytes);
  taosMemoryFree(buf);
  return;
}

uint8_t fstCountingWriterPackUint(FstCountingWriter* writer, uint64_t n) {
  uint8_t nBytes = packSize(n);
  fstCountingWriterPackUintIn(writer, n, nBytes);
  return nBytes;
}
