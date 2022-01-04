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
#include "index_fst_counting_writer.h"
#include "indexInt.h"
#include "index_fst_util.h"
#include "tutil.h"

static int writeCtxDoWrite(WriterCtx* ctx, uint8_t* buf, int len) {
  // if (ctx->offset + len > ctx->limit) { return -1; }

  if (ctx->type == TFile) {
    assert(len == tfWrite(ctx->file.fd, buf, len));
  } else {
    memcpy(ctx->mem.buf + ctx->offset, buf, len);
  }
  ctx->offset += len;
  return len;
}
static int writeCtxDoRead(WriterCtx* ctx, uint8_t* buf, int len) {
  int nRead = 0;
  if (ctx->type == TFile) {
    nRead = tfRead(ctx->file.fd, buf, len);
  } else {
    memcpy(buf, ctx->mem.buf + ctx->offset, len);
  }
  ctx->offset += nRead;

  return nRead;
}
static int writeCtxDoReadFrom(WriterCtx* ctx, uint8_t* buf, int len, int32_t offset) {
  int nRead = 0;
  if (ctx->type == TFile) {
    // tfLseek(ctx->file.fd, offset, 0);
    nRead = tfPread(ctx->file.fd, buf, len, offset);
  } else {
    // refactor later
    assert(0);
  }
  return nRead;
}
static int writeCtxDoFlush(WriterCtx* ctx) {
  if (ctx->type == TFile) {
    // taosFsyncFile(ctx->file.fd);
    tfFsync(ctx->file.fd);
    // tfFlush(ctx->file.fd);
  } else {
    // do nothing
  }
  return 1;
}

WriterCtx* writerCtxCreate(WriterType type, const char* path, bool readOnly, int32_t capacity) {
  WriterCtx* ctx = calloc(1, sizeof(WriterCtx));
  if (ctx == NULL) { return NULL; }

  ctx->type = type;
  if (ctx->type == TFile) {
    // ugly code, refactor later
    ctx->file.readOnly = readOnly;
    if (readOnly == false) {
      // ctx->file.fd = open(path, O_WRONLY | O_CREAT | O_APPEND, S_IRWXU | S_IRWXG | S_IRWXO);
      ctx->file.fd = tfOpenCreateWriteAppend(path);

      struct stat fstat;
      stat(path, &fstat);
      ctx->file.size = fstat.st_size;
    } else {
      // ctx->file.fd = open(path, O_RDONLY, S_IRWXU | S_IRWXG | S_IRWXO);
      ctx->file.fd = tfOpenRead(path);

      struct stat fstat;
      stat(path, &fstat);
      ctx->file.size = fstat.st_size;
    }
    memcpy(ctx->file.buf, path, strlen(path));
    if (ctx->file.fd < 0) {
      indexError("failed to open file, error %d", errno);
      goto END;
    }
  } else if (ctx->type == TMemory) {
    ctx->mem.buf = calloc(1, sizeof(char) * capacity);
    ctx->mem.capa = capacity;
  }
  ctx->write = writeCtxDoWrite;
  ctx->read = writeCtxDoRead;
  ctx->flush = writeCtxDoFlush;
  ctx->readFrom = writeCtxDoReadFrom;

  ctx->offset = 0;
  ctx->limit = capacity;

  return ctx;
END:
  if (ctx->type == TMemory) { free(ctx->mem.buf); }
  free(ctx);
  return NULL;
}
void writerCtxDestroy(WriterCtx* ctx, bool remove) {
  if (ctx->type == TMemory) {
    free(ctx->mem.buf);
  } else {
    // ctx->flush(ctx);
    tfClose(ctx->file.fd);
    if (remove) { unlink(ctx->file.buf); }
  }
  free(ctx);
}

FstCountingWriter* fstCountingWriterCreate(void* wrt) {
  FstCountingWriter* cw = calloc(1, sizeof(FstCountingWriter));
  if (cw == NULL) { return NULL; }

  cw->wrt = wrt;
  //(void *)(writerCtxCreate(TFile, readOnly));
  return cw;
}
void fstCountingWriterDestroy(FstCountingWriter* cw) {
  // free wrt object: close fd or free mem
  fstCountingWriterFlush(cw);
  // writerCtxDestroy((WriterCtx *)(cw->wrt));
  free(cw);
}

int fstCountingWriterWrite(FstCountingWriter* write, uint8_t* buf, uint32_t len) {
  if (write == NULL) { return 0; }
  // update checksum
  // write data to file/socket or mem
  WriterCtx* ctx = write->wrt;

  int nWrite = ctx->write(ctx, buf, len);
  assert(nWrite == len);
  write->count += len;
  return len;
}
int fstCountingWriterRead(FstCountingWriter* write, uint8_t* buf, uint32_t len) {
  if (write == NULL) { return 0; }
  WriterCtx* ctx = write->wrt;
  int        nRead = ctx->read(ctx, buf, len);
  // assert(nRead == len);
  return nRead;
}

uint32_t fstCountingWriterMaskedCheckSum(FstCountingWriter* write) { return 0; }

int fstCountingWriterFlush(FstCountingWriter* write) {
  WriterCtx* ctx = write->wrt;
  ctx->flush(ctx);
  // write->wtr->flush
  return 1;
}

void fstCountingWriterPackUintIn(FstCountingWriter* writer, uint64_t n, uint8_t nBytes) {
  assert(1 <= nBytes && nBytes <= 8);
  uint8_t* buf = calloc(8, sizeof(uint8_t));
  for (uint8_t i = 0; i < nBytes; i++) {
    buf[i] = (uint8_t)n;
    n = n >> 8;
  }
  fstCountingWriterWrite(writer, buf, nBytes);
  free(buf);
  return;
}

uint8_t fstCountingWriterPackUint(FstCountingWriter* writer, uint64_t n) {
  uint8_t nBytes = packSize(n);
  fstCountingWriterPackUintIn(writer, n, nBytes);
  return nBytes;
}
