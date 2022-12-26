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

#ifndef __INDEX_FST_FILE_H__
#define __INDEX_FST_FILE_H__

#include "indexInt.h"

#ifdef __cplusplus
extern "C" {
#endif

//#define USE_MMAP 1

#define DefaultMem 1024 * 1024

static char tmpFile[] = "./index";
typedef enum WriterType { TMEMORY, TFILE } WriterType;

typedef struct IFileCtx {
  int (*write)(struct IFileCtx* ctx, uint8_t* buf, int len);
  int (*read)(struct IFileCtx* ctx, uint8_t* buf, int len);
  int (*flush)(struct IFileCtx* ctx);
  int (*readFrom)(struct IFileCtx* ctx, uint8_t* buf, int len, int32_t offset);
  int (*size)(struct IFileCtx* ctx);

  SLRUCache* lru;
  WriterType type;
  union {
    struct {
      TdFilePtr pFile;
      bool      readOnly;
      char      buf[256];
      int64_t   size;

      char*   wBuf;
      int32_t wBufOffset;
      int32_t wBufCap;

#ifdef USE_MMAP
      char* ptr;
#endif
    } file;
    struct {
      int32_t cap;
      char*   buf;
    } mem;
  };
  int32_t offset;
  int32_t limit;
} IFileCtx;

static int idxFileCtxDoWrite(IFileCtx* ctx, uint8_t* buf, int len);
static int idxFileCtxDoRead(IFileCtx* ctx, uint8_t* buf, int len);
static int idxFileCtxDoReadFrom(IFileCtx* ctx, uint8_t* buf, int len, int32_t offset);
static int idxFileCtxDoFlush(IFileCtx* ctx);

IFileCtx* idxFileCtxCreate(WriterType type, const char* path, bool readOnly, int32_t capacity);
void      idxFileCtxDestroy(IFileCtx* w, bool remove);

typedef uint32_t CheckSummer;

typedef struct IdxFstFile {
  void*       wrt;  // wrap any writer that counts and checksum bytes written
  uint64_t    count;
  CheckSummer summer;
} IdxFstFile;

int idxFileWrite(IdxFstFile* write, uint8_t* buf, uint32_t len);

int idxFileRead(IdxFstFile* write, uint8_t* buf, uint32_t len);

int idxFileFlush(IdxFstFile* write);

uint32_t idxFileMaskedCheckSum(IdxFstFile* write);

IdxFstFile* idxFileCreate(void* wtr);
void        idxFileDestroy(IdxFstFile* w);

void    idxFilePackUintIn(IdxFstFile* writer, uint64_t n, uint8_t nBytes);
uint8_t idxFilePackUint(IdxFstFile* writer, uint64_t n);

#define FST_WRITER_COUNT(writer)        (writer->count)
#define FST_WRITER_INTER_WRITER(writer) (writer->wtr)
#define FST_WRITE_CHECK_SUMMER(writer)  (writer->summer)

#ifdef __cplusplus
}
#endif

#endif
