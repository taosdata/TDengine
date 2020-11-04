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

#ifndef TDENGINE_STSBUF_H
#define TDENGINE_STSBUF_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "taosdef.h"
#include "tvariant.h"

#define MEM_BUF_SIZE (1 << 20)
#define TS_COMP_FILE_MAGIC 0x87F5EC4C
#define TS_COMP_FILE_VNODE_MAX 512

typedef struct STSList {
  char*   rawBuf;
  int32_t allocSize;
  int32_t threshold;
  int32_t len;
} STSList;

typedef struct STSElem {
  TSKEY     ts;
  tVariant* tag;
  int32_t   vnode;
} STSElem;

typedef struct STSCursor {
  int32_t  vgroupIndex;
  int32_t  blockIndex;
  int32_t  tsIndex;
  uint32_t order;
} STSCursor;

typedef struct STSBlock {
  tVariant tag;        // tag value
  int32_t  numOfElem;  // number of elements
  int32_t  compLen;    // size after compressed
  int32_t  padding;    // 0xFFFFFFFF by default, after the payload
  char*    payload;    // actual data that is compressed
} STSBlock;

/*
 * The size of buffer file should not be greater than 2G,
 * and the offset of int32_t type is enough
 */
typedef struct STSVnodeBlockInfo {
  int32_t vnode;       // vnode id
  int32_t offset;      // offset set value in file
  int32_t numOfBlocks; // number of total blocks
  int32_t compLen;     // compressed size
} STSVnodeBlockInfo;

typedef struct STSVnodeBlockInfoEx {
  STSVnodeBlockInfo info;
  int32_t           len;  // length before compress
} STSVnodeBlockInfoEx;

typedef struct STSBuf {
  FILE*    f;
  char     path[PATH_MAX];
  uint32_t fileSize;

  // todo use array
  STSVnodeBlockInfoEx* pData;
  uint32_t             numOfAlloc;
  uint32_t             numOfVnodes;

  char*     assistBuf;
  int32_t   bufSize;
  STSBlock  block;
  STSList   tsData;  // uncompressed raw ts data
  uint64_t  numOfTotal;
  bool      autoDelete;
  int32_t   tsOrder;  // order of timestamp in ts comp buffer
  STSCursor cur;
} STSBuf;

typedef struct STSBufFileHeader {
  uint32_t magic;       // file magic number
  uint32_t numOfVnode;  // number of vnode stored in current file
  int32_t  tsOrder;     // timestamp order in current file
} STSBufFileHeader;

STSBuf* tsBufCreate(bool autoDelete, int32_t order);
STSBuf* tsBufCreateFromFile(const char* path, bool autoDelete);
STSBuf* tsBufCreateFromCompBlocks(const char* pData, int32_t numOfBlocks, int32_t len, int32_t tsOrder, int32_t vnodeId);

void* tsBufDestroy(STSBuf* pTSBuf);

void    tsBufAppend(STSBuf* pTSBuf, int32_t vnodeId, tVariant* tag, const char* pData, int32_t len);
int32_t tsBufMerge(STSBuf* pDestBuf, const STSBuf* pSrcBuf);

STSBuf* tsBufClone(STSBuf* pTSBuf);

STSVnodeBlockInfo* tsBufGetVnodeBlockInfo(STSBuf* pTSBuf, int32_t vnodeId);

void tsBufFlush(STSBuf* pTSBuf);

void    tsBufResetPos(STSBuf* pTSBuf);
STSElem tsBufGetElem(STSBuf* pTSBuf);

bool    tsBufNextPos(STSBuf* pTSBuf);

STSElem tsBufGetElemStartPos(STSBuf* pTSBuf, int32_t vnodeId, tVariant* tag);

STSCursor tsBufGetCursor(STSBuf* pTSBuf);
void      tsBufSetTraverseOrder(STSBuf* pTSBuf, int32_t order);

void tsBufSetCursor(STSBuf* pTSBuf, STSCursor* pCur);

/**
 * display all data in comp block file, for debug purpose only
 * @param pTSBuf
 */
void tsBufDisplay(STSBuf* pTSBuf);

int32_t tsBufGetNumOfVnodes(STSBuf* pTSBuf);

void tsBufGetVnodeIdList(STSBuf* pTSBuf, int32_t* num, int32_t** vnodeId);

int32_t dumpFileBlockByVnodeId(STSBuf* pTSBuf, int32_t vnodeId, void* buf, int32_t* len, int32_t* numOfBlocks);

STSElem tsBufFindElemStartPosByTag(STSBuf* pTSBuf, tVariant* pTag);

bool tsBufIsValidElem(STSElem* pElem);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STSBUF_H
