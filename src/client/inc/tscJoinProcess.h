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

#ifndef TDENGINE_TSCJOINPROCESS_H
#define TDENGINE_TSCJOINPROCESS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tscUtil.h"
#include "tsclient.h"

void tscFetchDatablockFromSubquery(SSqlObj* pSql);
void tscGetQualifiedTSList(SSqlObj* pSql, SJoinSubquerySupporter* p1, SJoinSubquerySupporter* p2, int32_t* num);

void tscSetupOutputColumnIndex(SSqlObj* pSql);

int32_t tscLaunchSecondPhaseDirectly(SSqlObj* pSql, SSubqueryState* pState);
int32_t tscLaunchSecondPhaseSubqueries(SSqlObj* pSql);
void tscJoinQueryCallback(void* param, TAOS_RES* tres, int code);

SJoinSubquerySupporter* tscCreateJoinSupporter(SSqlObj* pSql, SSubqueryState* pState, int32_t index);
void tscDestroyJoinSupporter(SJoinSubquerySupporter* pSupporter);

#define MEM_BUF_SIZE                (1u<<20)
#define TS_COMP_BLOCK_PADDING       0xFFFFFFFF
#define TS_COMP_FILE_MAGIC          0x87F5EC4C
#define TS_COMP_FILE_VNODE_MAX      512

typedef struct STSList {
  char*   rawBuf;
  int32_t allocSize;
  int32_t threshold;
  int32_t len;
} STSList;

typedef struct STSRawBlock {
  int32_t vnode;
  tVariant tag;
  TSKEY*  ts;
  int32_t len;
} STSRawBlock;

typedef struct STSElem {
  TSKEY   ts;
  tVariant tag;
  int32_t vnode;
} STSElem;

typedef struct STSCursor {
  int32_t vnodeIndex;
  int32_t blockIndex;
  int32_t tsIndex;
  int32_t order;
} STSCursor;

typedef struct STSBlock {
  tVariant tag;        // tag value
  int32_t numOfElem;  // number of elements
  int32_t compLen;    // size after compressed
  int32_t padding;    // 0xFFFFFFFF by default, after the payload
  char*   payload;    // actual data that is compressed
} STSBlock;

typedef struct STSVnodeBlockInfo {
  int32_t vnode;

  /*
   * The size of buffer file is not expected to be greater than 2G,
   * and the offset of int32_t type is enough
   */
  int32_t offset;
  int32_t numOfBlocks;
  int32_t compLen;
} STSVnodeBlockInfo;

typedef struct STSVnodeBlockInfoEx {
  STSVnodeBlockInfo info;
  int32_t           len;  // length before compress
} STSVnodeBlockInfoEx;

typedef struct STSBuf {
  FILE*    f;
  char     path[PATH_MAX];
  uint32_t fileSize;

  STSVnodeBlockInfoEx* pData;
  int32_t              numOfAlloc;
  int32_t              numOfVnodes;

  char*    assistBuf;
  int32_t  bufSize;
  STSBlock block;
  STSList  tsData;  // uncompressed raw ts data

  uint64_t numOfTotal;
  bool     autoDelete;
  int32_t  tsOrder; // order of timestamp in ts comp buffer

  STSCursor cur;
} STSBuf;

typedef struct STSBufFileHeader {
  uint32_t magic;         // file magic number
  uint32_t numOfVnode;    // number of vnode stored in current file
  uint32_t tsOrder;       // timestamp order in current file
} STSBufFileHeader;

STSBuf* tsBufCreate(bool autoDelete);
STSBuf* tsBufCreateFromFile(const char* path, bool autoDelete);
STSBuf* tsBufCreateFromCompBlocks(const char* pData, int32_t numOfBlocks, int32_t len, int32_t tsOrder);

void* tsBufDestory(STSBuf* pTSBuf);

void tsBufAppend(STSBuf* pTSBuf, int32_t vnodeId, tVariant* tag, const char* pData, int32_t len);
int32_t tsBufMerge(STSBuf* pDestBuf, const STSBuf* pSrcBuf, int32_t vnodeIdx);

STSVnodeBlockInfo* tsBufGetVnodeBlockInfo(STSBuf* pTSBuf, int32_t vnodeId);

void tsBufFlush(STSBuf* pTSBuf);

void tsBufResetPos(STSBuf* pTSBuf);
STSElem tsBufGetElem(STSBuf* pTSBuf);
bool tsBufNextPos(STSBuf* pTSBuf);

STSElem tsBufGetElemStartPos(STSBuf* pTSBuf, int32_t vnodeId, tVariant* tag);

STSCursor tsBufGetCursor(STSBuf* pTSBuf);
void tsBufSetTraverseOrder(STSBuf* pTSBuf, int32_t order);

void tsBufSetCursor(STSBuf* pTSBuf, STSCursor* pCur);
STSBuf* tsBufClone(STSBuf* pTSBuf);

/**
 * display all data in comp block file, for debug purpose only
 * @param pTSBuf
 */
void tsBufDisplay(STSBuf* pTSBuf);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCJOINPROCESS_H
