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

#ifndef _TSDB_FILE_SET_H
#define _TSDB_FILE_SET_H

#include "tsdbFile.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SFileSet SFileSet;
typedef struct SFileOp  SFileOp;
typedef struct SSttLvl  SSttLvl;

typedef enum {
  TSDB_FOP_EXTEND = -2,
  TSDB_FOP_CREATE,
  TSDB_FOP_NONE,
  TSDB_FOP_DELETE,
  TSDB_FOP_TRUNCATE,
} tsdb_fop_t;

int32_t tsdbFileSetCreate(int32_t fid, SFileSet **ppSet);
int32_t tsdbFileSetEdit(SFileSet *pSet, SFileOp *pOp);
int32_t tsdbFileSetToJson(SJson *pJson, const SFileSet *pSet);
int32_t tsdbEditFileSet(SFileSet *pFileSet, const SFileOp *pOp);

struct SFileOp {
  tsdb_fop_t op;
  int32_t    fid;
  STFile     oState;  // old file state
  STFile     nState;  // new file state
};

struct SSttLvl {
  int32_t        level;
  int32_t        nStt;
  struct STFile *fSttList;
  LISTD(SSttLvl) listNode;
};

struct SFileSet {
  int32_t        fid;
  int64_t        nextid;
  struct STFile *farr[TSDB_FTYPE_MAX];  // file array
  SSttLvl        lvl0;                  // level 0 of .stt
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FILE_SET_H*/