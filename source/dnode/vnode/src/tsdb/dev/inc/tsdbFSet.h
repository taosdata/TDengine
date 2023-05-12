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

#include "tsdbFile.h"

#ifndef _TSDB_FILE_SET_H
#define _TSDB_FILE_SET_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STFileSet STFileSet;
typedef struct STFileOp  STFileOp;

typedef enum {
  TSDB_FOP_NONE = 0,
  TSDB_FOP_EXTEND,
  TSDB_FOP_CREATE,
  TSDB_FOP_DELETE,
  TSDB_FOP_TRUNCATE,
} tsdb_fop_t;

int32_t tsdbFileSetToJson(const STFileSet *fset, cJSON *json);
int32_t tsdbFileSetFromJson(const cJSON *json, STFileSet *fset);

int32_t tsdbFileSetInit(STFileSet *pSet);
int32_t tsdbFileSetClear(STFileSet *pSet);

int32_t tsdbFSetEdit(STFileSet *pSet, const STFileOp *pOp);

int32_t tsdbFSetCmprFn(const STFileSet *pSet1, const STFileSet *pSet2);

struct STFileOp {
  tsdb_fop_t op;
  int32_t    fid;
  STFile     oState;  // old file state
  STFile     nState;  // new file state
};

typedef struct SSttLvl {
  LISTD(struct SSttLvl) listNode;
  int32_t lvl;   // level
  int32_t nstt;  // number of .stt files on this level
  STFile *fstt;  // .stt files
} SSttLvl;

struct STFileSet {
  int32_t fid;
  STFile *farr[TSDB_FTYPE_MAX];  // file array
  SSttLvl lvl0;                  // level 0 of .stt
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FILE_SET_H*/