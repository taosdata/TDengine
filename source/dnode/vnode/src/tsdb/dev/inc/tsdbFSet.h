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
typedef struct SSttLvl   SSttLvl;
typedef TARRAY2(STFileObj *) TFileObjArray;
typedef TARRAY2(SSttLvl *) TSttLvlArray;
typedef TARRAY2(STFileOp) TFileOpArray;

typedef enum {
  TSDB_FOP_NONE = 0,
  TSDB_FOP_EXTEND,
  TSDB_FOP_CREATE,
  TSDB_FOP_DELETE,
  TSDB_FOP_TRUNCATE,
} tsdb_fop_t;

// init/clear
int32_t tsdbTFileSetInit(int32_t fid, STFileSet **fset);
int32_t tsdbTFileSetInitEx(STsdb *pTsdb, const STFileSet *fset1, STFileSet **fset);
int32_t tsdbTFileSetClear(STFileSet **fset);
// to/from json
int32_t tsdbTFileSetToJson(const STFileSet *fset, cJSON *json);
int32_t tsdbJsonToTFileSet(STsdb *pTsdb, const cJSON *json, STFileSet **fset);
// cmpr
int32_t tsdbTFileSetCmprFn(const STFileSet **fset1, const STFileSet **fset2);
// edit
int32_t tsdbTFileSetEdit(STFileSet *fset, const STFileOp *op);
int32_t tsdbTFileSetEditEx(const STFileSet *fset1, STFileSet *fset);
// max commit id
int64_t tsdbTFileSetMaxCid(const STFileSet *fset);

const SSttLvl *tsdbTFileSetGetLvl(const STFileSet *fset, int32_t level);

struct STFileOp {
  tsdb_fop_t op;
  int32_t    fid;
  STFile     oState;  // old file state
  STFile     nState;  // new file state
};

struct SSttLvl {
  int32_t       level;
  TFileObjArray farr;
};

struct STFileSet {
  int32_t      fid;
  STFileObj   *farr[TSDB_FTYPE_MAX];  // file array
  TSttLvlArray lvlArr;                // level array
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FILE_SET_H*/