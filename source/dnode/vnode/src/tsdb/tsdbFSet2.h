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

#include "tsdbFile2.h"

#ifndef _TSDB_FILE_SET2_H
#define _TSDB_FILE_SET2_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STFileOp STFileOp;
typedef struct SSttLvl  SSttLvl;
typedef TARRAY2(STFileObj *) TFileObjArray;
typedef TARRAY2(SSttLvl *) TSttLvlArray;
typedef TARRAY2(STFileOp) TFileOpArray;

typedef enum {
  TSDB_FOP_NONE = 0,
  TSDB_FOP_CREATE,
  TSDB_FOP_REMOVE,
  TSDB_FOP_MODIFY,
} tsdb_fop_t;

#define TFILE_SET(fid_) \
  (STFileSet) { .fid = (fid_) }

// init/clear
int32_t tsdbTFileSetInit(int32_t fid, STFileSet **fset);
int32_t tsdbTFileSetInitCopy(STsdb *pTsdb, const STFileSet *fset1, STFileSet **fset);
int32_t tsdbTFileSetInitRef(STsdb *pTsdb, const STFileSet *fset1, STFileSet **fset);
int32_t tsdbTFileSetClear(STFileSet **fset);
int32_t tsdbTFileSetRemove(STFileSet *fset);

int32_t tsdbTFileSetFilteredInitDup(STsdb *pTsdb, const STFileSet *fset1, int64_t ever, STFileSet **fset,
                                    TFileOpArray *fopArr);

int32_t tsdbTFileSetRangeInitRef(STsdb *pTsdb, const STFileSet *fset1, int64_t sver, int64_t ever,
                                 STFileSetRange **fsr);

// to/from json
int32_t tsdbTFileSetToJson(const STFileSet *fset, cJSON *json);
int32_t tsdbJsonToTFileSet(STsdb *pTsdb, const cJSON *json, STFileSet **fset);
// cmpr
int32_t tsdbTFileSetCmprFn(const STFileSet **fset1, const STFileSet **fset2);
// edit
int32_t tsdbSttLvlClear(SSttLvl **lvl);
int32_t tsdbTFileSetEdit(STsdb *pTsdb, STFileSet *fset, const STFileOp *op);
int32_t tsdbTFileSetApplyEdit(STsdb *pTsdb, const STFileSet *fset1, STFileSet *fset);
// max commit id
int64_t tsdbTFileSetMaxCid(const STFileSet *fset);
// get
SSttLvl *tsdbTFileSetGetSttLvl(STFileSet *fset, int32_t level);
// is empty
bool tsdbTFileSetIsEmpty(const STFileSet *fset);
// stt
int32_t tsdbSttLvlInit(int32_t level, SSttLvl **lvl);
int32_t tsdbSttLvlClear(SSttLvl **lvl);
// open channel
int32_t tsdbTFileSetOpenChannel(STFileSet *fset);

struct STFileOp {
  tsdb_fop_t optype;
  int32_t    fid;
  STFile     of;  // old file state
  STFile     nf;  // new file state
};

struct SSttLvl {
  int32_t       level;
  TFileObjArray fobjArr[1];
};

struct STFileSet {
  int32_t      fid;
  int64_t      maxVerValid;
  STFileObj   *farr[TSDB_FTYPE_MAX];  // file array
  TSttLvlArray lvlArr[1];             // level array

  // background task channel
  int64_t bgTaskChannel;
  bool    mergeScheduled;

  // block commit variables
  TdThreadCond canCommit;
  int32_t      numWaitCommit;
  bool         blockCommit;
};

struct STFileSetRange {
  int32_t    fid;
  int64_t    sver;
  int64_t    ever;
  STFileSet *fset;
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FILE_SET2_H*/
