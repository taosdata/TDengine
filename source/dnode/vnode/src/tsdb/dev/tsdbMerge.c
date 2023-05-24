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

#include "inc/tsdbMerge.h"

typedef struct {
  bool launched;
} SMergeCtx;

typedef struct {
  STsdb           *tsdb;
  SMergeCtx        ctx;
  SSttFileWriter  *sttWriter;
  SDataFileWriter *dataWriter;
  TFileOpArray     fopArr;
} SMerger;

static int32_t tsdbMergerOpen(SMerger *merger) {
  merger->ctx.launched = true;
  TARRAY2_INIT(&merger->fopArr);
  return 0;
}

static int32_t tsdbMergerClose(SMerger *merger) {
  // TODO
  int32_t       code = 0;
  int32_t       lino = 0;
  SVnode       *pVnode = merger->tsdb->pVnode;
  int32_t       vid = TD_VID(pVnode);
  STFileSystem *fs = merger->tsdb->pFS;

  // edit file system
  code = tsdbFSEditBegin(fs, &merger->fopArr, TSDB_FEDIT_MERGE);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFSEditCommit(fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  // clear the merge
  TARRAY2_FREE(&merger->fopArr);

_exit:
  if (code) {
  } else {
  }
  return 0;
}

static int32_t tsdbMergeFileSet(SMerger *merger, STFileSet *fset) {
  int32_t code = 0;
  int32_t lino = 0;

  if (merger->ctx.launched == false) {
    code = tsdbMergerOpen(merger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  {  // prepare the merger file set
    SSttLvl   *lvl;
    STFileObj *fobj;
    bool       mergerToData = true;
    int32_t    level = -1;

    TARRAY2_FOREACH(&fset->lvlArr, lvl) {
      if (lvl->level - level > 1) {
        mergerToData = false;
        break;
      }

      if (lvl->level == 0) {
      } else {
        ASSERT(TARRAY2_SIZE(&lvl->farr) == 1);

        fobj = TARRAY2_FIRST(&lvl->farr);
      }
    }

    // merge to level
    level = level + 1;
    lvl = tsdbTFileSetGetLvl(fset, level);
    if (lvl == NULL) {
      // open new stt file to merge to
    } else {
      // open existing stt file to merge to
    }

    if (mergerToData) {
      // code = tsdbDataFWriterOpen(SDataFWriter * *ppWriter, STsdb * pTsdb, SDFileSet * pSet);
      // TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  {
      // do merge the file set
  }

  {  // end merge the file set
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(merger->tsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(merger->tsdb->pVnode), __func__, fset->fid);
  }
  return 0;
}

int32_t tsdbMerge(STsdb *tsdb) {
  int32_t code = 0;
  int32_t lino;

  SVnode       *vnode = tsdb->pVnode;
  int32_t       vid = TD_VID(vnode);
  STFileSystem *fs = tsdb->pFS;
  STFileSet    *fset;
  STFileObj    *fobj;
  int32_t       sttTrigger = vnode->config.sttTrigger;

  SMerger merger = {
      .tsdb = tsdb,
      .ctx =
          {
              .launched = false,
          },
  };

  // loop to merge each file set
  TARRAY2_FOREACH(&fs->cstate, fset) {
    SSttLvl *lvl0 = tsdbTFileSetGetLvl(fset, 0);
    if (lvl0 == NULL) {
      continue;
    }

    ASSERT(TARRAY2_SIZE(&lvl0->farr) > 0);

    fobj = TARRAY2_GET(&lvl0->farr, 0);

    if (fobj->f.stt.nseg >= sttTrigger) {
      code = tsdbMergeFileSet(&merger, fset);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // end the merge
  if (merger.ctx.launched) {
    code = tsdbMergerClose(&merger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  } else if (merger.ctx.launched) {
    tsdbDebug("vgId:%d %s done", vid, __func__);
  }
  return 0;
}
