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
  STsdb       *tsdb;
  SMergeCtx    ctx;
  TFileOpArray fopArr;
} SMerger;

static int32_t tsdbMergerOpen(SMerger *merger) {
  merger->ctx.launched = true;
  TARRAY2_INIT(&merger->fopArr);
  return 0;
}

static int32_t tsdbMergerClose(SMerger *merger) {
  // TODO
  ASSERT(0);
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
    TARRAY2_FOREACH(&fset->lvlArr, lvl) {
      TARRAY2_FOREACH(&lvl->farr, fobj) {
        if (fobj->f.stt.nseg >= merger->tsdb->pVnode->config.sttTrigger) {
          STFileOp op = {
              .fid = fset->fid,
              .optype = TSDB_FOP_REMOVE,
              .of = fobj->f,
          };

          code = TARRAY2_APPEND(&merger->fopArr, op);
          TSDB_CHECK_CODE(code, lino, _exit);
        } else {
          if (lvl->level == 0) {
            continue;
          } else {
            // TODO
          }
        }
      }
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
  } else {
    tsdbDebug("vgId:%d %s done, do merge: %d", vid, __func__, merger.ctx.launched);
  }
  return 0;
}
