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

#include "tsdbFSet2.h"
#include "vnd.h"

int32_t tsdbSttLvlInit(int32_t level, SSttLvl **lvl) {
  if (!(lvl[0] = taosMemoryMalloc(sizeof(SSttLvl)))) {
    return terrno;
  }
  lvl[0]->level = level;
  TARRAY2_INIT(lvl[0]->fobjArr);
  return 0;
}

static void tsdbSttLvlClearFObj(void *data) { TAOS_UNUSED(tsdbTFileObjUnref(*(STFileObj **)data)); }

void tsdbSttLvlClear(SSttLvl **lvl) {
  if (lvl[0] != NULL) {
    TARRAY2_DESTROY(lvl[0]->fobjArr, tsdbSttLvlClearFObj);
    taosMemoryFree(lvl[0]);
    lvl[0] = NULL;
  }
}

static int32_t tsdbSttLvlInitEx(STsdb *pTsdb, const SSttLvl *lvl1, SSttLvl **lvl) {
  int32_t code = tsdbSttLvlInit(lvl1->level, lvl);
  if (code) return code;

  const STFileObj *fobj1;
  TARRAY2_FOREACH(lvl1->fobjArr, fobj1) {
    STFileObj *fobj;
    code = tsdbTFileObjInit(pTsdb, fobj1->f, &fobj);
    if (code) {
      tsdbSttLvlClear(lvl);
      return code;
    }

    code = TARRAY2_APPEND(lvl[0]->fobjArr, fobj);
    if (code) {
      tsdbSttLvlClear(lvl);
      taosMemoryFree(fobj);
      return code;
    }
  }
  return 0;
}

static int32_t tsdbSttLvlInitRef(STsdb *pTsdb, const SSttLvl *lvl1, SSttLvl **lvl) {
  int32_t code = tsdbSttLvlInit(lvl1->level, lvl);
  if (code) return code;

  STFileObj *fobj1;
  TARRAY2_FOREACH(lvl1->fobjArr, fobj1) {
    code = tsdbTFileObjRef(fobj1);
    if (code) {
      tsdbSttLvlClear(lvl);
      return code;
    }
    code = TARRAY2_APPEND(lvl[0]->fobjArr, fobj1);
    if (code) {
      if (tsdbTFileObjUnref(fobj1) != 0) {
        tsdbError("failed to unref file obj, fobj:%p", fobj1);
      }
      tsdbSttLvlClear(lvl);
      return code;
    }
  }
  return 0;
}

static int32_t tsdbSttLvlFilteredInitEx(STsdb *pTsdb, const SSttLvl *lvl1, int64_t ever, SSttLvl **lvl,
                                        TFileOpArray *fopArr) {
  int32_t code = tsdbSttLvlInit(lvl1->level, lvl);
  if (code) return code;

  const STFileObj *fobj1;
  TARRAY2_FOREACH(lvl1->fobjArr, fobj1) {
    if (fobj1->f->maxVer <= ever) {
      STFileObj *fobj;
      code = tsdbTFileObjInit(pTsdb, fobj1->f, &fobj);
      if (code) {
        tsdbSttLvlClear(lvl);
        return code;
      }

      TAOS_CHECK_RETURN(TARRAY2_APPEND(lvl[0]->fobjArr, fobj));
    } else {
      STFileOp op = {
          .optype = TSDB_FOP_REMOVE,
          .fid = fobj1->f->fid,
          .of = fobj1->f[0],
      };
      TAOS_CHECK_RETURN(TARRAY2_APPEND(fopArr, op));
    }
  }
  return 0;
}

static void tsdbSttLvlRemoveFObj(void *data) {
  int32_t code = tsdbTFileObjRemove(*(STFileObj **)data);
  if (code) {
    tsdbError("failed to remove file obj, code:%d, error:%s", code, tstrerror(code));
  }
}
static void tsdbSttLvlRemove(SSttLvl **lvl) {
  TARRAY2_DESTROY(lvl[0]->fobjArr, tsdbSttLvlRemoveFObj);
  taosMemoryFree(lvl[0]);
  lvl[0] = NULL;
}

static int32_t tsdbSttLvlApplyEdit(STsdb *pTsdb, const SSttLvl *lvl1, SSttLvl *lvl2) {
  int32_t code = 0;

  if (lvl1->level != lvl2->level) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t i1 = 0, i2 = 0;
  while (i1 < TARRAY2_SIZE(lvl1->fobjArr) || i2 < TARRAY2_SIZE(lvl2->fobjArr)) {
    STFileObj *fobj1 = i1 < TARRAY2_SIZE(lvl1->fobjArr) ? TARRAY2_GET(lvl1->fobjArr, i1) : NULL;
    STFileObj *fobj2 = i2 < TARRAY2_SIZE(lvl2->fobjArr) ? TARRAY2_GET(lvl2->fobjArr, i2) : NULL;

    if (fobj1 && fobj2) {
      if (fobj1->f->cid < fobj2->f->cid) {
        // create a file obj
        code = tsdbTFileObjInit(pTsdb, fobj1->f, &fobj2);
        if (code) return code;
        code = TARRAY2_INSERT_PTR(lvl2->fobjArr, i2, &fobj2);
        if (code) return code;
        i1++;
        i2++;
      } else if (fobj1->f->cid > fobj2->f->cid) {
        // remove a file obj
        TARRAY2_REMOVE(lvl2->fobjArr, i2, tsdbSttLvlRemoveFObj);
      } else {
        if (tsdbIsSameTFile(fobj1->f, fobj2->f)) {
          if (tsdbIsTFileChanged(fobj1->f, fobj2->f)) {
            fobj2->f[0] = fobj1->f[0];
          }
        } else {
          TARRAY2_REMOVE(lvl2->fobjArr, i2, tsdbSttLvlRemoveFObj);
          code = tsdbTFileObjInit(pTsdb, fobj1->f, &fobj2);
          if (code) return code;
          code = TARRAY2_SORT_INSERT(lvl2->fobjArr, fobj2, tsdbTFileObjCmpr);
          if (code) return code;
        }
        i1++;
        i2++;
      }
    } else if (fobj1) {
      // create a file obj
      code = tsdbTFileObjInit(pTsdb, fobj1->f, &fobj2);
      if (code) return code;
      code = TARRAY2_INSERT_PTR(lvl2->fobjArr, i2, &fobj2);
      if (code) return code;
      i1++;
      i2++;
    } else {
      // remove a file obj
      TARRAY2_REMOVE(lvl2->fobjArr, i2, tsdbSttLvlRemoveFObj);
    }
  }
  return 0;
}

static int32_t tsdbSttLvlCmprFn(const SSttLvl **lvl1, const SSttLvl **lvl2) {
  if (lvl1[0]->level < lvl2[0]->level) return -1;
  if (lvl1[0]->level > lvl2[0]->level) return 1;
  return 0;
}

static int32_t tsdbSttLvlToJson(const SSttLvl *lvl, cJSON *json) {
  if (cJSON_AddNumberToObject(json, "level", lvl->level) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  cJSON *ajson = cJSON_AddArrayToObject(json, "files");
  if (ajson == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  const STFileObj *fobj;
  TARRAY2_FOREACH(lvl->fobjArr, fobj) {
    cJSON *item = cJSON_CreateObject();
    if (item == NULL) return TSDB_CODE_OUT_OF_MEMORY;
    (void)cJSON_AddItemToArray(ajson, item);

    int32_t code = tsdbTFileToJson(fobj->f, item);
    if (code) return code;
  }

  return 0;
}

static int32_t tsdbJsonToSttLvl(STsdb *pTsdb, const cJSON *json, SSttLvl **lvl) {
  const cJSON *item1, *item2;
  int32_t      level;

  item1 = cJSON_GetObjectItem(json, "level");
  if (cJSON_IsNumber(item1)) {
    level = item1->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  int32_t code = tsdbSttLvlInit(level, lvl);
  if (code) return code;

  item1 = cJSON_GetObjectItem(json, "files");
  if (!cJSON_IsArray(item1)) {
    tsdbSttLvlClear(lvl);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  cJSON_ArrayForEach(item2, item1) {
    STFile tf;
    code = tsdbJsonToTFile(item2, TSDB_FTYPE_STT, &tf);
    if (code) {
      tsdbSttLvlClear(lvl);
      return code;
    }

    STFileObj *fobj;
    code = tsdbTFileObjInit(pTsdb, &tf, &fobj);
    if (code) {
      tsdbSttLvlClear(lvl);
      return code;
    }

    code = TARRAY2_APPEND(lvl[0]->fobjArr, fobj);
    if (code) return code;
  }
  TARRAY2_SORT(lvl[0]->fobjArr, tsdbTFileObjCmpr);
  return 0;
}

int32_t tsdbTFileSetToJson(const STFileSet *fset, cJSON *json) {
  int32_t code = 0;
  cJSON  *item1, *item2;

  // fid
  if (cJSON_AddNumberToObject(json, "fid", fset->fid) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset->farr[ftype] == NULL) continue;

    code = tsdbTFileToJson(fset->farr[ftype]->f, json);
    if (code) return code;
  }

  // each level
  item1 = cJSON_AddArrayToObject(json, "stt lvl");
  if (item1 == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  const SSttLvl *lvl;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    item2 = cJSON_CreateObject();
    if (!item2) return TSDB_CODE_OUT_OF_MEMORY;
    (void)cJSON_AddItemToArray(item1, item2);

    code = tsdbSttLvlToJson(lvl, item2);
    if (code) return code;
  }

  // about compact and commit
  if (cJSON_AddNumberToObject(json, "last compact", fset->lastCompact) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (cJSON_AddNumberToObject(json, "last commit", fset->lastCommit) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return 0;
}

int32_t tsdbJsonToTFileSet(STsdb *pTsdb, const cJSON *json, STFileSet **fset) {
  int32_t      code;
  const cJSON *item1, *item2;
  int32_t      fid;
  STFile       tf;

  // fid
  item1 = cJSON_GetObjectItem(json, "fid");
  if (cJSON_IsNumber(item1)) {
    fid = item1->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  code = tsdbTFileSetInit(fid, fset);
  if (code) return code;

  for (tsdb_ftype_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    code = tsdbJsonToTFile(json, ftype, &tf);
    if (code == TSDB_CODE_NOT_FOUND) {
      continue;
    } else if (code) {
      tsdbTFileSetClear(fset);
      return code;
    } else {
      code = tsdbTFileObjInit(pTsdb, &tf, &(*fset)->farr[ftype]);
      if (code) return code;
    }
  }

  // each level
  item1 = cJSON_GetObjectItem(json, "stt lvl");
  if (cJSON_IsArray(item1)) {
    cJSON_ArrayForEach(item2, item1) {
      SSttLvl *lvl;
      code = tsdbJsonToSttLvl(pTsdb, item2, &lvl);
      if (code) {
        tsdbTFileSetClear(fset);
        return code;
      }

      code = TARRAY2_APPEND((*fset)->lvlArr, lvl);
      if (code) return code;
    }
    TARRAY2_SORT((*fset)->lvlArr, tsdbSttLvlCmprFn);
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }
  // about compact and commit
  item1 = cJSON_GetObjectItem(json, "last compact");
  if (cJSON_IsNumber(item1)) {
    (*fset)->lastCompact = item1->valuedouble;
  } else {
    (*fset)->lastCompact = 0;
  }

  item1 = cJSON_GetObjectItem(json, "last commit");
  if (cJSON_IsNumber(item1)) {
    (*fset)->lastCommit = item1->valuedouble;
  } else {
    (*fset)->lastCommit = 0;
  }

  return 0;
}

// NOTE: the api does not remove file (seems this is not true?), only do memory operation
int32_t tsdbTFileSetEdit(STsdb *pTsdb, STFileSet *fset, const STFileOp *op) {
  int32_t code = 0;

  if (op->optype == TSDB_FOP_CREATE) {
    // create a new file
    STFileObj *fobj;
    code = tsdbTFileObjInit(pTsdb, &op->nf, &fobj);
    if (code) return code;

    if (fobj->f->type == TSDB_FTYPE_STT) {
      SSttLvl *lvl = tsdbTFileSetGetSttLvl(fset, fobj->f->stt->level);
      if (!lvl) {
        code = tsdbSttLvlInit(fobj->f->stt->level, &lvl);
        if (code) return code;

        code = TARRAY2_SORT_INSERT(fset->lvlArr, lvl, tsdbSttLvlCmprFn);
        if (code) return code;
      }

      code = TARRAY2_SORT_INSERT(lvl->fobjArr, fobj, tsdbTFileObjCmpr);
      if (code) return code;
    } else {
      fset->farr[fobj->f->type] = fobj;
    }
  } else if (op->optype == TSDB_FOP_REMOVE) {
    // delete a file
    if (op->of.type == TSDB_FTYPE_STT) {
      SSttLvl *lvl = tsdbTFileSetGetSttLvl(fset, op->of.stt->level);

      STFileObj  tfobj = {.f[0] = {.cid = op->of.cid}};
      STFileObj *tfobjp = &tfobj;
      int32_t    idx = TARRAY2_SEARCH_IDX(lvl->fobjArr, &tfobjp, tsdbTFileObjCmpr, TD_EQ);
      TARRAY2_REMOVE(lvl->fobjArr, idx, tsdbSttLvlClearFObj);
    } else {
      code = tsdbTFileObjUnref(fset->farr[op->of.type]);
      if (code) return code;
      fset->farr[op->of.type] = NULL;
    }
  } else {
    if (op->nf.type == TSDB_FTYPE_STT) {
      SSttLvl *lvl = tsdbTFileSetGetSttLvl(fset, op->of.stt->level);

      STFileObj   tfobj = {.f[0] = {.cid = op->of.cid}}, *tfobjp = &tfobj;
      STFileObj **fobjPtr = TARRAY2_SEARCH(lvl->fobjArr, &tfobjp, tsdbTFileObjCmpr, TD_EQ);
      if (fobjPtr) {
        tfobjp = *fobjPtr;
        tfobjp->f[0] = op->nf;
      } else {
        tsdbError("file not found, cid:%" PRId64, op->of.cid);
      }
    } else {
      fset->farr[op->nf.type]->f[0] = op->nf;
    }
  }

  return 0;
}

int32_t tsdbTFileSetApplyEdit(STsdb *pTsdb, const STFileSet *fset1, STFileSet *fset2) {
  int32_t code = 0;

  if (fset1->fid != fset2->fid) {
    return TSDB_CODE_INVALID_PARA;
  }

  for (tsdb_ftype_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (!fset1->farr[ftype] && !fset2->farr[ftype]) continue;

    STFileObj *fobj1 = fset1->farr[ftype];
    STFileObj *fobj2 = fset2->farr[ftype];

    if (fobj1 && fobj2) {
      if (tsdbIsSameTFile(fobj1->f, fobj2->f)) {
        if (tsdbIsTFileChanged(fobj1->f, fobj2->f)) {
          fobj2->f[0] = fobj1->f[0];
        }
      } else {
        if (fobj1->f->cid != fobj2->f->cid) {
          code = tsdbTFileObjRemove(fobj2);
          if (code) return code;
        } else {
          code = tsdbTFileObjRemoveUpdateLC(fobj2);
          if (code) return code;
        }
        code = tsdbTFileObjInit(pTsdb, fobj1->f, &fset2->farr[ftype]);
        if (code) return code;
      }
    } else if (fobj1) {
      // create a new file
      code = tsdbTFileObjInit(pTsdb, fobj1->f, &fset2->farr[ftype]);
      if (code) return code;
    } else {
      // remove the file
      code = tsdbTFileObjRemove(fobj2);
      if (code) return code;
      fset2->farr[ftype] = NULL;
    }
  }

  // stt part
  int32_t i1 = 0, i2 = 0;
  while (i1 < TARRAY2_SIZE(fset1->lvlArr) || i2 < TARRAY2_SIZE(fset2->lvlArr)) {
    SSttLvl *lvl1 = i1 < TARRAY2_SIZE(fset1->lvlArr) ? TARRAY2_GET(fset1->lvlArr, i1) : NULL;
    SSttLvl *lvl2 = i2 < TARRAY2_SIZE(fset2->lvlArr) ? TARRAY2_GET(fset2->lvlArr, i2) : NULL;

    if (lvl1 && lvl2) {
      if (lvl1->level < lvl2->level) {
        // add a new stt level
        code = tsdbSttLvlInitEx(pTsdb, lvl1, &lvl2);
        if (code) return code;
        code = TARRAY2_SORT_INSERT(fset2->lvlArr, lvl2, tsdbSttLvlCmprFn);
        if (code) return code;
        i1++;
        i2++;
      } else if (lvl1->level > lvl2->level) {
        // remove the stt level
        TARRAY2_REMOVE(fset2->lvlArr, i2, tsdbSttLvlRemove);
      } else {
        // apply edit on stt level
        code = tsdbSttLvlApplyEdit(pTsdb, lvl1, lvl2);
        if (code) return code;
        i1++;
        i2++;
      }
    } else if (lvl1) {
      // add a new stt level
      code = tsdbSttLvlInitEx(pTsdb, lvl1, &lvl2);
      if (code) return code;
      code = TARRAY2_SORT_INSERT(fset2->lvlArr, lvl2, tsdbSttLvlCmprFn);
      if (code) return code;
      i1++;
      i2++;
    } else {
      // remove the stt level
      TARRAY2_REMOVE(fset2->lvlArr, i2, tsdbSttLvlRemove);
    }
  }

  fset2->lastCompact = fset1->lastCompact;
  fset2->lastCommit = fset1->lastCommit;

  return 0;
}

int32_t tsdbTFileSetInit(int32_t fid, STFileSet **fset) {
  fset[0] = taosMemoryCalloc(1, sizeof(STFileSet));
  if (fset[0] == NULL) {
    return terrno;
  }

  fset[0]->fid = fid;
  fset[0]->maxVerValid = VERSION_MAX;
  TARRAY2_INIT(fset[0]->lvlArr);

  // block commit variables
  (void)taosThreadCondInit(&fset[0]->canCommit, NULL);
  (*fset)->numWaitCommit = 0;
  (*fset)->blockCommit = false;

  for (int32_t i = 0; i < sizeof((*fset)->conds) / sizeof((*fset)->conds[0]); ++i) {
    struct STFileSetCond *cond = &(*fset)->conds[i];
    cond->running = false;
    cond->numWait = 0;
    (void)taosThreadCondInit(&cond->cond, NULL);
  }

  return 0;
}

int32_t tsdbTFileSetInitCopy(STsdb *pTsdb, const STFileSet *fset1, STFileSet **fset) {
  int32_t code = tsdbTFileSetInit(fset1->fid, fset);
  if (code) return code;

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset1->farr[ftype] == NULL) continue;

    code = tsdbTFileObjInit(pTsdb, fset1->farr[ftype]->f, &fset[0]->farr[ftype]);
    if (code) {
      tsdbTFileSetClear(fset);
      return code;
    }
  }

  const SSttLvl *lvl1;
  TARRAY2_FOREACH(fset1->lvlArr, lvl1) {
    SSttLvl *lvl;
    code = tsdbSttLvlInitEx(pTsdb, lvl1, &lvl);
    if (code) {
      tsdbTFileSetClear(fset);
      return code;
    }

    code = TARRAY2_APPEND(fset[0]->lvlArr, lvl);
    if (code) return code;
  }

  (*fset)->lastCompact = fset1->lastCompact;
  (*fset)->lastCommit = fset1->lastCommit;

  return 0;
}

int32_t tsdbTFileSetFilteredInitDup(STsdb *pTsdb, const STFileSet *fset1, int64_t ever, STFileSet **fset,
                                    TFileOpArray *fopArr) {
  int32_t code = tsdbTFileSetInit(fset1->fid, fset);
  if (code) return code;

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset1->farr[ftype] == NULL) continue;
    STFileObj *fobj = fset1->farr[ftype];
    if (fobj->f->maxVer <= ever) {
      code = tsdbTFileObjInit(pTsdb, fobj->f, &fset[0]->farr[ftype]);
      if (code) {
        tsdbTFileSetClear(fset);
        return code;
      }
    } else {
      STFileOp op = {
          .optype = TSDB_FOP_REMOVE,
          .fid = fobj->f->fid,
          .of = fobj->f[0],
      };
      code = TARRAY2_APPEND(fopArr, op);
      if (code) return code;
    }
  }

  const SSttLvl *lvl1;
  TARRAY2_FOREACH(fset1->lvlArr, lvl1) {
    SSttLvl *lvl;
    code = tsdbSttLvlFilteredInitEx(pTsdb, lvl1, ever, &lvl, fopArr);
    if (code) {
      tsdbTFileSetClear(fset);
      return code;
    }

    code = TARRAY2_APPEND(fset[0]->lvlArr, lvl);
    if (code) return code;
  }

  return 0;
}

int32_t tsdbTFileSetRangeInitRef(STsdb *pTsdb, const STFileSet *fset1, int64_t sver, int64_t ever,
                                 STFileSetRange **fsr) {
  fsr[0] = taosMemoryCalloc(1, sizeof(*fsr[0]));
  if (fsr[0] == NULL) {
    return terrno;
  }
  fsr[0]->fid = fset1->fid;
  fsr[0]->sver = sver;
  fsr[0]->ever = ever;

  int32_t code = tsdbTFileSetInitRef(pTsdb, fset1, &fsr[0]->fset);
  if (code) {
    taosMemoryFree(fsr[0]);
    fsr[0] = NULL;
  }
  return code;
}

int32_t tsdbTFileSetInitRef(STsdb *pTsdb, const STFileSet *fset1, STFileSet **fset) {
  int32_t code = tsdbTFileSetInit(fset1->fid, fset);
  if (code) return code;

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset1->farr[ftype] == NULL) continue;

    code = tsdbTFileObjRef(fset1->farr[ftype]);
    if (code) {
      tsdbTFileSetClear(fset);
      return code;
    }
    fset[0]->farr[ftype] = fset1->farr[ftype];
  }

  const SSttLvl *lvl1;
  TARRAY2_FOREACH(fset1->lvlArr, lvl1) {
    SSttLvl *lvl;
    code = tsdbSttLvlInitRef(pTsdb, lvl1, &lvl);
    if (code) {
      tsdbSttLvlClear(&lvl);
      tsdbTFileSetClear(fset);
      return code;
    }

    code = TARRAY2_APPEND(fset[0]->lvlArr, lvl);
    if (code) {
      tsdbSttLvlClear(&lvl);
      tsdbTFileSetClear(fset);
      return code;
    }
  }

  (*fset)->lastCompact = fset1->lastCompact;
  (*fset)->lastCommit = fset1->lastCommit;

  return 0;
}

void tsdbTFileSetRangeClear(STFileSetRange **fsr) {
  if (!fsr[0]) return;

  tsdbTFileSetClear(&fsr[0]->fset);
  taosMemoryFree(fsr[0]);
  fsr[0] = NULL;
  return;
}

void tsdbTFileSetRangeArrayDestroy(TFileSetRangeArray **ppArr) {
  if (ppArr && ppArr[0]) {
    TARRAY2_DESTROY(ppArr[0], tsdbTFileSetRangeClear);
    taosMemoryFree(ppArr[0]);
    ppArr[0] = NULL;
  }
}

void tsdbTFileSetClear(STFileSet **fset) {
  if (fset && *fset) {
    for (tsdb_ftype_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
      if ((*fset)->farr[ftype] == NULL) continue;
      int32_t code = tsdbTFileObjUnref((*fset)->farr[ftype]);
      if (code) {
        tsdbError("failed to unref file, fid:%d, ftype:%d", (*fset)->fid, ftype);
      }
      (*fset)->farr[ftype] = NULL;
    }

    TARRAY2_DESTROY((*fset)->lvlArr, tsdbSttLvlClear);

    (void)taosThreadCondDestroy(&(*fset)->canCommit);
    for (int32_t i = 0; i < sizeof((*fset)->conds) / sizeof((*fset)->conds[0]); ++i) {
      (void)taosThreadCondDestroy(&(*fset)->conds[i].cond);
    }
    taosMemoryFreeClear(*fset);
  }
}

void tsdbTFileSetRemove(STFileSet *fset) {
  if (fset == NULL) return;

  for (tsdb_ftype_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset->farr[ftype] != NULL) {
      int32_t code = tsdbTFileObjRemove(fset->farr[ftype]);
      if (code) {
        tsdbError("failed to remove file, fid:%d, ftype:%d", fset->fid, ftype);
      }
      fset->farr[ftype] = NULL;
    }
  }

  TARRAY2_DESTROY(fset->lvlArr, tsdbSttLvlRemove);
}

SSttLvl *tsdbTFileSetGetSttLvl(STFileSet *fset, int32_t level) {
  SSttLvl   sttLvl = {.level = level};
  SSttLvl  *lvl = &sttLvl;
  SSttLvl **lvlPtr = TARRAY2_SEARCH(fset->lvlArr, &lvl, tsdbSttLvlCmprFn, TD_EQ);
  return lvlPtr ? lvlPtr[0] : NULL;
}

int32_t tsdbTFileSetCmprFn(const STFileSet **fset1, const STFileSet **fset2) {
  if (fset1[0]->fid < fset2[0]->fid) return -1;
  if (fset1[0]->fid > fset2[0]->fid) return 1;
  return 0;
}

int64_t tsdbTFileSetMaxCid(const STFileSet *fset) {
  int64_t maxCid = 0;
  for (tsdb_ftype_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset->farr[ftype] == NULL) continue;
    maxCid = TMAX(maxCid, fset->farr[ftype]->f->cid);
  }
  const SSttLvl   *lvl;
  const STFileObj *fobj;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    TARRAY2_FOREACH(lvl->fobjArr, fobj) { maxCid = TMAX(maxCid, fobj->f->cid); }
  }
  return maxCid;
}

bool tsdbTFileSetIsEmpty(const STFileSet *fset) {
  for (tsdb_ftype_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset->farr[ftype] != NULL) return false;
  }
  return TARRAY2_SIZE(fset->lvlArr) == 0;
}
