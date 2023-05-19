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

#include "inc/tsdbFSet.h"

static int32_t tsdbSttLvlInit(int32_t level, SSttLvl **lvl) {
  lvl[0] = taosMemoryMalloc(sizeof(SSttLvl));
  if (lvl[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  lvl[0]->level = level;
  TARRAY2_INIT(&lvl[0]->farr);
  return 0;
}
static void    tsdbSttLvlClearFObj(void *data) { tsdbTFileObjUnref(*(STFileObj **)data); }
static int32_t tsdbSttLvlClear(SSttLvl **lvl) {
  TARRAY2_CLEAR_FREE(&lvl[0]->farr, tsdbSttLvlClearFObj);
  taosMemoryFree(lvl[0]);
  lvl[0] = NULL;
  return 0;
}
static int32_t tsdbSttLvlInitEx(const SSttLvl *lvl1, SSttLvl **lvl) {
  int32_t code = tsdbSttLvlInit(lvl1->level, lvl);
  if (code) return code;

  const STFileObj *fobj1;
  TARRAY2_FOREACH(&lvl1->farr, fobj1) {
    STFileObj *fobj;
    code = tsdbTFileObjInit(&fobj1->f, &fobj);
    if (code) {
      tsdbSttLvlClear(lvl);
      return code;
    }

    TARRAY2_APPEND(&lvl[0]->farr, fobj);
  }
  return 0;
}

static int32_t tsdbSttLvlToJson(const SSttLvl *lvl, cJSON *json) {
  if (cJSON_AddNumberToObject(json, "level", lvl->level) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  cJSON *ajson = cJSON_AddArrayToObject(json, "files");
  if (ajson == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  const STFileObj *fobj;
  TARRAY2_FOREACH(&lvl->farr, fobj) {
    cJSON *item = cJSON_CreateObject();
    if (item == NULL) return TSDB_CODE_OUT_OF_MEMORY;
    cJSON_AddItemToArray(ajson, item);

    int32_t code = tsdbTFileToJson(&fobj->f, item);
    if (code) return code;
  }

  return 0;
}

static int32_t tsdbJsonToSttLvl(const cJSON *json, SSttLvl **lvl) {
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
    code = tsdbTFileObjInit(&tf, &fobj);
    if (code) {
      tsdbSttLvlClear(lvl);
      return code;
    }

    TARRAY2_APPEND(&lvl[0]->farr, fobj);
  }
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

    code = tsdbTFileToJson(&fset->farr[ftype]->f, json);
    if (code) return code;
  }

  // each level
  item1 = cJSON_AddArrayToObject(json, "stt lvl");
  if (item1 == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  const SSttLvl *lvl;
  TARRAY2_FOREACH(&fset->lvlArr, lvl) {
    item2 = cJSON_CreateObject();
    if (!item2) return TSDB_CODE_OUT_OF_MEMORY;
    cJSON_AddItemToArray(item1, item2);

    code = tsdbSttLvlToJson(lvl, item2);
    if (code) return code;
  }

  return 0;
}

int32_t tsdbJsonToTFileSet(const cJSON *json, STFileSet **fset) {
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
    }
  }

  // each level
  item1 = cJSON_GetObjectItem(json, "stt lvl");
  if (cJSON_IsArray(item1)) {
    cJSON_ArrayForEach(item2, item1) {
      SSttLvl *lvl;
      code = tsdbJsonToSttLvl(item2, &lvl);
      if (code) {
        tsdbTFileSetClear(fset);
        return code;
      }

      TARRAY2_APPEND(&(*fset)->lvlArr, lvl);
    }
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  return 0;
}

int32_t tsdbTFileSetEdit(STFileSet *fset, const STFileOp *op) {
  int32_t code = 0;

  // if (op->oState.size == 0  //
  //     || 0                  /* TODO*/
  // ) {
  //   STFileObj *fobj;
  //   // code = tsdbTFileObjCreate(&fobj);
  //   if (code) return code;
  //   fobj->f = op->nState;
  //   add_file_to_fset(fset, fobj);
  // } else if (op->nState.size == 0) {
  //   // delete
  //   ASSERT(0);
  // } else {
  //   // modify
  //   ASSERT(0);
  // }
  return 0;
}

int32_t tsdbTFileSetEditEx(const STFileSet *fset1, STFileSet *fset) {
  ASSERT(fset1->fid == fset->fid);
  // TODO
  return 0;
}

int32_t tsdbTFileSetInit(int32_t fid, STFileSet **fset) {
  fset[0] = taosMemoryCalloc(1, sizeof(STFileSet));
  if (fset[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  fset[0]->fid = fid;
  TARRAY2_INIT(&fset[0]->lvlArr);
  return 0;
}

int32_t tsdbTFileSetInitEx(const STFileSet *fset1, STFileSet **fset) {
  int32_t code = tsdbTFileSetInit(fset1->fid, fset);
  if (code) return code;

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset1->farr[ftype] == NULL) continue;

    code = tsdbTFileObjInit(&fset1->farr[ftype]->f, &fset[0]->farr[ftype]);
    if (code) {
      tsdbTFileSetClear(fset);
      return code;
    }
  }

  const SSttLvl *lvl1;
  TARRAY2_FOREACH(&fset1->lvlArr, lvl1) {
    SSttLvl *lvl;
    code = tsdbSttLvlInitEx(lvl1, &lvl);
    if (code) {
      tsdbTFileSetClear(fset);
      return code;
    }

    TARRAY2_APPEND(&fset[0]->lvlArr, lvl);
  }

  return 0;
}

int32_t tsdbTFileSetClear(STFileSet **fset) {
  if (!fset[0]) return 0;

  for (tsdb_ftype_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset[0]->farr[ftype] == NULL) continue;
    tsdbTFileObjUnref(fset[0]->farr[ftype]);
  }

  TARRAY2_CLEAR_FREE(&fset[0]->lvlArr, tsdbSttLvlClear);

  taosMemoryFree(fset[0]);
  fset[0] = NULL;

  return 0;
}

const SSttLvl *tsdbTFileSetGetLvl(const STFileSet *fset, int32_t level) {
  // SSttLvl      tlvl = {.level = level};
  // SRBTreeNode *node = tRBTreeGet(&fset->lvlTree, &tlvl.rbtn);
  // return node ? TCONTAINER_OF(node, SSttLvl, rbtn) : NULL;
  // TODO
  return NULL;
}

int32_t tsdbTFileSetCmprFn(const STFileSet **fset1, const STFileSet **fset2) {
  if (fset1[0]->fid < fset2[0]->fid) return -1;
  if (fset1[0]->fid > fset2[0]->fid) return 1;
  return 0;
}