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

static int32_t stt_lvl_to_json(const SSttLvl *lvl, cJSON *json) {
  if (cJSON_AddNumberToObject(json, "lvl", lvl->lvl) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  cJSON *ajson = cJSON_AddArrayToObject(json, "files");
  if (ajson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  SRBTreeIter iter = tRBTreeIterCreate(&lvl->sttTree, 1);
  for (SRBTreeNode *node = tRBTreeIterNext(&iter); node; node = tRBTreeIterNext(&iter)) {
    STFileObj *fobj = TCONTAINER_OF(node, STFileObj, rbtn);

    cJSON *item = cJSON_CreateObject();
    if (item == NULL) return TSDB_CODE_OUT_OF_MEMORY;
    cJSON_AddItemToArray(ajson, item);

    int32_t code = tsdbTFileToJson(&fobj->f, item);
    if (code) return code;
  }

  return 0;
}

static int32_t stt_file_cmpr(const SRBTreeNode *n1, const SRBTreeNode *n2) {
  STFileObj *f1 = TCONTAINER_OF(n1, STFileObj, rbtn);
  STFileObj *f2 = TCONTAINER_OF(n2, STFileObj, rbtn);
  if (f1->f.cid < f2->f.cid) {
    return -1;
  } else if (f1->f.cid > f2->f.cid) {
    return 1;
  }
  return 0;
}

static int32_t stt_lvl_init(SSttLvl *lvl) {
  lvl->lvl = 0;
  lvl->nstt = 0;
  tRBTreeCreate(&lvl->sttTree, stt_file_cmpr);
  return 0;
}

static int32_t add_file_to_stt_lvl(SSttLvl *lvl, STFileObj *fobj) {
  lvl->nstt++;
  tRBTreePut(&lvl->sttTree, &fobj->rbtn);
  return 0;
}

static int32_t json_to_stt_lvl(const cJSON *json, SSttLvl *lvl) {
  stt_lvl_init(lvl);

  const cJSON *item1, *item2;

  item1 = cJSON_GetObjectItem(json, "lvl");
  if (cJSON_IsNumber(item1)) {
    lvl->lvl = item1->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  item1 = cJSON_GetObjectItem(json, "files");
  if (cJSON_IsArray(item1)) {
    cJSON_ArrayForEach(item2, item1) {
      STFileObj *fobj;

      int32_t code = tsdbTFileObjCreate(&fobj);
      if (code) return code;

      code = tsdbJsonToTFile(item2, TSDB_FTYPE_STT, &fobj->f);
      if (code) return code;

      add_file_to_stt_lvl(lvl, fobj);
    }
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  return 0;
}

static int32_t add_file(STFileSet *fset, STFile *f) {
  if (f->type == TSDB_FTYPE_STT) {
    SSttLvl *lvl = NULL;  // TODO

    // lvl->nstt++;
    // lvl->fstt = f;
  } else {
    // fset->farr[f->type] = f;
  }

  return 0;
}

static int32_t add_stt_lvl(STFileSet *fset, SSttLvl *lvl) {
  tRBTreePut(&fset->lvlTree, &lvl->rbtn);
  return 0;
}

static int32_t stt_lvl_cmpr(const SRBTreeNode *n1, const SRBTreeNode *n2) {
  SSttLvl *lvl1 = TCONTAINER_OF(n1, SSttLvl, rbtn);
  SSttLvl *lvl2 = TCONTAINER_OF(n2, SSttLvl, rbtn);

  if (lvl1->lvl < lvl2->lvl) {
    return -1;
  } else if (lvl1->lvl > lvl2->lvl) {
    return 1;
  }
  return 0;
}

static int32_t fset_init(STFileSet *fset) {
  memset(fset, 0, sizeof(*fset));
  tRBTreeCreate(&fset->lvlTree, stt_lvl_cmpr);
  return 0;
}

static int32_t fset_clear(STFileSet *fset) {
  // TODO
  return 0;
}

int32_t tsdbFileSetToJson(const STFileSet *fset, cJSON *json) {
  int32_t code = 0;

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
  cJSON *ajson = cJSON_AddArrayToObject(json, "stt");
  if (ajson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  SRBTreeIter iter = tRBTreeIterCreate(&fset->lvlTree, 1);
  for (SRBTreeNode *node = tRBTreeIterNext(&iter); node; node = tRBTreeIterNext(&iter)) {
    SSttLvl *lvl = TCONTAINER_OF(node, SSttLvl, rbtn);
    code = stt_lvl_to_json(lvl, ajson);
    if (code) return code;
  }

  return 0;
}

int32_t tsdbJsonToFileSet(const cJSON *json, STFileSet *fset) {
  const cJSON *item1, *item2;

  fset_init(fset);

  /* fid */
  item1 = cJSON_GetObjectItem(json, "fid");
  if (cJSON_IsNumber(item1)) {
    fset->fid = item1->valueint;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  int32_t code;
  STFile  tf;
  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    code = tsdbJsonToTFile(json, ftype, &tf);
    if (code == TSDB_CODE_NOT_FOUND) {
      continue;
    } else if (code) {
      return code;
    } else {
      code = tsdbTFileObjCreate(&fset->farr[ftype]);
      if (code) return code;
      fset->farr[ftype]->f = tf;
    }
  }

  // each level
  item1 = cJSON_GetObjectItem(json, "stt");
  if (cJSON_IsArray(item1)) {
    cJSON_ArrayForEach(item2, item1) {
      SSttLvl *lvl = taosMemoryCalloc(1, sizeof(*lvl));
      if (lvl == NULL) return TSDB_CODE_OUT_OF_MEMORY;

      code = json_to_stt_lvl(item2, lvl);
      if (code) {
        taosMemoryFree(lvl);
        return code;
      }

      add_stt_lvl(fset, lvl);
    }
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  return 0;
}

int32_t tsdbFSetCmprFn(const STFileSet *pSet1, const STFileSet *pSet2) {
  if (pSet1->fid < pSet2->fid) return -1;
  if (pSet1->fid > pSet2->fid) return 1;
  return 0;
}

int32_t tsdbFSetEdit(STFileSet *fset, const STFileOp *op) {
  int32_t code;

  ASSERT(fset->fid == op->fid);

  if (op->oState.size == 0) {
    // create
    // STFile *f;
    // code = tsdbTFileCreate(&op->nState, &f);
    // if (code) return code;

    // add_file(fset, f);
  } else if (op->nState.size == 0) {
    // delete
  } else {
    // modify
  }
  return 0;
}

int32_t tsdbFileSetInit(STFileSet *pSet) {
  for (tsdb_ftype_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ftype++) {
    pSet->farr[ftype] = NULL;
  }

  // LISTD_INIT(&pSet->lvl0, listNode);
  // pSet->lvl0.lvl = 0;
  // pSet->lvl0.nstt = 0;
  // pSet->lvl0.fstt = NULL;
  return 0;
}

int32_t tsdbFileSetClear(STFileSet *pSet) {
  // TODO
  return 0;
}
