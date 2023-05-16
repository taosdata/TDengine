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

  cJSON *ajson = cJSON_AddArrayToObject(json, "stt");
  if (ajson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  SRBTreeIter iter = tRBTreeIterCreate(&lvl->sttTree, 1);
  for (SRBTreeNode *node = tRBTreeIterNext(&iter); node; node = tRBTreeIterNext(&iter)) {
    STFileObj *fobj = TCONTAINER_OF(node, STFileObj, rbtn);

    cJSON *item = cJSON_CreateObject();
    if (item == NULL) return TSDB_CODE_OUT_OF_MEMORY;

    int32_t code = tsdbTFileToJson(&fobj->f, item);
    if (code) {
      cJSON_Delete(item);
      return code;
    }

    cJSON_AddItemToArray(ajson, item);
  }

  return 0;
}

static int32_t stt_lvl_from_json(const cJSON *json, SSttLvl *lvl) {
  // TODO
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

int32_t tsdbFileSetToJson(const STFileSet *fset, cJSON *json) {
  int32_t code = 0;

  // fid
  if (cJSON_AddNumberToObject(json, "fid", fset->fid) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset->farr[ftype] == NULL) {
      continue;
    }

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

int32_t tsdbFileSetFromJson(const cJSON *json, STFileSet *fset) {
  const cJSON *item;

  /* fid */
  item = cJSON_GetObjectItem(json, "fid");
  if (cJSON_IsNumber(item)) {
    fset->fid = item->valueint;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    // int32_t code = tsdbTFileFromJson(json, ftype, &fset->farr[ftype]->f);
    // if (code) return code;
  }

  // each level
  item = cJSON_GetObjectItem(json, "stt");
  if (cJSON_IsArray(item)) {
    // TODO
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
