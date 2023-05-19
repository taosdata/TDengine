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
  if (cJSON_AddNumberToObject(json, "level", lvl->level) == NULL) {
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

static int32_t stt_lvl_init(SSttLvl *lvl, int32_t level) {
  lvl->level = level;
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
  const cJSON *item1, *item2;

  item1 = cJSON_GetObjectItem(json, "level");
  if (cJSON_IsNumber(item1)) {
    lvl->level = item1->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  stt_lvl_init(lvl, lvl->level);

  item1 = cJSON_GetObjectItem(json, "files");
  if (cJSON_IsArray(item1)) {
    cJSON_ArrayForEach(item2, item1) {
      STFileObj *fobj;

      // int32_t code = tsdbTFileObjCreate(&fobj);
      // if (code) return code;

      // code = tsdbJsonToTFile(item2, TSDB_FTYPE_STT, &fobj->f);
      // if (code) return code;

      add_file_to_stt_lvl(lvl, fobj);
    }
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  return 0;
}

static int32_t add_stt_lvl(STFileSet *fset, SSttLvl *lvl) {
  // tRBTreePut(&fset->lvlTree, &lvl->rbtn);
  return 0;
}

static int32_t add_file_to_fset(STFileSet *fset, STFileObj *fobj) {
  // if (fobj->f.type == TSDB_FTYPE_STT) {
  //   SSttLvl *lvl;
  //   SSttLvl  tlvl = {.level = fobj->f.stt.level};

  //   SRBTreeNode *node = tRBTreeGet(&fset->lvlTree, &tlvl.rbtn);
  //   if (node) {
  //     lvl = TCONTAINER_OF(node, SSttLvl, rbtn);
  //   } else {
  //     lvl = taosMemoryMalloc(sizeof(*lvl));
  //     if (!lvl) return TSDB_CODE_OUT_OF_MEMORY;

  //     stt_lvl_init(lvl, fobj->f.stt.level);
  //     add_stt_lvl(fset, lvl);
  //   }
  //   add_file_to_stt_lvl(lvl, fobj);
  // } else {
  //   fset->farr[fobj->f.type] = fobj;
  // }

  return 0;
}

static int32_t stt_lvl_cmpr(const SRBTreeNode *n1, const SRBTreeNode *n2) {
  SSttLvl *lvl1 = TCONTAINER_OF(n1, SSttLvl, rbtn);
  SSttLvl *lvl2 = TCONTAINER_OF(n2, SSttLvl, rbtn);

  if (lvl1->level < lvl2->level) {
    return -1;
  } else if (lvl1->level > lvl2->level) {
    return 1;
  }
  return 0;
}

// static int32_t fset_init(STFileSet *fset, int32_t fid) {
//   fset->fid = fid;
//   for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
//     fset->farr[ftype] = NULL;
//   }
//   tRBTreeCreate(&fset->lvlTree, stt_lvl_cmpr);
//   return 0;
// }

static int32_t fset_clear(STFileSet *fset) {
  // TODO
  return 0;
}

int32_t tsdbFileSetToJson(const STFileSet *fset, cJSON *json) {
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
  item1 = cJSON_AddArrayToObject(json, "stt levels");
  if (item1 == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  const SSttLvl *lvl;
  TARRAY2_FOREACH(&fset->lvlArr, lvl) {
    item2 = cJSON_CreateObject();
    if (!item2) return TSDB_CODE_OUT_OF_MEMORY;
    cJSON_AddItemToArray(item1, item2);

    code = stt_lvl_to_json(lvl, item2);
    if (code) return code;
  }

  return 0;
}

int32_t tsdbJsonToFileSet(const cJSON *json, STFileSet **fset) {
  // const cJSON *item1, *item2;
  // int32_t      code;
  // STFile       tf;

  // /* fid */
  // item1 = cJSON_GetObjectItem(json, "fid");
  // if (cJSON_IsNumber(item1)) {
  //   fset->fid = item1->valueint;
  // } else {
  //   return TSDB_CODE_FILE_CORRUPTED;
  // }

  // fset_init(fset, fset->fid);
  // for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
  //   code = tsdbJsonToTFile(json, ftype, &tf);
  //   if (code == TSDB_CODE_NOT_FOUND) {
  //     continue;
  //   } else if (code) {
  //     return code;
  //   } else {
  //     code = tsdbTFileObjCreate(&fset->farr[ftype]);
  //     if (code) return code;
  //     fset->farr[ftype]->f = tf;
  //   }
  // }

  // // each level
  // item1 = cJSON_GetObjectItem(json, "stt");
  // if (cJSON_IsArray(item1)) {
  //   cJSON_ArrayForEach(item2, item1) {
  //     SSttLvl *lvl = taosMemoryCalloc(1, sizeof(*lvl));
  //     if (lvl == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  //     code = json_to_stt_lvl(item2, lvl);
  //     if (code) {
  //       taosMemoryFree(lvl);
  //       return code;
  //     }

  //     add_stt_lvl(fset, lvl);
  //   }
  // } else {
  //   return TSDB_CODE_FILE_CORRUPTED;
  // }

  return 0;
}

int32_t tsdbFSetCmprFn(const STFileSet *pSet1, const STFileSet *pSet2) {
  if (pSet1->fid < pSet2->fid) return -1;
  if (pSet1->fid > pSet2->fid) return 1;
  return 0;
}

int32_t tsdbFileSetEdit(STFileSet *fset, const STFileOp *op) {
  int32_t code = 0;

  if (op->oState.size == 0  //
      || 0                  /* TODO*/
  ) {
    STFileObj *fobj;
    // code = tsdbTFileObjCreate(&fobj);
    if (code) return code;
    fobj->f = op->nState;
    add_file_to_fset(fset, fobj);
  } else if (op->nState.size == 0) {
    // delete
    ASSERT(0);
  } else {
    // modify
    ASSERT(0);
  }
  return 0;
}

int32_t tsdbFileSetInit(int32_t fid, STFileSet **fset) {
  fset[0] = taosMemoryCalloc(1, sizeof(STFileSet));
  if (fset[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  fset[0]->fid = fid;
  TARRAY2_INIT(&fset[0]->lvlArr);
  return 0;
}

int32_t tsdbFileSetInitEx(const STFileSet *fset1, STFileSet **fset) {
  int32_t code = tsdbFileSetInit(fset1->fid, fset);
  if (code) return code;

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset1->farr[ftype] == NULL) continue;

    code = tsdbTFileObjCreate(&fset1->farr[ftype]->f, &fset[0]->farr[ftype]);
    if (code) {
      tsdbFileSetClear(fset);
      return code;
    }
  }

  const SSttLvl *lvl1;
  TARRAY2_FOREACH(&fset1->lvlArr, lvl1) {
    SSttLvl *lvl;
    // code = stt_lvl_init_ex(lvl1, &lvl);
    if (code) {
      tsdbFileSetClear(fset);
      return code;
    }
  }

  // SRBTreeIter iter = tRBTreeIterCreate(&fset1->lvlTree, 1);
  // for (SRBTreeNode *node = tRBTreeIterNext(&iter); node; node = tRBTreeIterNext(&iter)) {
  //   SSttLvl *lvl1 = TCONTAINER_OF(node, SSttLvl, rbtn);
  //   SSttLvl *lvl2 = taosMemoryCalloc(1, sizeof(*lvl2));
  //   if (lvl2 == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  //   stt_lvl_init(lvl2, lvl1->level);
  //   add_stt_lvl(fset2, lvl2);

  //   SRBTreeIter iter2 = tRBTreeIterCreate(&lvl1->sttTree, 1);
  //   for (SRBTreeNode *node2 = tRBTreeIterNext(&iter2); node2; node2 = tRBTreeIterNext(&iter2)) {
  //     STFileObj *fobj1 = TCONTAINER_OF(node2, STFileObj, rbtn);
  //     STFileObj *fobj2;
  //     code = tsdbTFileObjCreate(&fobj2);
  //     if (code) return code;
  //     fobj2->f = fobj1->f;
  //     add_file_to_stt_lvl(lvl2, fobj2);
  //   }
  // }
  return 0;
}

int32_t tsdbFileSetClear(STFileSet **fset) {
  if (fset[0]) {
    for (tsdb_ftype_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
      // if (fset[0]->farr[ftype]) {
      //   tsdbTFileObjDestroy(&fset[0]->farr[ftype]);
      //   fset[0]->farr[ftype] = NULL;
      // }
    }

    // TODO
    // SSttLvl *lvl;
    // TARRAY2_FOREACH(&fset[0]->lvlArr, lvl) {
    //   // stt_lvl_clear(&lvl);
    // }

    taosMemoryFree(fset[0]);
    fset[0] = NULL;
  }
  return 0;
}

const SSttLvl *tsdbFileSetGetLvl(const STFileSet *fset, int32_t level) {
  // SSttLvl      tlvl = {.level = level};
  // SRBTreeNode *node = tRBTreeGet(&fset->lvlTree, &tlvl.rbtn);
  // return node ? TCONTAINER_OF(node, SSttLvl, rbtn) : NULL;
  // TODO
  return NULL;
}