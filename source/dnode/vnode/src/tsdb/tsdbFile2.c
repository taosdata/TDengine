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
#include "cos.h"

// to_json
static int32_t head_to_json(const STFile *file, cJSON *json);
static int32_t data_to_json(const STFile *file, cJSON *json);
static int32_t sma_to_json(const STFile *file, cJSON *json);
static int32_t tomb_to_json(const STFile *file, cJSON *json);
static int32_t stt_to_json(const STFile *file, cJSON *json);

// from_json
static int32_t head_from_json(const cJSON *json, STFile *file);
static int32_t data_from_json(const cJSON *json, STFile *file);
static int32_t sma_from_json(const cJSON *json, STFile *file);
static int32_t tomb_from_json(const cJSON *json, STFile *file);
static int32_t stt_from_json(const cJSON *json, STFile *file);

static const struct {
  const char *suffix;
  int32_t (*to_json)(const STFile *file, cJSON *json);
  int32_t (*from_json)(const cJSON *json, STFile *file);
} g_tfile_info[] = {
    [TSDB_FTYPE_HEAD] = {"head", head_to_json, head_from_json},
    [TSDB_FTYPE_DATA] = {"data", data_to_json, data_from_json},
    [TSDB_FTYPE_SMA] = {"sma", sma_to_json, sma_from_json},
    [TSDB_FTYPE_TOMB] = {"tomb", tomb_to_json, tomb_from_json},
    [TSDB_FTYPE_STT] = {"stt", stt_to_json, stt_from_json},
};

void remove_file(const char *fname, bool last_level) {
  int32_t code = taosRemoveFile(fname);
  if (code) {
    if (tsS3Enabled && last_level) {
      const char *object_name = taosDirEntryBaseName((char *)fname);
      long        s3_size = tsS3Enabled ? s3Size(object_name) : 0;
      if (!strncmp(fname + strlen(fname) - 5, ".data", 5) && s3_size > 0) {
        s3DeleteObjects(&object_name, 1);
        tsdbInfo("file:%s is removed from s3", fname);
      } else {
        tsdbError("file:%s remove failed", fname);
      }
    } else {
      tsdbError("file:%s remove failed", fname);
    }
  } else {
    tsdbInfo("file:%s is removed", fname);
  }
}

static int32_t tfile_to_json(const STFile *file, cJSON *json) {
  /* did.level */
  if (cJSON_AddNumberToObject(json, "did.level", file->did.level) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  /* did.id */
  if (cJSON_AddNumberToObject(json, "did.id", file->did.id) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  /* fid */
  if (cJSON_AddNumberToObject(json, "fid", file->fid) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  /* cid */
  if (cJSON_AddNumberToObject(json, "cid", file->cid) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  /* size */
  if (cJSON_AddNumberToObject(json, "size", file->size) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (file->minVer <= file->maxVer) {
    /* minVer */
    if (cJSON_AddNumberToObject(json, "minVer", file->minVer) == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    /* maxVer */
    if (cJSON_AddNumberToObject(json, "maxVer", file->maxVer) == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return 0;
}

static int32_t tfile_from_json(const cJSON *json, STFile *file) {
  const cJSON *item;

  /* did.level */
  item = cJSON_GetObjectItem(json, "did.level");
  if (cJSON_IsNumber(item)) {
    file->did.level = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  /* did.id */
  item = cJSON_GetObjectItem(json, "did.id");
  if (cJSON_IsNumber(item)) {
    file->did.id = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  /* fid */
  item = cJSON_GetObjectItem(json, "fid");
  if (cJSON_IsNumber(item)) {
    file->fid = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  /* cid */
  item = cJSON_GetObjectItem(json, "cid");
  if (cJSON_IsNumber(item)) {
    file->cid = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  /* size */
  item = cJSON_GetObjectItem(json, "size");
  if (cJSON_IsNumber(item)) {
    file->size = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  /* minVer */
  file->minVer = VERSION_MAX;
  item = cJSON_GetObjectItem(json, "minVer");
  if (cJSON_IsNumber(item)) {
    file->minVer = item->valuedouble;
  }

  /* maxVer */
  file->maxVer = VERSION_MIN;
  item = cJSON_GetObjectItem(json, "maxVer");
  if (cJSON_IsNumber(item)) {
    file->maxVer = item->valuedouble;
  }
  return 0;
}

static int32_t head_to_json(const STFile *file, cJSON *json) { return tfile_to_json(file, json); }
static int32_t data_to_json(const STFile *file, cJSON *json) { return tfile_to_json(file, json); }
static int32_t sma_to_json(const STFile *file, cJSON *json) { return tfile_to_json(file, json); }
static int32_t tomb_to_json(const STFile *file, cJSON *json) { return tfile_to_json(file, json); }
static int32_t stt_to_json(const STFile *file, cJSON *json) {
  int32_t code = tfile_to_json(file, json);
  if (code) return code;

  /* lvl */
  if (cJSON_AddNumberToObject(json, "level", file->stt->level) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return 0;
}

static int32_t head_from_json(const cJSON *json, STFile *file) { return tfile_from_json(json, file); }
static int32_t data_from_json(const cJSON *json, STFile *file) { return tfile_from_json(json, file); }
static int32_t sma_from_json(const cJSON *json, STFile *file) { return tfile_from_json(json, file); }
static int32_t tomb_from_json(const cJSON *json, STFile *file) { return tfile_from_json(json, file); }
static int32_t stt_from_json(const cJSON *json, STFile *file) {
  int32_t code = tfile_from_json(json, file);
  if (code) return code;

  const cJSON *item;

  /* lvl */
  item = cJSON_GetObjectItem(json, "level");
  if (cJSON_IsNumber(item)) {
    file->stt->level = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  return 0;
}

int32_t tsdbTFileToJson(const STFile *file, cJSON *json) {
  if (file->type == TSDB_FTYPE_STT) {
    return g_tfile_info[file->type].to_json(file, json);
  } else {
    cJSON *item = cJSON_AddObjectToObject(json, g_tfile_info[file->type].suffix);
    if (item == NULL) return TSDB_CODE_OUT_OF_MEMORY;
    return g_tfile_info[file->type].to_json(file, item);
  }
}

int32_t tsdbJsonToTFile(const cJSON *json, tsdb_ftype_t ftype, STFile *f) {
  f[0] = (STFile){.type = ftype};

  if (ftype == TSDB_FTYPE_STT) {
    int32_t code = g_tfile_info[ftype].from_json(json, f);
    if (code) return code;
  } else {
    const cJSON *item = cJSON_GetObjectItem(json, g_tfile_info[ftype].suffix);
    if (cJSON_IsObject(item)) {
      int32_t code = g_tfile_info[ftype].from_json(item, f);
      if (code) return code;
    } else {
      return TSDB_CODE_NOT_FOUND;
    }
  }

  return 0;
}

int32_t tsdbTFileObjInit(STsdb *pTsdb, const STFile *f, STFileObj **fobj) {
  fobj[0] = taosMemoryMalloc(sizeof(*fobj[0]));
  if (!fobj[0]) return TSDB_CODE_OUT_OF_MEMORY;

  taosThreadMutexInit(&fobj[0]->mutex, NULL);
  fobj[0]->f[0] = f[0];
  fobj[0]->state = TSDB_FSTATE_LIVE;
  fobj[0]->ref = 1;
  tsdbTFileName(pTsdb, f, fobj[0]->fname);
  fobj[0]->nlevel = tfsGetLevel(pTsdb->pVnode->pTfs);
  return 0;
}

int32_t tsdbTFileObjRef(STFileObj *fobj) {
  int32_t nRef;
  taosThreadMutexLock(&fobj->mutex);
  ASSERT(fobj->ref > 0 && fobj->state == TSDB_FSTATE_LIVE);
  nRef = ++fobj->ref;
  taosThreadMutexUnlock(&fobj->mutex);
  tsdbTrace("ref file %s, fobj:%p ref %d", fobj->fname, fobj, nRef);
  return 0;
}

int32_t tsdbTFileObjUnref(STFileObj *fobj) {
  taosThreadMutexLock(&fobj->mutex);
  int32_t nRef = --fobj->ref;
  taosThreadMutexUnlock(&fobj->mutex);
  ASSERT(nRef >= 0);
  tsdbTrace("unref file %s, fobj:%p ref %d", fobj->fname, fobj, nRef);
  if (nRef == 0) {
    if (fobj->state == TSDB_FSTATE_DEAD) {
      remove_file(fobj->fname, fobj->nlevel > 1 && fobj->f->did.level == fobj->nlevel - 1);
    }
    taosMemoryFree(fobj);
  }

  return 0;
}

int32_t tsdbTFileObjRemove(STFileObj *fobj) {
  taosThreadMutexLock(&fobj->mutex);
  ASSERT(fobj->state == TSDB_FSTATE_LIVE && fobj->ref > 0);
  fobj->state = TSDB_FSTATE_DEAD;
  int32_t nRef = --fobj->ref;
  taosThreadMutexUnlock(&fobj->mutex);
  tsdbTrace("remove unref file %s, fobj:%p ref %d", fobj->fname, fobj, nRef);
  if (nRef == 0) {
    remove_file(fobj->fname, fobj->nlevel > 1 && fobj->f->did.level == fobj->nlevel - 1);
    taosMemoryFree(fobj);
  }
  return 0;
}

int32_t tsdbTFileName(STsdb *pTsdb, const STFile *f, char fname[]) {
  SVnode *pVnode = pTsdb->pVnode;
  STfs   *pTfs = pVnode->pTfs;

  if (pTfs) {
    snprintf(fname,                              //
             TSDB_FILENAME_LEN,                  //
             "%s%s%s%sv%df%dver%" PRId64 ".%s",  //
             tfsGetDiskPath(pTfs, f->did),       //
             TD_DIRSEP,                          //
             pTsdb->path,                        //
             TD_DIRSEP,                          //
             TD_VID(pVnode),                     //
             f->fid,                             //
             f->cid,                             //
             g_tfile_info[f->type].suffix);
  } else {
    snprintf(fname,                          //
             TSDB_FILENAME_LEN,              //
             "%s%sv%df%dver%" PRId64 ".%s",  //
             pTsdb->path,                    //
             TD_DIRSEP,                      //
             TD_VID(pVnode),                 //
             f->fid,                         //
             f->cid,                         //
             g_tfile_info[f->type].suffix);
  }
  return 0;
}

bool tsdbIsSameTFile(const STFile *f1, const STFile *f2) {
  if (f1->type != f2->type) return false;
  if (f1->did.level != f2->did.level) return false;
  if (f1->did.id != f2->did.id) return false;
  if (f1->fid != f2->fid) return false;
  if (f1->cid != f2->cid) return false;
  if (f1->s3flag != f2->s3flag) return false;
  return true;
}

bool tsdbIsTFileChanged(const STFile *f1, const STFile *f2) {
  if (f1->size != f2->size) return true;
  // if (f1->type == TSDB_FTYPE_STT && f1->stt->nseg != f2->stt->nseg) return true;
  return false;
}

int32_t tsdbTFileObjCmpr(const STFileObj **fobj1, const STFileObj **fobj2) {
  if (fobj1[0]->f->cid < fobj2[0]->f->cid) {
    return -1;
  } else if (fobj1[0]->f->cid > fobj2[0]->f->cid) {
    return 1;
  } else {
    return 0;
  }
}
