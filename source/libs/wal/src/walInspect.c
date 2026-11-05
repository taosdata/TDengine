/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "walInt.h"
#include "cJSON.h"
#include "tencrypt.h"
#include "tjson.h"
#include "tmsg.h"

enum { WAL_INSPECT_LOOKBACK = 20 };

static int32_t walInspectInteger(const cJSON *json, const char *key,
                                  int64_t *value) {
  const char *str = cJSON_GetStringValue(cJSON_GetObjectItem(json, key));
  if (str == NULL || str[0] == '\0') return TSDB_CODE_INVALID_JSON_FORMAT;
  const char *p = str;
  if (*p == '-') ++p;
  if (*p == '\0') return TSDB_CODE_INVALID_JSON_FORMAT;
  for (; *p != '\0'; ++p) {
    if (*p < '0' || *p > '9') return TSDB_CODE_INVALID_JSON_FORMAT;
  }
  return taosStr2int64(str, value);
}

// A WAL that was deleted is a normal state: the directory is gone, or it is
// empty. The caller then treats it as "no logs left". A directory that still
// holds segment files but lost its meta file is not covered by that, because
// the retained segments can no longer be verified.
static int32_t walInspectMetaName(const char *dir, char *path, int32_t size, bool *absent) {
  *absent = false;
  if (!taosIsDir(dir)) {
    *absent = true;
    return 0;
  }
  TdDirPtr handle = taosOpenDir(dir);
  if (handle == NULL) return terrno;
  int64_t latest = -1;
  bool    hasSegment = false;
  int32_t code = 0;
  TdDirEntryPtr entry;
  // A failed read is reported as NULL with terrno set, which looks exactly like
  // the end of the directory. Mistaking one for the other would report an
  // existing WAL as absent and let the caller fall back to the snapshot.
  terrno = 0;
  while ((entry = taosReadDir(handle)) != NULL) {
    const char *name = taosGetDirEntryName(entry);
    const char *dot = strrchr(name, '.');
    if (dot != NULL && (strcmp(dot, ".log") == 0 || strcmp(dot, ".idx") == 0)) {
      hasSegment = true;
      continue;
    }
    if (strcmp(name, "meta-ver.tmp") == 0) {
      code = TSDB_CODE_WAL_FILE_CORRUPTED;
      break;
    }
    if (strncmp(name, "meta-ver", 8) != 0) continue;
    bool valid = name[8] != '\0';
    for (const char *p = name + 8; *p != '\0'; ++p) {
      if (*p < '0' || *p > '9') valid = false;
    }
    int64_t version = -1;
    if (!valid || taosStr2int64(name + 8, &version) != 0 || version < 0) {
      code = TSDB_CODE_WAL_FILE_CORRUPTED;
      break;
    }
    if (version > latest) {
      latest = version;
      int32_t n = snprintf(path, size, "%s%s%s", dir, TD_DIRSEP, name);
      if (n < 0 || n >= size) code = TSDB_CODE_OUT_OF_BUFFER;
    }
  }
  if (code == 0 && terrno != 0) code = terrno;
  int32_t closeCode = taosCloseDir(&handle);
  if (code == 0) code = closeCode;
  if (code != 0 || latest >= 0) return code;
  if (hasSegment) return TSDB_CODE_WAL_FILE_CORRUPTED;
  *absent = true;
  return 0;
}

static int32_t walInspectMeta(const cJSON *json, SWalVer *ver, SArray *files) {
  const cJSON *meta = cJSON_GetObjectItem(json, "meta");
  TAOS_CHECK_RETURN(walInspectInteger(meta, "firstVer", &ver->firstVer));
  TAOS_CHECK_RETURN(walInspectInteger(meta, "snapshotVer", &ver->snapshotVer));
  TAOS_CHECK_RETURN(walInspectInteger(meta, "commitVer", &ver->commitVer));
  TAOS_CHECK_RETURN(walInspectInteger(meta, "lastVer", &ver->lastVer));
  const cJSON *array = cJSON_GetObjectItem(json, "files");
  if (!cJSON_IsArray(array)) return TSDB_CODE_INVALID_JSON_FORMAT;
  for (int32_t i = 0; i < cJSON_GetArraySize(array); ++i) {
    const cJSON *item = cJSON_GetArrayItem(array, i);
    SWalFileInfo info = {.diskId = {.level = 0, .id = -1}};
    TAOS_CHECK_RETURN(walInspectInteger(item, "firstVer", &info.firstVer));
    TAOS_CHECK_RETURN(walInspectInteger(item, "lastVer", &info.lastVer));
    TAOS_CHECK_RETURN(walInspectInteger(item, "fileSize", &info.fileSize));
    // A newly rolled, still-empty segment retains the -1 lastVer sentinel.
    if (info.lastVer == -1 && info.firstVer >= 0 && info.fileSize == 0) {
      info.lastVer = info.firstVer - 1;
    }
    const cJSON *disk = cJSON_GetObjectItem(item, "diskId");
    const cJSON *level = cJSON_GetObjectItem(item, "diskLevel");
    if (disk != NULL) {
      if (!cJSON_IsNumber(disk) || disk->valuedouble != disk->valueint ||
          disk->valueint < -1) return TSDB_CODE_INVALID_JSON_FORMAT;
      info.diskId.id = disk->valueint;
    }
    if (level != NULL &&
        (!cJSON_IsNumber(level) || level->valuedouble != 0)) {
      return TSDB_CODE_INVALID_JSON_FORMAT;
    }
    if (info.firstVer < 0 || info.lastVer < info.firstVer - 1 ||
        info.fileSize < 0) return TSDB_CODE_WAL_FILE_CORRUPTED;
    if (taosArrayPush(files, &info) == NULL) return terrno;
  }
  return 0;
}

static const char *walInspectDisk(const SWalInspectOption *opt, int32_t id) {
  if (id == -1) return opt->path;
  if (id < 0 || id >= opt->numOfDisks) return NULL;
  return opt->diskPaths[id];
}

static int32_t walInspectReplay(const SWalInspectOption *opt,
                                const SWalCont *record, bool *safe) {
  *safe = false;
  if (WAL_IS_TXN_MSG(record)) return 0;
  bool allowed = record->msgType == TDMT_SYNC_NOOP ||
      (opt->cfg.vgId > 1 && opt->allowAlterConfirm &&
       record->msgType == TDMT_VND_ALTER_CONFIRM);
  if (!allowed) {
    if (opt->checkReplay != NULL) {
      return opt->checkReplay(opt->cfg.vgId, record->msgType, record->body,
                               record->bodyLen, safe);
    }
    return 0;
  }
  if (record->bodyLen != sizeof(SMsgHead)) return TSDB_CODE_WAL_FILE_CORRUPTED;
  // Both local noop construction and vnode queue decoding use host byte order.
  SMsgHead header = {0};
  memcpy(&header, record->body, sizeof(header));
  if (header.vgId != opt->cfg.vgId ||
      header.contLen != sizeof(SMsgHead)) {
    return TSDB_CODE_WAL_FILE_CORRUPTED;
  }
  *safe = true;
  return 0;
}

static int32_t walInspectRecord(const SWalInspectOption *opt, TdFilePtr log,
                                const SWalIdxEntry *idx, int64_t limit,
                                int64_t *end, bool *safe) {
  if (idx->offset < 0 || idx->offset > limit - (int64_t)sizeof(SWalCkHead)) {
    return TSDB_CODE_WAL_FILE_CORRUPTED;
  }
  if (taosLSeekFile(log, idx->offset, SEEK_SET) != idx->offset) return terrno;
  SWalCkHead head = {0};
  if (taosReadFile(log, &head, sizeof(head)) != sizeof(head)) {
    return TSDB_CODE_WAL_FILE_CORRUPTED;
  }
  if (head.magic != WAL_MAGIC || walValidHeadCksum(&head) != 0 ||
      head.head.version != idx->ver || !walValidBodyLen(head.head.bodyLen) ||
      WAL_GET_PROTO_VER(&head.head) > WAL_PROTO_VER_SUPPORTED ||
      (WAL_IS_TXN_MSG(&head.head) && head.head.bodyLen < WAL_TXN_HDR_SIZE)) {
    return TSDB_CODE_WAL_FILE_CORRUPTED;
  }
  int32_t len = head.head.bodyLen;
  if (opt->cfg.encryptData.encryptAlgrName[0] != '\0') len = ENCRYPTED_LEN(len);
  if (len > limit - idx->offset - (int64_t)sizeof(head)) {
    return TSDB_CODE_WAL_FILE_CORRUPTED;
  }
  *end = idx->offset + sizeof(head) + len;
  if (idx->ver == opt->snapshotIndex &&
      head.head.syncMeta.term != opt->snapshotTerm) {
    return TSDB_CODE_WAL_FILE_CORRUPTED;
  }
  SWalCkHead *record = taosMemoryMalloc(sizeof(head) + len);
  if (record == NULL) return terrno;
  memcpy(record, &head, sizeof(head));
  int32_t code = 0;
  if (taosReadFile(log, record->head.body, len) != len) {
    code = TSDB_CODE_WAL_FILE_CORRUPTED;
  } else {
    SWalCfg cfg = opt->cfg;
    code = decryptBody(&cfg, record, head.head.bodyLen, __func__);
    if (code == 0 && walValidBodyCksum(record) != 0) {
      code = TSDB_CODE_WAL_FILE_CORRUPTED;
    }
    if (code == 0 && safe != NULL) {
      code = walInspectReplay(opt, &record->head, safe);
    }
  }
  taosMemoryFree(record);
  return code;
}

static int32_t walInspectTail(const SWalInspectOption *opt,
                              const SWalFileInfo *info,
                              TdFilePtr log, TdFilePtr idx) {
  if (info->lastVer <= opt->snapshotIndex ||
      info->lastVer < info->firstVer) return 0;
  // Include S when retained, to verify its term and the next record's offset.
  int64_t first = TMAX(opt->snapshotIndex, info->firstVer);
  int64_t previousEnd = -1;
  for (int64_t version = first; version <= info->lastVer; ++version) {
    SWalIdxEntry entry = {0};
    TAOS_CHECK_RETURN(walReadIdxEntry(idx, info->firstVer, version, &entry));
    if ((previousEnd >= 0 && entry.offset != previousEnd) ||
        (version == info->firstVer && entry.offset != 0)) {
      return TSDB_CODE_WAL_FILE_CORRUPTED;
    }
    bool safe = false;
    TAOS_CHECK_RETURN(walInspectRecord(opt, log, &entry, info->fileSize,
                                       &previousEnd,
                                       version > opt->snapshotIndex ? &safe : NULL));
    if (version > opt->snapshotIndex && !safe) {
      wError("vgId:%d, unsafe WAL tail record, version:%" PRId64,
              opt->cfg.vgId, version);
      return TSDB_CODE_INVALID_CFG;
    }
  }
  return previousEnd == info->fileSize ? 0 : TSDB_CODE_WAL_FILE_CORRUPTED;
}

static int32_t walInspectSegment(const SWalInspectOption *opt,
                                  const SWalFileInfo *info) {
  const char *dir = walInspectDisk(opt, info->diskId.id);
  if (dir == NULL) return TSDB_CODE_INVALID_CFG;
  char path[WAL_FILE_LEN] = {0};
  walBuildLogNameAt(dir, info->firstVer, path);
  TdFilePtr log = taosOpenFile(path, TD_FILE_READ | TD_FILE_CLOEXEC);
  if (log == NULL) return terrno;
  walBuildIdxNameAt(dir, info->firstVer, path);
  TdFilePtr idx = taosOpenFile(path, TD_FILE_READ | TD_FILE_CLOEXEC);
  int32_t code = idx == NULL ? terrno : 0;
  int64_t logSize = 0, idxSize = 0;
  if (code == 0) code = taosFStatFile(log, &logSize, NULL);
  if (code == 0) code = taosFStatFile(idx, &idxSize, NULL);
  int64_t count = info->lastVer - info->firstVer + 1;
  if (code == 0 &&
      (count > INT64_MAX / sizeof(SWalIdxEntry) ||
       logSize != info->fileSize || idxSize != count * sizeof(SWalIdxEntry) ||
       (count == 0 && logSize != 0))) {
    code = TSDB_CODE_WAL_FILE_CORRUPTED;
  }
  for (int32_t i = 0; code == 0 && count > 0 && i < 2; ++i) {
    int64_t      slot = i == 0 ? 0 : count - 1;
    SWalIdxEntry entry = {0};
    code = walReadIdxEntry(idx, info->firstVer, info->firstVer + slot, &entry);
    if (code != 0) break;
    if (i == 0 && entry.offset != 0) {
      code = TSDB_CODE_WAL_FILE_CORRUPTED;
      break;
    }
    int64_t end = 0;
    code = walInspectRecord(opt, log, &entry, logSize, &end, NULL);
    if (code == 0 && i == 1 && end != logSize) {
      code = TSDB_CODE_WAL_FILE_CORRUPTED;
    }
  }
  if (code == 0) code = walInspectTail(opt, info, log, idx);
  int32_t closeCode = taosCloseFile(&log);
  if (code == 0) code = closeCode;
  if (idx != NULL) {
    closeCode = taosCloseFile(&idx);
    if (code == 0) code = closeCode;
  }
  return code;
}

static int32_t walInspectPrevious(const SWalInspectOption *opt,
                                   const SWalFileInfo *info, int64_t version,
                                   int64_t *nextOffset, bool *safe) {
  const char *dir = walInspectDisk(opt, info->diskId.id);
  if (dir == NULL) return TSDB_CODE_INVALID_CFG;
  char path[WAL_FILE_LEN];
  walBuildLogNameAt(dir, info->firstVer, path);
  TdFilePtr log = taosOpenFile(path, TD_FILE_READ | TD_FILE_CLOEXEC);
  if (log == NULL) return terrno;
  walBuildIdxNameAt(dir, info->firstVer, path);
  TdFilePtr idx = taosOpenFile(path, TD_FILE_READ | TD_FILE_CLOEXEC);
  int32_t code = idx == NULL ? terrno : 0;
  SWalIdxEntry entry = {0};
  if (code == 0) {
    code = walReadIdxEntry(idx, info->firstVer, version, &entry);
  }
  int64_t end = 0;
  if (code == 0) {
    code = walInspectRecord(opt, log, &entry, info->fileSize, &end, safe);
  }
  if (code == 0 && ((*nextOffset >= 0 && end != *nextOffset) ||
                    (version == info->firstVer && entry.offset != 0))) {
    code = TSDB_CODE_WAL_FILE_CORRUPTED;
  }
  if (code == 0) *nextOffset = entry.offset;
  int32_t closeCode = taosCloseFile(&log);
  if (code == 0) code = closeCode;
  if (idx != NULL) {
    closeCode = taosCloseFile(&idx);
    if (code == 0) code = closeCode;
  }
  return code;
}

static int32_t walInspectLookback(const SWalInspectOption *opt,
                                   const SArray *files, int64_t *bound) {
  *bound = opt->snapshotIndex;
  const SWalFileInfo *previous = NULL;
  int64_t nextOffset = -1;
  for (int32_t count = 0; count < WAL_INSPECT_LOOKBACK && *bound >= 0; ++count) {
    const SWalFileInfo *found = NULL;
    for (size_t i = 0; i < taosArrayGetSize(files); ++i) {
      const SWalFileInfo *info = taosArrayGet(files, i);
      if (*bound >= info->firstVer && *bound <= info->lastVer) {
        found = info;
        break;
      }
    }
    if (found == NULL) break;  // Already covered by the snapshot and trimmed.
    if (previous != found) {
      nextOffset = *bound == found->lastVer ? found->fileSize : -1;
    }
    bool safe = false;
    TAOS_CHECK_RETURN(walInspectPrevious(opt, found, *bound, &nextOffset, &safe));
    if (!safe) break;
    previous = found;
    --*bound;
  }
  return 0;
}

static int32_t walInspectDirectory(const SWalInspectOption *opt,
                                    const char *dir, const SArray *files) {
  TdDirPtr handle = taosOpenDir(dir);
  if (handle == NULL) return terrno;
  int32_t code = 0;
  TdDirEntryPtr entry;
  // A partial scan would leave the files it did not reach unreported, so a
  // failed read must not pass as a complete one.
  terrno = 0;
  while ((entry = taosReadDir(handle)) != NULL) {
    const char *name = taosGetDirEntryName(entry);
    const char *dot = strrchr(name, '.');
    if (dot == NULL ||
        (strcmp(dot, ".log") != 0 && strcmp(dot, ".idx") != 0)) continue;
    bool found = false;
    for (size_t i = 0; i < taosArrayGetSize(files); ++i) {
      const SWalFileInfo *info = taosArrayGet(files, i);
      char expected[32];
      snprintf(expected, sizeof(expected), "%020" PRId64 "%s",
               info->firstVer, dot);
      const char *expectedDir = walInspectDisk(opt, info->diskId.id);
      if (expectedDir != NULL && strcmp(dir, expectedDir) == 0 &&
          strcmp(name, expected) == 0) {
        found = true;
        break;
      }
    }
    if (!found) {
      wError("vgId:%d, unexpected WAL file %s%s%s is not listed in the metadata",
             opt->cfg.vgId, dir, TD_DIRSEP, name);
      code = TSDB_CODE_WAL_FILE_CORRUPTED;
      break;
    }
  }
  if (code == 0 && terrno != 0) code = terrno;
  int32_t closeCode = taosCloseDir(&handle);
  return code == 0 ? closeCode : code;
}

int32_t walInspect(const SWalInspectOption *opt, SWalInspectResult *result) {
  // Every level-0 disk must name a directory: the scan compares against them and
  // walks into them, and a missing entry means the WAL cannot be covered at all.
  if (opt->numOfDisks > 0 && opt->diskPaths == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  for (int32_t i = 0; i < opt->numOfDisks; ++i) {
    if (opt->diskPaths[i] == NULL) return TSDB_CODE_INVALID_PARA;
  }
  SWalVer *ver = &result->versions;
  result->safeLowerBound = opt->snapshotIndex;
  char path[WAL_FILE_LEN] = {0};
  char *content = NULL;
  int32_t len = 0;
  cJSON *json = NULL;
  SArray *files = taosArrayInit(1, sizeof(SWalFileInfo));
  if (files == NULL) return terrno;
  bool    absent = false;
  int32_t code = walInspectMetaName(opt->path, path, sizeof(path), &absent);
  if (code != 0) goto _exit;
  if (absent) {
    // Without a WAL the last durably persisted position is all there is, so the
    // commit and last positions equal the snapshot position, and the record just
    // below it is the last one that was not verified.
    ver->snapshotVer = opt->snapshotIndex;
    ver->commitVer = opt->snapshotIndex;
    ver->lastVer = opt->snapshotIndex;
    ver->firstVer = opt->snapshotIndex + 1;
    result->safeLowerBound = opt->snapshotIndex > 0 ? opt->snapshotIndex - 1
                                                    : -1;
    code = 0;
    goto _exit;
  }
  code = taosReadCfgFile(path, &content, &len);
  if (code != 0) goto _exit;
  json = cJSON_ParseWithOpts(content, NULL, true);
  if (json == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }
  code = walInspectMeta(json, ver, files);
  if (code != 0) goto _exit;
  if (opt->snapshotIndex < -1 || opt->snapshotTerm < -1 ||
      (opt->snapshotIndex >= 0 && opt->snapshotTerm < 0) ||
      ver->snapshotVer < -1 || ver->snapshotVer > opt->snapshotIndex ||
      ver->commitVer < opt->snapshotIndex ||
      ver->lastVer < ver->commitVer ||
      ver->lastVer == INT64_MAX || ver->firstVer < -1 ||
      ver->firstVer > ver->lastVer + 1 ||
      (ver->lastVer > opt->snapshotIndex &&
       (ver->firstVer < 0 || ver->firstVer > opt->snapshotIndex + 1))) {
    code = TSDB_CODE_INVALID_CFG;
    goto _exit;
  }
  for (size_t i = 0; i < taosArrayGetSize(files); ++i) {
    const SWalFileInfo *info = taosArrayGet(files, i);
    if (info->lastVer > ver->lastVer ||
        (i == 0 && info->firstVer != ver->firstVer) ||
        (i > 0 && info->firstVer !=
         ((SWalFileInfo *)taosArrayGet(files, i - 1))->lastVer + 1)) {
      code = TSDB_CODE_WAL_FILE_CORRUPTED;
      goto _exit;
    }
    code = walInspectSegment(opt, info);
    if (code != 0) goto _exit;
  }
  if (taosArrayGetSize(files) > 0) {
    const SWalFileInfo *last = taosArrayGetLast(files);
    if (last->lastVer != ver->lastVer) code = TSDB_CODE_WAL_FILE_CORRUPTED;
  } else if (!(ver->firstVer == ver->lastVer + 1 ||
               (ver->firstVer == -1 && ver->lastVer == -1))) {
    code = TSDB_CODE_WAL_FILE_CORRUPTED;
  }
  if (code != 0) goto _exit;
  code = walInspectDirectory(opt, opt->path, files);
  for (int32_t i = 0; code == 0 && i < opt->numOfDisks; ++i) {
    const char *dir = opt->diskPaths[i];
    if (strcmp(dir, opt->path) != 0 && taosCheckExistFile(dir)) {
      code = walInspectDirectory(opt, dir, files);
    }
  }
  if (code == 0) code = walInspectLookback(opt, files, &result->safeLowerBound);
_exit:
  if (code != 0) {
    wError("vgId:%d, failed to inspect WAL:%s since %s", opt->cfg.vgId,
           opt->path, tstrerror(code));
  }
  tjsonDelete(json);
  taosMemoryFree(content);
  taosArrayDestroy(files);
  return code;
}
