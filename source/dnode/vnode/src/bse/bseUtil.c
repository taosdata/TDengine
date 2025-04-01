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
#include "bseUtil.h"

#define BSE_FILE_FULL_LEN TSDB_FILENAME_LEN

void bseBuildDataFullName(SBse *pBse, int64_t ver, char *buf) {
  // build data file name
  // snprintf(name, strlen(name), "%s/%s020"."BSE_DATA_SUFFIX, ver, pBse->path);
  // sprintf(name, strlen(name), "%s/%020"."BSE_DATA_SUFFIX, ver, pBse->path);
  TAOS_UNUSED(sprintf(buf, "%s/%" PRId64 ".%s", pBse->path, ver, BSE_DATA_SUFFIX));
}

void bseBuildIndexFullName(SBse *pBse, int64_t ver, char *buf) {
  // build index file name
  TAOS_UNUSED(sprintf(buf, "%s/%020" PRId64 "." BSE_INDEX_SUFFIX, pBse->path, ver));
}
void bseBuildLogFullName(SBse *pBse, int64_t ver, char *buf) {
  TAOS_UNUSED(sprintf(buf, "%s/%020" PRId64 "." BSE_LOG_SUFFIX, pBse->path, ver));
}
void bseBuildCurrentMetaName(SBse *pBse, char *name) {
  snprintf(name, BSE_FILE_FULL_LEN, "%s%sbse-current.meta", pBse->path, TD_DIRSEP);
}

void bseBuildTempCurrentMetaName(SBse *pBse, char *name) {
  snprintf(name, BSE_FILE_FULL_LEN, "%s%sbse-current.meta.temp", pBse->path, TD_DIRSEP);
}

void bseBuildMetaName(SBse *pBse, int ver, char *name) {
  snprintf(name, BSE_FILE_FULL_LEN, "%s%sbse-ver%d", pBse->path, TD_DIRSEP, ver);
}
void bseBuildTempMetaName(SBse *pBse, char *name) {
  snprintf(name, BSE_FILE_FULL_LEN, "%s%sbse-ver.tmp", pBse->path, TD_DIRSEP);
}
void bseBuildFullName(SBse *pBse, char *name, char *fullname) {
  snprintf(name, BSE_FILE_FULL_LEN, "%s%s%s", pBse->path, TD_DIRSEP, fullname);
}

void bseBuildDataName(SBse *pBse, int64_t seq, char *name) {
  snprintf(name, BSE_FILE_FULL_LEN, "%" PRId64 ".%s", seq, BSE_DATA_SUFFIX);
}