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
#ifndef _TD_TPATH_H_
#define _TD_TPATH_H_

#include <stdio.h>
#include "taosdef.h"

#ifdef __cplusplus
extern "C" {
#endif

static FORCE_INLINE void tdGetMnodeRootDir(char *baseDir, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/mnode", baseDir);
}

static FORCE_INLINE void tdGetDnodeRootDir(char *baseDir, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/dnode", baseDir);
}

static FORCE_INLINE void tdGetVnodeRootDir(char *baseDir, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/vnode", baseDir);
}

static FORCE_INLINE void tdGetVnodeBackRootDir(char *baseDir, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/vnode_bak", baseDir);
}

static FORCE_INLINE void tdGetVnodeDir(char *baseDir, int vid, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/vnode/vnode%d", baseDir, vid);
}

static FORCE_INLINE void tdGetVnodeBackDir(char *baseDir, int vid, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/vnode_bak/vnode%d", baseDir, vid);
}

static FORCE_INLINE void tdGetTsdbRootDir(char *baseDir, int vid, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/vnode/vnode%d/tsdb", baseDir, vid);
}

static FORCE_INLINE void tdGetTsdbDataDir(char *baseDir, int vid, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/vnode/vnode%d/tsdb/data", baseDir, vid);
}

#ifdef __cplusplus
}
#endif

#endif // _TD_TPATH_H_