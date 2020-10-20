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

static FORCE_INLINE void tdGetMnodeRootDir(char *rootDir, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/mnode", rootDir);
}

static FORCE_INLINE void tdGetDnodeRootDir(char *rootDir, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/dnode", rootDir);
}

static FORCE_INLINE void tdGetVnodeRootDir(char *rootDir, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/vnode", rootDir);
}

static FORCE_INLINE void tdGetVnodeBackRootDir(char *rootDir, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/vnode_bak", rootDir);
}

static FORCE_INLINE void tdGetVnodeDir(char *rootDir, int vid, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/vnode/vnode%d", rootDir, vid);
}

static FORCE_INLINE void tdGetVnodeBackDir(char *rootDir, int vid, char *dirName) {
  snprintf(dirName, TSDB_FILENAME_LEN, "%s/vnode_bak/vnode%d", rootDir, vid);
}

#ifdef __cplusplus
}
#endif

#endif // _TD_TPATH_H_