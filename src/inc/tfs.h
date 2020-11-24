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

#ifndef TD_TFS_H
#define TD_TFS_H

#include "tglobal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int level;
  int id;
} SDiskID;

#define TFS_UNDECIDED_LEVEL -1
#define TFS_UNDECIDED_ID -1
#define TFS_PRIMARY_LEVEL 0
#define TFS_PRIMARY_ID 0

// FS APIs ====================================
int  tfsInit(SDiskCfg *pDiskCfg, int ndisk);
void tfsDestroy();
void tfsUpdateInfo();
void tfsIncDiskFile(int level, int id, int num);
void tfsDecDiskFile(int level, int id, int num);

const char *TFS_PRIMARY_PATH();
const char *TFS_DISK_PATH(int level, int id);

// TFILE APIs ====================================
typedef struct {
  int  level;
  int  id;
  char rname[TSDB_FILENAME_LEN];  // REL name
  char aname[TSDB_FILENAME_LEN];  // ABS name
} TFILE;

#define TFILE_LEVEL(pf) ((pf)->level)
#define TFILE_ID(pf) ((pf)->id)
#define TFILE_NAME(pf) ((pf)->aname)

void tfsInitFile(TFILE *pf, int level, int id, const char *bname);
int  tfsopen(TFILE *pf, int flags);
int  tfsclose(int fd);
int  tfsremove(TFILE *pf);

// DIR APIs ====================================
int tfsMkdir(const char *rname);
int tfsRmdir(const char *rname);
int tfsRename(char *orname, char *nrname);

typedef struct TDIR TDIR;

TDIR *       tfsOpendir(const char *rname);
const TFILE *tfsReaddir(TDIR *tdir);
void         tfsClosedir(TDIR *tdir);

#ifdef __cplusplus
}
#endif

#endif