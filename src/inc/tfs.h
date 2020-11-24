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

const char *TFS_PRIMARY_PATH();
const char *TFS_DISK_PATH(int level, int id);

// MANIP APIS ====================================
int tfsMkdir(const char *rname);
int tfsRmdir(const char *rname);
int tfsRename(char *orname, char *nrname);

// tfcntl.c ====================================
typedef struct TFSFILE {
  int  level;
  int  id;
  char rname[TSDB_FILENAME_LEN];  // REL name
  char aname[TSDB_FILENAME_LEN];  // ABS name
} TFSFILE;

const char *tfsAbsName(TFSFILE *pfile);
const char *tfsRelName(TFSFILE *pfile);
void        tfsDirName(TFSFILE *pfile, char dest[]);
void        tfsBaseName(TFSFILE *pfile, char dest[]);
int         tfsopen(TFSFILE *pfile, int flags);
int         tfsclose(int fd);
int         tfsremove(TFSFILE *pfile);
SDiskID     tfsFileID(TFSFILE *pfile);

typedef struct TFSDIR TFSDIR;

int            tfsCreateDir(char *dirname);
int            tfsRemoveDir(char *dirname);
int            tfsRename(char *oldpath, char *newpath);
TFSDIR *       tfsOpenDir(char *dir);
void           tfsCloseDir(TFSDIR *tdir);
const TFSFILE *tfsReadDir(TFSDIR *tdir);

#ifdef __cplusplus
}
#endif

#endif