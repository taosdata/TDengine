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

#include "os.h"
#include "taoserror.h"
#include "tdisk.h"
#include "tfs.h"

struct TFSFILE {
  int  level;
  int  id;
  char name[TSDB_FILENAME_LEN];
};

struct TFSDIR {
  int     level;
  int     id;
  char    name[TSDB_FILENAME_LEN];
  TFSFILE tfsfile;
  DIR *   dir;
};

TFSDIR *tfsOpenDir(char *dir) {
  TFSDIR *tdir = (TFSDIR *)calloc(1, sizeof(*tdir));
  if (tdir == NULL) {
    terrno = TSDB_CODE_FS_OUT_OF_MEMORY;
    return NULL;
  }

  if (tfsOpenDirImpl(tdir) < 0) {
    tfsCloseDir(tdir);
    return NULL;
  }

  return tdir;
}

void tfsCloseDir(TFSDIR *tdir) {
  if (tdir) {
    if (tdir->dir != NULL) {
      closedir(tdir->dir);
      tdir->dir = NULL;
    }
    free(tdir);
  }
}

const TFSFILE *tfsReadDir(TFSDIR *tdir) {
  if (tdir->dir == NULL) return NULL;

  while (true) {
    struct dirent *dp = readdir(tdir->dir);
    if (dp != NULL) {
      tdir->tfsfile.level = tdir->level;
      tdir->tfsfile.id = tdir->id;
      snprintf(tdir->tfsfile.name, TSDB_FILENAME_LEN, "%s/%s", tdir->name, dp->d_name);

      return &(tdir->tfsfile);
    }

    closedir(tdir->dir);

    // Move to next
    if (tdir->id < tfsNDisksAt(tdir->level) - 1) {
      tdir->id++;
    } else {
      tdir->level++;
      tdir->id = 0;
    }

    if (tfsOpenDirImpl(tdir) < 0) {
      return NULL;
    }

    if (tdir->dir == NULL) return NULL;
  }
}

static int tfsOpenDirImpl(TFSDIR *tdir) {
  char dirName[TSDB_FILENAME_LEN] = "\0";

  while (tdir->level < tfsMaxLevel()) {
    while (tdir->id < tfsNDisksAt(tdir->level)) {
      snprintf(dirName, TSDB_FILENAME_LEN, "%s/%s", tfsGetDiskDir(tdir->level, tdir->id), tdir->name);

      tdir->dir = opendir(dirName);
      if (tdir->dir == NULL) {
        if (errno == ENOENT) {
          tdir->id++;
        } else {
          fError("failed to open dir %s since %s", dirName, strerror(errno));
          terrno = TAOS_SYSTEM_ERROR(errno);
          return -1;
        }
      } else {
        return 0;
      }
    }

    tdir->id = 0;
    tdir->level++;
  }

  ASSERT(tdir->dir == NULL);
  return 0;
}