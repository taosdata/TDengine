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
  char rname[TSDB_FILENAME_LEN];   // REL name
  char aname[TSDB_FILENAME_LEN];  // ABS name
};

struct TFSDIR {
  int     level;
  int     id;
  char    name[TSDB_FILENAME_LEN];
  TFSFILE tfsfile;
  DIR *   dir;
};

// PUBLIC ==========================================
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

  char rname[TSDB_FILENAME_LEN] = "\0";

  while (true) {
    struct dirent *dp = readdir(tdir->dir);
    if (dp != NULL) {
      snprintf(rname, TSDB_FILENAME_LEN, "%s/%s", tdir->name, dp->d_name);
      tfsInitFile(&(tdir->tfsfile), tdir->level, tdir->id, rname);

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

const char *tfsAbsName(TFSFILE *pfile, char dest[]) { return pfile->aname; }
const char *tfsRelName(TFSFILE *pfile, char dest[]) { return pfile->rname; }

void tfsDirName(TFSFILE *pfile, char dest[]) {
  char fname[TSDB_FILENAME_LEN] = "\0";

  tfsAbsFname(pfile, fname);
  strncpy(dest, dirname(fname), TSDB_FILENAME_LEN);
}

void tfsBaseName(TFSFILE *pfile, char dest[]) {
  char fname[TSDB_FILENAME_LEN] = "\0";
  memcpy((void *)fname, (void *)pfile->rname, TSDB_FILENAME_LEN);
  strncpy(dest, basename(fname), TSDB_FILENAME_LEN);
}

int tfsopen(TFSFILE *pfile, int flags) {
  int fd = open(pfile->aname, flags);
  if (fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return fd;
}

int tfsclose(int fd) {
  if (close(fd) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0
}

TFSFILE *tfsCreateFiles(int level, int nfile, ...) {
  // TODO
  return NULL;
}

int tfsRemoveFiles(int nfile, ...) {
  va_list  valist;
  TFSFILE *pfile = NULL;
  int      code = 0;

  va_start(valist, nfile);
  tfsLock();

  for (int i = 0; i < nfile; i++) {
    pfile = va_arg(valist, TFSFILE *);
    code = remove(pfile->aname);
    if (code != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      tfsUnLock();
      va_end(valist);
      return -1;
    }

    tfsDecFileAt(pfile->level, pfile->id);
  }

  tfsUnLock();
  va_end(valist);

  return 0;
}

SDiskID tfsFileID(TFSFILE *pfile) {
  SDiskID did;

  did.level = pfile->level;
  did.id = pfile->id;

  return did;
}

// PRIVATE =============================================
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

static void tfsInitFile(TFSFILE *pfile, int level, int id, char *rname) {
  pfile->level = level;
  pfile->id = id;
  strncpy(pfile->rname, rname, TSDB_FILENAME_LEN);
  snprintf(pfile->aname, TSDB_FILENAME_LEN, "%s/%s", tfsGetDiskName(level, id), rname);
}

static TFSFILE *tfsNewFile(int level, int id, char *rname) {
  TFSFILE *pfile = (TFSFILE *)calloc(1, sizeof(*pfile));
  if (pfile == NULL) {
    terrno = TSDB_CODE_FS_OUT_OF_MEMORY;
    return NULL;
  }

  tfsInitFile(pfile, level, id, rname);
  return pfile;
}
