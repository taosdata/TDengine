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

#define _DEFAULT_SOURCE
#define ALLOW_FORBID_FUNC

#include "os.h"
#include "osString.h"

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
/*
 * windows implementation
 */

// todo

#else
/*
 * linux implementation
 */

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <wordexp.h>

typedef struct dirent dirent;
typedef struct DIR TdDir;
typedef struct dirent TdDirent;

void taosRemoveDir(const char *dirname) {
  DIR *dir = opendir(dirname);
  if (dir == NULL) return;

  struct dirent *de = NULL;
  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

    char filename[1024];
    snprintf(filename, sizeof(filename), "%s/%s", dirname, de->d_name);
    if (de->d_type & DT_DIR) {
      taosRemoveDir(filename);
    } else {
      (void)taosRemoveFile(filename);
      //printf("file:%s is removed\n", filename);
    }
  }

  closedir(dir);
  rmdir(dirname);

  //printf("dir:%s is removed\n", dirname);
}

bool taosDirExist(char *dirname) { return taosCheckExistFile(dirname); }

int32_t taosMkDir(const char *dirname) {
  int32_t code = mkdir(dirname, 0755);
  if (code < 0 && errno == EEXIST) {
    return 0;
  }

  return code;
}

int32_t taosMulMkDir(const char *dirname) {
  if (dirname == NULL) return -1;
  char *temp = strdup(dirname);
  char *pos = temp;
  int32_t code = 0;

  if (strncmp(temp, "/", 1) == 0) {
    pos += 1;
  } else if (strncmp(temp, "./", 2) == 0) {
    pos += 2;
  }
  
  for ( ; *pos != '\0'; pos++) {
    if (*pos == '/') {
      *pos = '\0';
      code = mkdir(temp, 0755);
      if (code < 0 && errno != EEXIST) {
        free(temp);
        return code;
      }
      *pos = '/';
    }
  }
  
  if (*(pos - 1) != '/') {
    code = mkdir(temp, 0755);
    if (code < 0 && errno != EEXIST) {
      free(temp);
      return code;
    }
  }
  free(temp);

  // int32_t code = mkdir(dirname, 0755);
  if (code < 0 && errno == EEXIST) {
    return 0;
  }

  return code;
}

void taosRemoveOldFiles(const char *dirname, int32_t keepDays) {
  DIR *dir = opendir(dirname);
  if (dir == NULL) return;

  int64_t        sec = taosGetTimestampSec();
  struct dirent *de = NULL;

  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

    char filename[1024];
    snprintf(filename, sizeof(filename), "%s/%s", dirname, de->d_name);
    if (de->d_type & DT_DIR) {
      continue;
    } else {
      int32_t len = (int32_t)strlen(filename);
      if (len > 3 && strcmp(filename + len - 3, ".gz") == 0) {
        len -= 3;
      }

      int64_t fileSec = 0;
      for (int32_t i = len - 1; i >= 0; i--) {
        if (filename[i] == '.') {
          fileSec = atoll(filename + i + 1);
          break;
        }
      }

      if (fileSec <= 100) continue;
      int32_t days = (int32_t)(TABS(sec - fileSec) / 86400 + 1);
      if (days > keepDays) {
        (void)taosRemoveFile(filename);
        //printf("file:%s is removed, days:%d keepDays:%d", filename, days, keepDays);
      } else {
        //printf("file:%s won't be removed, days:%d keepDays:%d", filename, days, keepDays);
      }
    }
  }

  closedir(dir);
  rmdir(dirname);
}

int32_t taosExpandDir(const char *dirname, char *outname, int32_t maxlen) {
  wordexp_t full_path;
  if (0 != wordexp(dirname, &full_path, 0)) {
    //printf("failed to expand path:%s since %s", dirname, strerror(errno));
    wordfree(&full_path);
    return -1;
  }

  if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
    strncpy(outname, full_path.we_wordv[0], maxlen);
  }

  wordfree(&full_path);

  return 0;
}

int32_t taosRealPath(char *dirname, int32_t maxlen) {
  char tmp[PATH_MAX] = {0};
  if (realpath(dirname, tmp) != NULL) {
    strncpy(dirname, tmp, maxlen);
  }

  return 0;
}

bool taosIsDir(const char *dirname) {
  DIR *dir = opendir(dirname);
  if (dir != NULL) {
    closedir(dir);
    return true;
  }
  return false;
}

char* taosDirName(char *name) {
  return dirname(name);
}

char* taosDirEntryBaseName(char *name) {
  return basename(name);
}

TdDirPtr taosOpenDir(const char *dirname) {
  if (dirname == NULL) {
    return NULL;
  }
  return (TdDirPtr)opendir(dirname);
}

TdDirEntryPtr taosReadDir(TdDirPtr pDir) {
  if (pDir == NULL) {
    return NULL;
  }
  return (TdDirEntryPtr)readdir((DIR*)pDir);
}

bool taosDirEntryIsDir(TdDirEntryPtr pDirEntry) {
  if (pDirEntry == NULL) {
    return false;
  }
  return (((dirent*)pDirEntry)->d_type & DT_DIR) != 0;
}

char* taosGetDirEntryName(TdDirEntryPtr pDirEntry) {
  if (pDirEntry == NULL) {
    return NULL;
  }
  return ((dirent*)pDirEntry)->d_name;
}

int32_t taosCloseDir(TdDirPtr pDir) {
  if (pDir == NULL) {
    return -1;
  }
  return closedir((DIR*)pDir);
}

#endif
