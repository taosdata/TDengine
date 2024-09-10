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

#ifdef WINDOWS

#include <windows.h>

typedef struct TdDirEntry {
  WIN32_FIND_DATA findFileData;
} TdDirEntry;

typedef struct TdDir {
  TdDirEntry dirEntry;
  HANDLE     hFind;
} TdDir;

enum {
  WRDE_NOSPACE = 1, /* Ran out of memory.  */
  WRDE_BADCHAR,     /* A metachar appears in the wrong place.  */
  WRDE_BADVAL,      /* Undefined var reference with WRDE_UNDEF.  */
  WRDE_CMDSUB,      /* Command substitution with WRDE_NOCMD.  */
  WRDE_SYNTAX       /* Shell syntax error.  */
};

int wordexp(char *words, wordexp_t *pwordexp, int flags) {
  pwordexp->we_offs = 0;
  pwordexp->we_wordc = 1;
  pwordexp->we_wordv[0] = pwordexp->wordPos;

  (void)memset(pwordexp->wordPos, 0, 1025);
  if (_fullpath(pwordexp->wordPos, words, 1024) == NULL) {
    pwordexp->we_wordv[0] = words;
    printf("failed to parse relative path:%s to abs path\n", words);
    return -1;
  }

  // printf("parse relative path:%s to abs path:%s\n", words, pwordexp->wordPos);
  return 0;
}

void wordfree(wordexp_t *pwordexp) {}

#elif defined(DARWIN)

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <wordexp.h>

typedef struct dirent dirent;
typedef struct dirent TdDirEntry;

typedef struct TdDir {
  TdDirEntry    dirEntry;
  TdDirEntry    dirEntry1;
  TdDirEntryPtr dirEntryPtr;
  DIR          *pDir;
} TdDir;

#else

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <wordexp.h>

typedef struct dirent dirent;
typedef struct DIR    TdDir;
typedef struct dirent TdDirEntry;

#endif

#define TDDIRMAXLEN 1024

void taosRemoveDir(const char *dirname) {
  TdDirPtr pDir = taosOpenDir(dirname);
  if (pDir == NULL) return;

  TdDirEntryPtr de = NULL;
  while ((de = taosReadDir(pDir)) != NULL) {
    if (strcmp(taosGetDirEntryName(de), ".") == 0 || strcmp(taosGetDirEntryName(de), "..") == 0) continue;

    char filename[1024] = {0};
    (void)snprintf(filename, sizeof(filename), "%s%s%s", dirname, TD_DIRSEP, taosGetDirEntryName(de));
    if (taosDirEntryIsDir(de)) {
      taosRemoveDir(filename);
    } else {
      (void)taosRemoveFile(filename);
      // printf("file:%s is removed\n", filename);
    }
  }

  (void)taosCloseDir(&pDir);
  (void)rmdir(dirname);

  // printf("dir:%s is removed\n", dirname);
  return;
}

bool taosDirExist(const char *dirname) { return taosCheckExistFile(dirname); }

int32_t taosMkDir(const char *dirname) {
  if (taosDirExist(dirname)) return 0;
#ifdef WINDOWS
  int32_t code = _mkdir(dirname, 0755);
#elif defined(DARWIN)
  int32_t code = mkdir(dirname, 0777);
#else
  int32_t code = mkdir(dirname, 0755);
#endif
  if (-1 == code) {
    if (errno == EEXIST) {
      return 0;
    } else {
      terrno = TAOS_SYSTEM_ERROR(errno);
      code = terrno;
    }
  }

  return code;
}

int32_t taosMulMkDir(const char *dirname) {
  if (dirname == NULL || strlen(dirname) >= TDDIRMAXLEN) return -1;
  char    temp[TDDIRMAXLEN];
  char   *pos = temp;
  int32_t code = 0;
#ifdef WINDOWS
  taosRealPath(dirname, temp, sizeof(temp));
  if (temp[1] == ':') pos += 3;
#else
  tstrncpy(temp, dirname, sizeof(temp));
#endif

  if (taosDirExist(temp)) return code;

  if (strncmp(temp, TD_DIRSEP, 1) == 0) {
    pos += 1;
  } else if (strncmp(temp, "." TD_DIRSEP, 2) == 0) {
    pos += 2;
  }

  for (; *pos != '\0'; pos++) {
    if (*pos == TD_DIRSEP[0]) {
      *pos = '\0';
#ifdef WINDOWS
      code = _mkdir(temp, 0755);
#elif defined(DARWIN)
      code = mkdir(temp, 0777);
#else
      code = mkdir(temp, 0755);
#endif
      if (code < 0 && errno != EEXIST) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return code;
      }
      *pos = TD_DIRSEP[0];
    }
  }

  if (*(pos - 1) != TD_DIRSEP[0]) {
#ifdef WINDOWS
    code = _mkdir(temp, 0755);
#elif defined(DARWIN)
    code = mkdir(dirname, 0777);
#else
    code = mkdir(temp, 0755);
#endif
    if (code < 0 && errno != EEXIST) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return code;
    }
  }

  if (code < 0 && errno == EEXIST) {
    return 0;
  }

  return code;
}

int32_t taosMulModeMkDir(const char *dirname, int mode, bool checkAccess) {
  if (dirname == NULL || strlen(dirname) >= TDDIRMAXLEN) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  char    temp[TDDIRMAXLEN];
  char   *pos = temp;
  int32_t code = 0;
#ifdef WINDOWS
  taosRealPath(dirname, temp, sizeof(temp));
  if (temp[1] == ':') pos += 3;
#else
  tstrncpy(temp, dirname, sizeof(temp));
#endif

  if (taosDirExist(temp)) {
    if (checkAccess &&
        taosCheckAccessFile(temp, TD_FILE_ACCESS_EXIST_OK | TD_FILE_ACCESS_READ_OK | TD_FILE_ACCESS_WRITE_OK)) {
      return 0;
    }
    code = chmod(temp, mode);
    if (-1 == code) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return terrno;
    }
  }

  if (strncmp(temp, TD_DIRSEP, 1) == 0) {
    pos += 1;
  } else if (strncmp(temp, "." TD_DIRSEP, 2) == 0) {
    pos += 2;
  }

  for (; *pos != '\0'; pos++) {
    if (*pos == TD_DIRSEP[0]) {
      *pos = '\0';
#ifdef WINDOWS
      code = _mkdir(temp, mode);
#elif defined(DARWIN)
      code = mkdir(temp, 0777);
#else
      code = mkdir(temp, mode);
#endif
      if (code < 0 && errno != EEXIST) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return terrno;
      }
      *pos = TD_DIRSEP[0];
    }
  }

  if (*(pos - 1) != TD_DIRSEP[0]) {
#ifdef WINDOWS
    code = _mkdir(temp, mode);
#elif defined(DARWIN)
    code = mkdir(dirname, 0777);
#else
    code = mkdir(temp, mode);
#endif
    if (code < 0 && errno != EEXIST) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return terrno;
    }
  }

  if (code < 0 && errno == EEXIST) {
    if (checkAccess &&
        taosCheckAccessFile(temp, TD_FILE_ACCESS_EXIST_OK | TD_FILE_ACCESS_READ_OK | TD_FILE_ACCESS_WRITE_OK)) {
      return 0;
    }
  }

  code = chmod(temp, mode);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }
  return code;
}

void taosRemoveOldFiles(const char *dirname, int32_t keepDays) {
  TdDirPtr pDir = taosOpenDir(dirname);
  if (pDir == NULL) return;

  int64_t       sec = taosGetTimestampSec();
  TdDirEntryPtr de = NULL;

  while ((de = taosReadDir(pDir)) != NULL) {
    if (strcmp(taosGetDirEntryName(de), ".") == 0 || strcmp(taosGetDirEntryName(de), "..") == 0) continue;

    char filename[1024];
    (void)snprintf(filename, sizeof(filename), "%s/%s", dirname, taosGetDirEntryName(de));
    if (taosDirEntryIsDir(de)) {
      continue;
    } else {
      int32_t len = (int32_t)strlen(filename);
      if (len > 3 && strcmp(filename + len - 3, ".gz") == 0) {
        len -= 3;
      } else {
        continue;
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
        uInfo("file:%s is removed, days:%d keepDays:%d, sed:%" PRId64, filename, days, keepDays, fileSec);
      } else {
        // printf("file:%s won't be removed, days:%d keepDays:%d", filename, days, keepDays);
      }
    }
  }

  (void)taosCloseDir(&pDir);
  (void)rmdir(dirname);
}

int32_t taosExpandDir(const char *dirname, char *outname, int32_t maxlen) {
  wordexp_t full_path;
  int32_t   code = wordexp(dirname, &full_path, 0);
  switch (code) {
    case 0:
      break;
    case WRDE_NOSPACE:
      wordfree(&full_path);
      // FALL THROUGH
    default:
      return code;
  }

  if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
    (void)strncpy(outname, full_path.we_wordv[0], maxlen);
  }

  wordfree(&full_path);

  return 0;
}

int32_t taosRealPath(char *dirname, char *realPath, int32_t maxlen) {
  char tmp[PATH_MAX] = {0};
#ifdef WINDOWS
  if (_fullpath(tmp, dirname, maxlen) != NULL) {
#else
  if (realpath(dirname, tmp) != NULL) {
#endif
    if (strlen(tmp) < maxlen) {
      if (realPath == NULL) {
        (void)strncpy(dirname, tmp, maxlen);
      } else {
        (void)strncpy(realPath, tmp, maxlen);
      }
      return 0;
    }
  }

  terrno = TAOS_SYSTEM_ERROR(errno);

  return terrno;
}

bool taosIsDir(const char *dirname) {
  TdDirPtr pDir = taosOpenDir(dirname);
  if (pDir != NULL) {
    (void)taosCloseDir(&pDir);
    return true;
  }
  return false;
}

char *taosDirName(char *name) {
#ifdef WINDOWS
  char Drive1[MAX_PATH], Dir1[MAX_PATH];
  _splitpath(name, Drive1, Dir1, NULL, NULL);
  size_t dirNameLen = strlen(Drive1) + strlen(Dir1);
  if (dirNameLen > 0) {
    if (name[dirNameLen - 1] == '/' || name[dirNameLen - 1] == '\\') {
      name[dirNameLen - 1] = 0;
    } else {
      name[dirNameLen] = 0;
    }
  } else {
    name[0] = 0;
  }
  return name;
#else
  char *end = strrchr(name, '/');
  if (end != NULL) {
    *end = '\0';
  } else {
    name[0] = 0;
  }
  return name;
#endif
}

char *taosDirEntryBaseName(char *name) {
#ifdef WINDOWS
  char Filename1[MAX_PATH], Ext1[MAX_PATH];
  _splitpath(name, NULL, NULL, Filename1, Ext1);
  return name + (strlen(name) - strlen(Filename1) - strlen(Ext1));
#else
  if (name == NULL || (name[0] == '/' && name[1] == '\0')) return name;
  char *pPoint = strrchr(name, '/');
  if (pPoint != NULL) {
    if (*(pPoint + 1) == '\0') {
      *pPoint = '\0';
      return taosDirEntryBaseName(name);
    }
    return pPoint + 1;
  }
  return name;
#endif
}

TdDirPtr taosOpenDir(const char *dirname) {
  if (dirname == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

#ifdef WINDOWS
  char   szFind[MAX_PATH];  //这是要找的
  HANDLE hFind;

  TdDirPtr pDir = taosMemoryMalloc(sizeof(TdDir));

  tstrncpy(szFind, dirname, sizeof(szFind));
  strcat(szFind, "\\*.*");  //利用通配符找这个目录下的所以文件，包括目录

  pDir->hFind = FindFirstFile(szFind, &(pDir->dirEntry.findFileData));
  if (INVALID_HANDLE_VALUE == pDir->hFind) {
    taosMemoryFree(pDir);
    return NULL;
  }
  return pDir;
#elif defined(DARWIN)
  DIR *pDir = opendir(dirname);
  if (pDir == NULL) return NULL;
  TdDirPtr dirPtr = (TdDirPtr)taosMemoryMalloc(sizeof(TdDir));
  dirPtr->dirEntryPtr = (TdDirEntryPtr) & (dirPtr->dirEntry1);
  dirPtr->pDir = pDir;
  return dirPtr;
#else
  TdDirPtr ptr = (TdDirPtr)opendir(dirname);
  if (NULL == ptr) {
    terrno = TAOS_SYSTEM_ERROR(errno);
  }
  return ptr;
#endif
}

TdDirEntryPtr taosReadDir(TdDirPtr pDir) {
  if (pDir == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
#ifdef WINDOWS
  if (!FindNextFile(pDir->hFind, &(pDir->dirEntry.findFileData))) {
    return NULL;
  }
  return (TdDirEntryPtr) & (pDir->dirEntry.findFileData);
#elif defined(DARWIN)
  if (readdir_r(pDir->pDir, (dirent *)&(pDir->dirEntry), (dirent **)&(pDir->dirEntryPtr)) == 0) {
    return pDir->dirEntryPtr;
  } else {
    return NULL;
  }
#else
  errno = 0;
  terrno = 0;
  TdDirEntryPtr p = (TdDirEntryPtr)readdir((DIR *)pDir);
  if (NULL == p && errno) {
    terrno = TAOS_SYSTEM_ERROR(errno);
  }
  return p;
#endif
}

bool taosDirEntryIsDir(TdDirEntryPtr pDirEntry) {
  if (pDirEntry == NULL) {
    return false;
  }
#ifdef WINDOWS
  return (pDirEntry->findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
#else
  return (((dirent *)pDirEntry)->d_type & DT_DIR) != 0;
#endif
}

char *taosGetDirEntryName(TdDirEntryPtr pDirEntry) {
  /*if (pDirEntry == NULL) {*/
  /*return NULL;*/
  /*}*/
#ifdef WINDOWS
  return pDirEntry->findFileData.cFileName;
#else
  return ((dirent *)pDirEntry)->d_name;
#endif
}

int32_t taosCloseDir(TdDirPtr *ppDir) {
  if (ppDir == NULL || *ppDir == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifdef WINDOWS
  FindClose((*ppDir)->hFind);
  taosMemoryFree(*ppDir);
  *ppDir = NULL;
  return 0;
#elif defined(DARWIN)
  closedir((*ppDir)->pDir);
  taosMemoryFree(*ppDir);
  *ppDir = NULL;
  return 0;
#else
  int32_t code = closedir((DIR *)*ppDir);
  *ppDir = NULL;
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }
  return 0;
#endif
}

void taosGetCwd(char *buf, int32_t len) {
#if !defined(WINDOWS)
  char *unused __attribute__((unused));
  unused = getcwd(buf, len - 1);
#else
  strncpy(buf, "not implemented on windows", len - 1);
#endif
}
