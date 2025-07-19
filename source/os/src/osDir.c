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

int32_t wordexp(char *words, wordexp_t *pwordexp, int32_t flags) {
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
#include <dlfcn.h>
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

#include <dlfcn.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#ifndef TD_ASTRA
#include <wordexp.h>
#endif

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
      TAOS_UNUSED(taosRemoveFile(filename));
      // printf("file:%s is removed\n", filename);
    }
  }

  TAOS_UNUSED(taosCloseDir(&pDir));
  TAOS_UNUSED(rmdir(dirname));

  // printf("dir:%s is removed\n", dirname);
  return;
}

bool taosDirExist(const char *dirname) {
  if (dirname == NULL || strlen(dirname) >= TDDIRMAXLEN) return false;
  return taosCheckExistFile(dirname);
}

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
    if (ERRNO == EEXIST) {
      return 0;
    } else {
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
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
  code = taosRealPath(dirname, temp, sizeof(temp));
  if (code != 0) {
    return code;
  }
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
      if (code < 0 && ERRNO != EEXIST) {
        terrno = TAOS_SYSTEM_ERROR(ERRNO);
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
    if (code < 0 && ERRNO != EEXIST) {
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
      return code;
    }
  }

  return 0;
}

int32_t taosMulModeMkDir(const char *dirname, int32_t mode, bool checkAccess) {
  if (dirname == NULL || strlen(dirname) >= TDDIRMAXLEN) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  char    temp[TDDIRMAXLEN];
  char   *pos = temp;
  int32_t code = 0;
#ifdef WINDOWS
  code = taosRealPath(dirname, temp, sizeof(temp));
  if (code != 0) {
    return code;
  }
  if (temp[1] == ':') pos += 3;
#else
  tstrncpy(temp, dirname, sizeof(temp));
#endif

  if (taosDirExist(temp)) {
    if (checkAccess &&
        taosCheckAccessFile(temp, TD_FILE_ACCESS_EXIST_OK | TD_FILE_ACCESS_READ_OK | TD_FILE_ACCESS_WRITE_OK)) {
      return 0;
    }
#ifndef TD_ASTRA  // TD_ASTRA_TODO  IMPORTANT
    code = chmod(temp, mode);
    if (-1 == code) {
      struct stat statbuf = {0};
      code = stat(temp, &statbuf);
      if (code != 0 || (statbuf.st_mode & mode) != mode) {
        terrno = TAOS_SYSTEM_ERROR(ERRNO);
        return terrno;
      }
    }
#else
    return 0;
#endif
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
      if (code < 0 && ERRNO != EEXIST) {
        terrno = TAOS_SYSTEM_ERROR(ERRNO);
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
    if (code < 0 && ERRNO != EEXIST) {
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
      return terrno;
    }
  }

  if (code < 0 && ERRNO == EEXIST) {
    if (checkAccess &&
        taosCheckAccessFile(temp, TD_FILE_ACCESS_EXIST_OK | TD_FILE_ACCESS_READ_OK | TD_FILE_ACCESS_WRITE_OK)) {
      return 0;
    }
  }
#ifndef TD_ASTRA  // TD_ASTRA_TODO  IMPORTANT
  code = chmod(temp, mode);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }
#else
  code = 0;
#endif
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
    (void)snprintf(filename, sizeof(filename), "%s%s%s", dirname, TD_DIRSEP, taosGetDirEntryName(de));
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
        TAOS_UNUSED(taosRemoveFile(filename));
        // printf("file:%s is removed, days:%d keepDays:%d, sed:%" PRId64, filename, days, keepDays, fileSec);
      } else {
        // printf("file:%s won't be removed, days:%d keepDays:%d", filename, days, keepDays);
      }
    }
  }

  TAOS_UNUSED(taosCloseDir(&pDir));
  TAOS_UNUSED(rmdir(dirname));
}

int32_t taosExpandDir(const char *dirname, char *outname, int32_t maxlen) {
  OS_PARAM_CHECK(dirname);
  OS_PARAM_CHECK(outname);
  if (dirname[0] == 0) return 0;

#ifndef TD_ASTRA
  wordexp_t full_path = {0};
  int32_t   code = wordexp(dirname, &full_path, 0);
  switch (code) {
    case 0:
      break;
    case WRDE_NOSPACE:
      wordfree(&full_path);
      // FALL THROUGH
    default:
      return terrno = TSDB_CODE_INVALID_PARA;
  }

  if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
    tstrncpy(outname, full_path.we_wordv[0], maxlen);
  }

  wordfree(&full_path);
#else // TD_ASTRA_TODO
  tstrncpy(outname, dirname, maxlen);
#endif
  return 0;
}

int32_t taosRealPath(char *dirname, char *realPath, int32_t maxlen) {
  OS_PARAM_CHECK(dirname);

#ifndef TD_ASTRA
  char tmp[PATH_MAX + 1] = {0};
#ifdef WINDOWS
  if (_fullpath(tmp, dirname, maxlen) != NULL) {
#else
  if (realpath(dirname, tmp) != NULL) {
#endif
    if (strlen(tmp) < maxlen) {
      if (realPath == NULL) {
        tstrncpy(dirname, tmp, maxlen);
      } else {
        tstrncpy(realPath, tmp, maxlen);
      }
      return 0;
    } else {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return terrno;
    }
  } else {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }
#else  // TD_ASTRA_TODO
  if (realPath) tstrncpy(realPath, dirname, maxlen);
  return 0;
#endif
}

bool taosIsDir(const char *dirname) {
  TdDirPtr pDir = taosOpenDir(dirname);
  if (pDir != NULL) {
    TAOS_SKIP_ERROR(taosCloseDir(&pDir));
    return true;
  }
  return false;
}

char *taosDirName(char *name) {
  if (name == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
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
  if (name == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
#ifdef WINDOWS
  char Filename1[MAX_PATH], Ext1[MAX_PATH];
  _splitpath(name, NULL, NULL, Filename1, Ext1);
  return name + (strlen(name) - strlen(Filename1) - strlen(Ext1));
#else
  if ((name[0] == '/' && name[1] == '\0')) return name;
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
  char   szFind[MAX_PATH];  // 这是要找的
  HANDLE hFind;

  TdDirPtr pDir = taosMemoryMalloc(sizeof(TdDir));
  if (pDir == NULL) {
    return NULL;
  }

  snprintf(szFind, sizeof(szFind), "%s%s", dirname, "\\*.*");  // 利用通配符找这个目录下的所以文件，包括目录

  pDir->hFind = FindFirstFile(szFind, &(pDir->dirEntry.findFileData));
  if (INVALID_HANDLE_VALUE == pDir->hFind) {
    taosMemoryFree(pDir);
    DWORD errorCode = GetLastError();
    terrno = TAOS_SYSTEM_WINAPI_ERROR(errorCode);
    return NULL;
  }
  return pDir;
#elif defined(DARWIN)
  DIR *pDir = opendir(dirname);
  if (pDir == NULL) return NULL;
  TdDirPtr dirPtr = (TdDirPtr)taosMemoryMalloc(sizeof(TdDir));
  if (dirPtr == NULL) {
    (void)closedir(pDir);
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return NULL;
  }
  dirPtr->dirEntryPtr = (TdDirEntryPtr) & (dirPtr->dirEntry1);
  dirPtr->pDir = pDir;
  return dirPtr;
#else
  TdDirPtr ptr = (TdDirPtr)opendir(dirname);
  if (NULL == ptr) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
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
  SET_ERRNO(0);
  terrno = 0;
  TdDirEntryPtr p = (TdDirEntryPtr)readdir((DIR *)pDir);
  if (NULL == p && ERRNO) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
  }
  return p;
#endif
}

bool taosDirEntryIsDir(TdDirEntryPtr pDirEntry) {
  if (pDirEntry == NULL) {
    return false;
  }
#ifdef WINDOWS
  if (pDirEntry->findFileData.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) {
    return false;
  }
  return (pDirEntry->findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
#elif defined(TD_ASTRA)  // TD_ASTRA_TODO
  return ((dirent *)pDirEntry)->d_mode == 1;  // DIRECTORY_ENTRY;
#else
  return (((dirent *)pDirEntry)->d_type & DT_DIR) != 0;
#endif
}

char *taosGetDirEntryName(TdDirEntryPtr pDirEntry) {
  if (pDirEntry == NULL) {
    return NULL;
  }
#ifdef WINDOWS
  return pDirEntry->findFileData.cFileName;
#else
  return ((dirent *)pDirEntry)->d_name;
#endif
}

int32_t taosCloseDir(TdDirPtr *ppDir) {
  int32_t code = 0;
  if (ppDir == NULL || *ppDir == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifdef WINDOWS
  if (!FindClose((*ppDir)->hFind)) {
    terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
    return terrno;
  }
  taosMemoryFree(*ppDir);
  *ppDir = NULL;
  return 0;
#elif defined(DARWIN)
  code = closedir((*ppDir)->pDir);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }
  taosMemoryFree(*ppDir);
  *ppDir = NULL;
  return 0;
#else
  code = closedir((DIR *)*ppDir);
  *ppDir = NULL;
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
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
  tstrncpy(buf, "not implemented on windows", len);
#endif
}

int32_t taosAppPath(char *path, int32_t maxLen) {
  int32_t ret = 0;

#ifdef WINDOWS
  ret = GetModuleFileName(NULL, path, maxLen - 1);
#elif defined(DARWIN)
  ret = _NSGetExecutablePath(path, &maxLen) ;
#else
  ret = readlink("/proc/self/exe", path, maxLen - 1);
#endif

  if (ret >= 0) {
    ret = (taosDirName(path) == NULL) ? -1 : 0;
  }

  return ret;
}

int32_t taosGetDirSize(const char *path, int64_t *size) {
  int32_t code = 0;
  char    fullPath[PATH_MAX + 100] = {0};

  TdDirPtr pDir = taosOpenDir(path);
  if (pDir == NULL) {
    return code = terrno;
  }

  int64_t       totalSize = 0;
  TdDirEntryPtr de = NULL;
  while ((de = taosReadDir(pDir)) != NULL) {
    char *name = taosGetDirEntryName(de);
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
      continue;
    }
    (void)snprintf(fullPath, sizeof(fullPath), "%s%s%s", path, TD_DIRSEP, name);

    int64_t subSize = 0;
    if (taosIsDir(fullPath)) {
      code = taosGetDirSize(fullPath, &subSize);
    } else {
      code = taosStatFile(fullPath, &subSize, NULL, NULL);
    }

    if (code != 0) goto _OVER;
    
    totalSize += subSize;
    fullPath[0] = 0;
  }

_OVER:
  *size = totalSize;
  TAOS_UNUSED(taosCloseDir(&pDir));
  return code;
}


void* taosLoadDll(const char* fileName) {
#if defined(WINDOWS)
  void* handle = LoadLibraryA(fileName);
#else
  void* handle = dlopen(fileName, RTLD_LAZY);
#endif

  if (handle == NULL) {
    if (errno != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
    } else {
      terrno = TSDB_CODE_DLL_NOT_LOAD;
    }
  }

  return handle;
}

void taosCloseDll(void* handle) {
  if (handle == NULL) return;

#if defined(WINDOWS)
  FreeLibrary((HMODULE)handle);
#else
  if (dlclose(handle) != 0 && errno != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
  }
#endif
}

void* taosLoadDllFunc(void* handle, const char* funcName) {
  if (handle == NULL) return NULL;

#if defined(WINDOWS)
  void *fptr = GetProcAddress((HMODULE)handle, funcName);
#else
  void *fptr = dlsym(handle, funcName);
#endif

  if (handle == NULL) {
    if (errno != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
    } else {
      terrno = TSDB_CODE_DLL_FUNC_NOT_LOAD;
    }
  }

  return fptr;
}
