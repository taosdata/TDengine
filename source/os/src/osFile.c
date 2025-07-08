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
#define ALLOW_FORBID_FUNC
#include "os.h"
#include "osSemaphore.h"
#include "tdef.h"
#include "zlib.h"

#ifdef WINDOWS
#include <WinBase.h>
#include <io.h>
#include <ktmw32.h>
#include <windows.h>
#define F_OK 0
#define X_OK 1
#define W_OK 2
#define R_OK 4

#define _SEND_FILE_STEP_ 1024

#else
#include <fcntl.h>
#ifndef TD_ASTRA
#include <sys/file.h>

#if !defined(_TD_DARWIN_64)
#include <sys/sendfile.h>
#endif
#endif
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>
#define LINUX_FILE_NO_TEXT_OPTION 0
#define O_TEXT                    LINUX_FILE_NO_TEXT_OPTION

#define _SEND_FILE_STEP_ 1000
#endif

typedef int32_t FileFd;

#ifdef WINDOWS
typedef struct TdFile {
  TdThreadRwlock rwlock;
  HANDLE         hFile;
  FILE          *fp;
  int32_t        tdFileOptions;
} TdFile;
#else
typedef struct TdFile {
  TdThreadRwlock rwlock;
  FileFd         fd;
  FILE          *fp;
} TdFile;
#endif  // WINDOWS

#define FILE_WITH_LOCK 1

#ifdef BUILD_WITH_RAND_ERR
#define STUB_RAND_IO_ERR(ret)                                \
  if (tsEnableRandErr && (tsRandErrScope & RAND_ERR_FILE)) { \
    uint32_t r = taosRand() % tsRandErrDivisor;              \
    if ((r + 1) <= tsRandErrChance) {                        \
      errno = EIO;                                           \
      terrno = TAOS_SYSTEM_ERROR(errno);                     \
      return (ret);                                          \
    }                                                        \
  }
#else
#define STUB_RAND_IO_ERR(ret)
#endif

void taosGetTmpfilePath(const char *inputTmpDir, const char *fileNamePrefix, char *dstPath) {
  if (inputTmpDir == NULL || fileNamePrefix == NULL) return;
#ifdef WINDOWS

  char tmpPath[PATH_MAX];

  int32_t len = (int32_t)strlen(inputTmpDir);
  memcpy(tmpPath, inputTmpDir, len);

  if (tmpPath[len - 1] != '/' && tmpPath[len - 1] != '\\') {
    tmpPath[len++] = '\\';
  }

  snprintf(tmpPath + len, sizeof(tmpPath) - len, "%s%s%s", TD_TMP_FILE_PREFIX, fileNamePrefix, "-%d-%s");

  char rand[8] = {0};
  taosRandStr(rand, tListLen(rand) - 1);
  snprintf(dstPath, PATH_MAX, tmpPath, taosGetPId(), rand);

#else

  char    tmpPath[PATH_MAX];
  int32_t len = strlen(inputTmpDir);
  (void)memcpy(tmpPath, inputTmpDir, len);
  static uint64_t seqId = 0;

  if (tmpPath[len - 1] != '/') {
    tmpPath[len++] = '/';
  }

  snprintf(tmpPath + len, sizeof(tmpPath) - len, "%s%s%s", TD_TMP_FILE_PREFIX, fileNamePrefix, "-%d-%s");

  char rand[32] = {0};

  (void)snprintf(rand, sizeof(rand), "%" PRIu64, atomic_add_fetch_64(&seqId, 1));

  (void)snprintf(dstPath, PATH_MAX, tmpPath, taosGetPId(), rand);

#endif
}

int64_t taosCopyFile(const char *from, const char *to) {
  if (from == NULL || to == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
#ifdef WINDOWS
  if (CopyFile(from, to, 0)) {
    return 1;
  } else {
    terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
    return -1;
  }
#else
  char    buffer[4096];
  int64_t size = 0;
  int64_t bytes;
  int32_t code = TSDB_CODE_SUCCESS;

  TdFilePtr pFileFrom = NULL;
  TdFilePtr pFileTo = NULL;
  pFileFrom = taosOpenFile(from, TD_FILE_READ);
  if (pFileFrom == NULL) {
    code = terrno;
    goto _err;
  }

  pFileTo = taosOpenFile(to, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_EXCL);
  if (pFileTo == NULL) {
    code = terrno;
    goto _err;
  }

  while (true) {
    bytes = taosReadFile(pFileFrom, buffer, sizeof(buffer));
    if (bytes < 0) {
      code = terrno;
      goto _err;
    }

    if (bytes == 0) break;

    size += bytes;

    if (taosWriteFile(pFileTo, (void *)buffer, bytes) < bytes) {
      code = terrno;
      goto _err;
    }
    if (bytes < sizeof(buffer)) break;
  }

  code = taosFsyncFile(pFileTo);
  if (code != 0) {
    goto _err;
  }

  TAOS_UNUSED(taosCloseFile(&pFileFrom));
  TAOS_UNUSED(taosCloseFile(&pFileTo));

  if (code != 0) {
    terrno = code;
    return -1;
  }

  return size;

_err:

  if (pFileFrom != NULL) TAOS_SKIP_ERROR(taosCloseFile(&pFileFrom));
  if (pFileTo != NULL) TAOS_SKIP_ERROR(taosCloseFile(&pFileTo));
  /* coverity[+retval] */
  TAOS_SKIP_ERROR(taosRemoveFile(to));

  terrno = code;
  return -1;
#endif
}

TdFilePtr taosCreateFile(const char *path, int32_t tdFileOptions) {
  if (path == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  TdFilePtr fp = taosOpenFile(path, tdFileOptions);
  if (!fp) {
    if (terrno == TAOS_SYSTEM_ERROR(ENOENT)) {
      // Try to create directory recursively
      char s[PATH_MAX];
      tstrncpy(s, path, sizeof(s));
      if (taosMulMkDir(taosDirName(s)) != 0) {
        return NULL;
      }
      fp = taosOpenFile(path, tdFileOptions);
      if (!fp) {
        return NULL;
      }
    }
  }

  return fp;
}

int32_t taosRemoveFile(const char *path) {
  OS_PARAM_CHECK(path);
  int32_t code = remove(path);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }
  return code;
}

int32_t taosRenameFile(const char *oldName, const char *newName) {
  OS_PARAM_CHECK(oldName);
  OS_PARAM_CHECK(newName);
#ifdef WINDOWS
  bool finished = false;

  HANDLE transactionHandle = CreateTransaction(NULL, NULL, 0, 0, 0, INFINITE, NULL);
  if (transactionHandle == INVALID_HANDLE_VALUE) {
    DWORD error = GetLastError();
    terrno = TAOS_SYSTEM_WINAPI_ERROR(error);
    return terrno;
  }

  BOOL result = MoveFileTransacted(oldName, newName, NULL, NULL, MOVEFILE_REPLACE_EXISTING, transactionHandle);

  if (result) {
    finished = CommitTransaction(transactionHandle);
    if (!finished) {
      DWORD error = GetLastError();
      terrno = TAOS_SYSTEM_WINAPI_ERROR(error);
    }
  } else {
    RollbackTransaction(transactionHandle);
    DWORD error = GetLastError();
    terrno = TAOS_SYSTEM_WINAPI_ERROR(error);
    finished = false;
  }

  CloseHandle(transactionHandle);

  return finished ? 0 : terrno;
#else
#ifdef TD_ASTRA // TD_ASTRA_TODO
  if (taosCheckExistFile(newName)) taosRemoveFile(newName);
#endif
  int32_t code = rename(oldName, newName);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
#endif
}

int32_t taosStatFile(const char *path, int64_t *size, int64_t *mtime, int64_t *atime) {
  OS_PARAM_CHECK(path);
#ifdef WINDOWS
  struct _stati64 fileStat;
  int32_t         code = _stati64(path, &fileStat);
#else
  struct stat fileStat;
  int32_t     code = stat(path, &fileStat);
#endif
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  if (size != NULL) {
    *size = fileStat.st_size;
  }

  if (mtime != NULL) {
    *mtime = fileStat.st_mtime;
  }

  if (atime != NULL) {
    *atime = fileStat.st_atime;
  }

  return 0;
}

int32_t taosGetFileDiskID(const char *path, int64_t *diskid) {
  OS_PARAM_CHECK(path);
#ifdef WINDOWS
  struct _stati64 fileStat;
  int32_t         code = _stati64(path, &fileStat);
#else
  struct stat fileStat;
  int32_t     code = stat(path, &fileStat);
#endif
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  if (diskid != NULL) {
    *diskid = fileStat.st_dev;
  }

  return 0;
}

int32_t taosDevInoFile(TdFilePtr pFile, int64_t *stDev, int64_t *stIno) {
#ifdef WINDOWS
  if (pFile == NULL || pFile->hFile == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  BY_HANDLE_FILE_INFORMATION bhfi;
  if (GetFileInformationByHandle(pFile->hFile, &bhfi) == FALSE) {
    DWORD error = GetLastError();
    terrno = TAOS_SYSTEM_WINAPI_ERROR(error);
    return terrno;
  }

  if (stDev != NULL) {
    *stDev = (int64_t)(bhfi.dwVolumeSerialNumber);
  }

  if (stIno != NULL) {
    *stIno = (int64_t)((((uint64_t)bhfi.nFileIndexHigh) << 32) + bhfi.nFileIndexLow);
  }

#else
  if (pFile == NULL || pFile->fd < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  struct stat fileStat;
  int32_t     code = fstat(pFile->fd, &fileStat);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  if (stDev != NULL) {
    *stDev = fileStat.st_dev;
  }

  if (stIno != NULL) {
    *stIno = fileStat.st_ino;
  }
#endif

  return 0;
}

FILE *taosOpenFileForStream(const char *path, int32_t tdFileOptions) {
  if (path == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  char *mode = NULL;
  if (tdFileOptions & TD_FILE_APPEND) {
    mode = (tdFileOptions & TD_FILE_TEXT) ? "at+" : "ab+";
  } else if (tdFileOptions & TD_FILE_TRUNC) {
    mode = (tdFileOptions & TD_FILE_TEXT) ? "wt+" : "wb+";
  } else if ((tdFileOptions & TD_FILE_READ) && !(tdFileOptions & TD_FILE_WRITE)) {
    mode = (tdFileOptions & TD_FILE_TEXT) ? "rt" : "rb";
  } else {
    mode = (tdFileOptions & TD_FILE_TEXT) ? "rt+" : "rb+";
  }
  if (tdFileOptions & TD_FILE_EXCL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  FILE *f = fopen(path, mode);
  if (NULL == f) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
  }
  return f;
}

#ifdef WINDOWS
HANDLE taosOpenFileNotStream(const char *path, int32_t tdFileOptions) {
  if (path == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return INVALID_HANDLE_VALUE;
  }
  DWORD openMode = 0;
  DWORD access = 0;
  DWORD fileFlag = FILE_ATTRIBUTE_NORMAL;
  DWORD shareMode = FILE_SHARE_READ;

  openMode = OPEN_EXISTING;
  if (tdFileOptions & TD_FILE_CREATE) {
    openMode = OPEN_ALWAYS;
  } else if (tdFileOptions & TD_FILE_EXCL) {
    openMode = CREATE_NEW;
  } else if ((tdFileOptions & TD_FILE_TRUNC)) {
    openMode = TRUNCATE_EXISTING;
    access |= GENERIC_WRITE;
  }
  if (tdFileOptions & TD_FILE_APPEND) {
    access |= FILE_APPEND_DATA;
  }
  if (tdFileOptions & TD_FILE_WRITE) {
    access |= GENERIC_WRITE;
  }

  shareMode |= FILE_SHARE_WRITE;

  access |= GENERIC_READ;

  if (tdFileOptions & TD_FILE_AUTO_DEL) {
    fileFlag |= FILE_ATTRIBUTE_TEMPORARY;
  }
  if (tdFileOptions & TD_FILE_WRITE_THROUGH) {
    fileFlag |= FILE_FLAG_WRITE_THROUGH;
  }

  HANDLE h = CreateFile(path, access, shareMode, NULL, openMode, fileFlag, NULL);
  if (h != INVALID_HANDLE_VALUE && (tdFileOptions & TD_FILE_APPEND) && (tdFileOptions & TD_FILE_WRITE)) {
    SetFilePointer(h, 0, NULL, FILE_END);
  }
  if (h == INVALID_HANDLE_VALUE) {
    DWORD dwError = GetLastError();
    terrno = TAOS_SYSTEM_WINAPI_ERROR(dwError);
    // LPVOID lpMsgBuf;
    // FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM, NULL, dwError, 0, (LPTSTR)&lpMsgBuf,
    // 0,
    //               NULL);
    // printf("CreateFile failed with error %d: %s", dwError, (char *)lpMsgBuf);
    // LocalFree(lpMsgBuf);
  }
  return h;
}

int64_t taosReadFile(TdFilePtr pFile, void *buf, int64_t count) {
  if (pFile == NULL || buf == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockRdlock(&(pFile->rwlock));
#endif
  if (pFile->hFile == NULL) {
#if FILE_WITH_LOCK
    (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  int64_t res = 0;
  DWORD   bytesRead;
  if (!ReadFile(pFile->hFile, buf, count, &bytesRead, NULL)) {
    DWORD errCode = GetLastError();
    terrno = TAOS_SYSTEM_WINAPI_ERROR(errCode);
    res = -1;
  } else {
    res = bytesRead;
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  return res;
}

int64_t taosWriteFile(TdFilePtr pFile, const void *buf, int64_t count) {
  if (pFile == NULL || pFile->hFile == NULL || buf == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockWrlock(&(pFile->rwlock));
#endif

  DWORD bytesWritten;
  if (!WriteFile(pFile->hFile, buf, count, &bytesWritten, NULL)) {
    SET_ERRNO(GetLastError());
    terrno = TAOS_SYSTEM_WINAPI_ERROR(ERRNO);
    bytesWritten = -1;
  }

#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  return bytesWritten;
}

int64_t taosPWriteFile(TdFilePtr pFile, const void *buf, int64_t count, int64_t offset) {
  if (pFile == NULL || buf == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockWrlock(&(pFile->rwlock));
#endif
  if (pFile->hFile == NULL) {
#if FILE_WITH_LOCK
    (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
    return 0;
  }

  DWORD      ret = 0;
  OVERLAPPED ol = {0};
  ol.OffsetHigh = (uint32_t)((offset & 0xFFFFFFFF00000000LL) >> 0x20);
  ol.Offset = (uint32_t)(offset & 0xFFFFFFFFLL);

  SetLastError(0);
  BOOL result = WriteFile(pFile->hFile, buf, count, &ret, &ol);
  if (!result) {
    SET_ERRNO(GetLastError());
    terrno = TAOS_SYSTEM_WINAPI_ERROR(ERRNO);
    ret = -1;
  }

#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  return ret;
}

int64_t taosLSeekFile(TdFilePtr pFile, int64_t offset, int32_t whence) {
  if (pFile == NULL || pFile->hFile == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockRdlock(&(pFile->rwlock));
#endif

  LARGE_INTEGER liOffset;
  liOffset.QuadPart = offset;
  if (!SetFilePointerEx(pFile->hFile, liOffset, NULL, whence)) {
    SET_ERRNO(GetLastError());
    terrno = TAOS_SYSTEM_WINAPI_ERROR(ERRNO);
    return -1;
  }

  liOffset.QuadPart = 0;
  if (!SetFilePointerEx(pFile->hFile, liOffset, &liOffset, FILE_CURRENT)) {
    SET_ERRNO(GetLastError());
    terrno = TAOS_SYSTEM_WINAPI_ERROR(ERRNO);
    return -1;
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  return liOffset.QuadPart;
}

int32_t taosFStatFile(TdFilePtr pFile, int64_t *size, int64_t *mtime) {
  if (pFile == NULL || pFile->hFile == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  if (size != NULL) {
    LARGE_INTEGER fileSize;
    if (!GetFileSizeEx(pFile->hFile, &fileSize)) {
      SET_ERRNO(GetLastError());
      terrno = TAOS_SYSTEM_WINAPI_ERROR(ERRNO);
      return terrno;  // Error getting file size
    }
    *size = fileSize.QuadPart;
  }

  if (mtime != NULL) {
    FILETIME creationTime, lastAccessTime, lastWriteTime;
    if (!GetFileTime(pFile->hFile, &creationTime, &lastAccessTime, &lastWriteTime)) {
      SET_ERRNO(GetLastError());
      terrno = TAOS_SYSTEM_WINAPI_ERROR(ERRNO);
      return terrno;  // Error getting file time
    }
    // Convert the FILETIME structure to a time_t value
    ULARGE_INTEGER ull;
    ull.LowPart = lastWriteTime.dwLowDateTime;
    ull.HighPart = lastWriteTime.dwHighDateTime;
    *mtime = (int64_t)((ull.QuadPart - 116444736000000000ULL) / 10000000ULL);
  }
  return 0;
}

int32_t taosLockFile(TdFilePtr pFile) {
  if (pFile == NULL || pFile->hFile == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  BOOL          fSuccess = FALSE;
  LARGE_INTEGER fileSize;
  OVERLAPPED    overlapped = {0};

  fSuccess = LockFileEx(pFile->hFile, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                        0,           // reserved
                        ~0,          // number of bytes to lock low
                        ~0,          // number of bytes to lock high
                        &overlapped  // overlapped structure
  );
  if (!fSuccess) {
    return TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
  }
  return 0;
}

int32_t taosUnLockFile(TdFilePtr pFile) {
  if (pFile == NULL || pFile->hFile == NULL) {
    return 0;
  }
  BOOL       fSuccess = FALSE;
  OVERLAPPED overlapped = {0};

  fSuccess = UnlockFileEx(pFile->hFile, 0, ~0, ~0, &overlapped);
  if (!fSuccess) {
    return TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
  }
  return 0;
}

int32_t taosFtruncateFile(TdFilePtr pFile, int64_t l_size) {
  if (pFile == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  if (pFile->hFile == NULL) {
    printf("Ftruncate file error, hFile was null\n");
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  LARGE_INTEGER li_0;
  li_0.QuadPart = (int64_t)0;
  BOOL cur = SetFilePointerEx(pFile->hFile, li_0, NULL, FILE_CURRENT);
  if (!cur) {
    printf("SetFilePointerEx Error getting current position in file.\n");
    terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
    return terrno;
  }

  LARGE_INTEGER li_size;
  li_size.QuadPart = l_size;
  BOOL cur2 = SetFilePointerEx(pFile->hFile, li_size, NULL, FILE_BEGIN);
  if (cur2 == 0) {
    int error = GetLastError();
    switch (error) {
      case ERROR_INVALID_HANDLE:
        SET_ERRNO(EBADF);
        break;
      default:
        SET_ERRNO(EIO);
        break;
    }
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  if (!SetEndOfFile(pFile->hFile)) {
    int error = GetLastError();
    printf("SetEndOfFile GetLastError is:%d", error);
    switch (error) {
      case ERROR_INVALID_HANDLE:
        SET_ERRNO(EBADF);
        break;
      default:
        SET_ERRNO(EIO);
        break;
    }
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }
  return 0;
}

int64_t taosFSendFile(TdFilePtr pFileOut, TdFilePtr pFileIn, int64_t *offset, int64_t size) {
  if (pFileOut == NULL || pFileIn == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
  if (pFileIn->hFile == NULL || pFileOut->hFile == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  LARGE_INTEGER fileOffset;
  fileOffset.QuadPart = *offset;

  if (!SetFilePointerEx(pFileIn->hFile, fileOffset, &fileOffset, FILE_BEGIN)) {
    terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
    return -1;
  }

  int64_t writeLen = 0;
  uint8_t buffer[_SEND_FILE_STEP_] = {0};

  DWORD bytesRead;
  DWORD bytesWritten;
  for (int64_t len = 0; len < (size - _SEND_FILE_STEP_); len += _SEND_FILE_STEP_) {
    if (!ReadFile(pFileIn->hFile, buffer, _SEND_FILE_STEP_, &bytesRead, NULL)) {
      terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
      return writeLen;
    }

    if (bytesRead <= 0) {
      return writeLen;
    } else if (bytesRead < _SEND_FILE_STEP_) {
      if (!WriteFile(pFileOut->hFile, buffer, bytesRead, &bytesWritten, NULL)) {
        terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
        return -1;
      } else {
        return (int64_t)(writeLen + bytesRead);
      }
    } else {
      if (!WriteFile(pFileOut->hFile, buffer, _SEND_FILE_STEP_, &bytesWritten, NULL)) {
        terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
        return -1;
      } else {
        writeLen += _SEND_FILE_STEP_;
      }
    }
  }

  int64_t remain = size - writeLen;
  if (remain > 0) {
    DWORD bytesRead;
    if (!ReadFile(pFileIn->hFile, buffer, (DWORD)remain, &bytesRead, NULL)) {
      terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
      return -1;
    }

    if (bytesRead <= 0) {
      return writeLen;
    } else {
      DWORD bytesWritten;
      if (!WriteFile(pFileOut->hFile, buffer, bytesRead, &bytesWritten, NULL)) {
        terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
        return -1;
      } else {
        writeLen += bytesWritten;
      }
    }
  }
  return writeLen;
}

bool lastErrorIsFileNotExist() {
  DWORD dwError = GetLastError();
  return dwError == ERROR_FILE_NOT_FOUND;
}

#else
int taosOpenFileNotStream(const char *path, int32_t tdFileOptions) {
  if (path == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
  int access = O_BINARY;
  access |= (tdFileOptions & TD_FILE_CREATE) ? O_CREAT : 0;
  if ((tdFileOptions & TD_FILE_WRITE) && (tdFileOptions & TD_FILE_READ)) {
    access |= O_RDWR;
  } else if (tdFileOptions & TD_FILE_WRITE) {
    access |= O_WRONLY;
  } else if (tdFileOptions & TD_FILE_READ) {
    access |= O_RDONLY;
  }
  access |= (tdFileOptions & TD_FILE_TRUNC) ? O_TRUNC : 0;
  access |= (tdFileOptions & TD_FILE_APPEND) ? O_APPEND : 0;
  access |= (tdFileOptions & TD_FILE_TEXT) ? O_TEXT : 0;
  access |= (tdFileOptions & TD_FILE_EXCL) ? O_EXCL : 0;
  access |= (tdFileOptions & TD_FILE_CLOEXEC) ? O_CLOEXEC : 0;

  int fd = open(path, access, S_IRWXU | S_IRWXG | S_IRWXO);
  if (-1 == fd) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
  }
  return fd;
}

int64_t taosReadFile(TdFilePtr pFile, void *buf, int64_t count) {
  if (pFile == NULL || buf == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
  STUB_RAND_IO_ERR(terrno)
#if FILE_WITH_LOCK
  (void)taosThreadRwlockRdlock(&(pFile->rwlock));
#endif

  if (pFile->fd < 0) {
#if FILE_WITH_LOCK
    (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  int64_t leftbytes = count;
  int64_t readbytes;
  char   *tbuf = (char *)buf;
  int32_t code = 0;

  while (leftbytes > 0) {
#ifdef WINDOWS
    readbytes = _read(pFile->fd, (void *)tbuf, (uint32_t)leftbytes);
#else
    readbytes = read(pFile->fd, (void *)tbuf, (uint32_t)leftbytes);
#endif
    if (readbytes < 0) {
      if (ERRNO == EINTR) {
        continue;
      } else {
        code = TAOS_SYSTEM_ERROR(ERRNO);
#if FILE_WITH_LOCK
        (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
        terrno = code;
        return -1;
      }
    } else if (readbytes == 0) {
#if FILE_WITH_LOCK
      (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
      return (int64_t)(count - leftbytes);
    }

    leftbytes -= readbytes;
    tbuf += readbytes;
  }

#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif

  return count;
}

int64_t taosWriteFile(TdFilePtr pFile, const void *buf, int64_t count) {
  STUB_RAND_IO_ERR(terrno)
  if (pFile == NULL || buf == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockWrlock(&(pFile->rwlock));
#endif
  if (pFile->fd < 0) {
#if FILE_WITH_LOCK
    (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }

  int64_t nleft = count;
  int64_t nwritten = 0;
  char   *tbuf = (char *)buf;
  int32_t code = 0;

  while (nleft > 0) {
    nwritten = write(pFile->fd, (void *)tbuf, (uint32_t)nleft);
    if (nwritten < 0) {
      if (ERRNO == EINTR) {
        continue;
      }
      code = TAOS_SYSTEM_ERROR(ERRNO);
#if FILE_WITH_LOCK
      (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
      terrno = code;
      return -1;
    }
    nleft -= nwritten;
    tbuf += nwritten;
  }

#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif

  return count;
}

int64_t taosPWriteFile(TdFilePtr pFile, const void *buf, int64_t count, int64_t offset) {
  STUB_RAND_IO_ERR(terrno)
  if (pFile == NULL || buf == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }

  int32_t code = 0;
#if FILE_WITH_LOCK
  (void)taosThreadRwlockWrlock(&(pFile->rwlock));
#endif

#if FILE_WITH_LOCK
  if (pFile->fd < 0) {
    (void)taosThreadRwlockUnlock(&(pFile->rwlock));
    return 0;
  }
#endif
#ifndef TD_ASTRA
  int64_t ret = pwrite(pFile->fd, buf, count, offset);
  if (-1 == ret) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
  }
#else  // TD_ASTRA_TODO
  int64_t ret = -1;
  int64_t cur = lseek(pFile->fd, 0, SEEK_CUR);
  if (cur < 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    goto _exit;
  }
  if (lseek(pFile->fd, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    goto _exit;
  }
  if ((ret = write(pFile->fd, buf, count)) < 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    goto _exit;
  }
_exit:
  if (cur >= 0 && lseek(pFile->fd, cur, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
  }
#endif
#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif

  if (code) {
    terrno = code;
  }

  return ret;
}

int64_t taosLSeekFile(TdFilePtr pFile, int64_t offset, int32_t whence) {
  if (pFile == NULL || pFile->fd < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockRdlock(&(pFile->rwlock));
#endif

  int32_t code = 0;

  int64_t ret = lseek(pFile->fd, offset, whence);
  if (-1 == ret) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
  }

#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif

  if (code) {
    terrno = code;
    return -1;
  }

  return ret;
}

int32_t taosFStatFile(TdFilePtr pFile, int64_t *size, int64_t *mtime) {
  if (pFile == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  if (pFile->fd < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  struct stat fileStat;
  int32_t     code = fstat(pFile->fd, &fileStat);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  if (size != NULL) {
    *size = fileStat.st_size;
  }

  if (mtime != NULL) {
    *mtime = fileStat.st_mtime;
  }

  return 0;
}

int32_t taosLockFile(TdFilePtr pFile) {
  if (NULL == pFile || pFile->fd < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifndef TD_ASTRA
  int32_t code = (int32_t)flock(pFile->fd, LOCK_EX | LOCK_NB);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }
#else // TD_ASTRA_TODO
  struct flock lock;
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = 0;
  lock.l_len = 0;
  int32_t code = fcntl(pFile->fd, F_SETLK, &lock);
  if (-1 == code) {
    //    terrno = TAOS_SYSTEM_ERROR(ERRNO); // TD_ASTRA_TODO
    //    return terrno;                     // TD_ASTRA_TODO
  }
#endif
  return 0;
}

int32_t taosUnLockFile(TdFilePtr pFile) {
  if (NULL == pFile || pFile->fd < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifndef TD_ASTRA
  int32_t code = (int32_t)flock(pFile->fd, LOCK_UN | LOCK_NB);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }
#else // TD_ASTRA_TODO
  struct flock lock;
  lock.l_type = F_UNLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = 0;
  lock.l_len = 0;
  int32_t code = fcntl(pFile->fd, F_SETLK, &lock);
  if (-1 == code) {
    //    terrno = TAOS_SYSTEM_ERROR(ERRNO);// TD_ASTRA_TODO
    //    return terrno;// TD_ASTRA_TODO
  }
#endif
  return 0;
}

int32_t taosFtruncateFile(TdFilePtr pFile, int64_t l_size) {
  if (NULL == pFile || pFile->fd < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  int32_t code = ftruncate(pFile->fd, l_size);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }
  return 0;
}

int64_t taosFSendFile(TdFilePtr pFileOut, TdFilePtr pFileIn, int64_t *offset, int64_t size) {
  if (pFileOut == NULL || pFileIn == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
  if (pFileIn->fd < 0 || pFileOut->fd < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

#if defined(_TD_DARWIN_64) || defined(TD_ASTRA)  // TD_ASTRA_TODO
  if (lseek(pFileIn->fd, (int32_t)(*offset), 0) < 0) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return -1;
  }
  int64_t writeLen = 0;
  uint8_t buffer[_SEND_FILE_STEP_] = {0};

  for (int64_t len = 0; len < (size - _SEND_FILE_STEP_); len += _SEND_FILE_STEP_) {
    size_t rlen = read(pFileIn->fd, (void *)buffer, _SEND_FILE_STEP_);
    if (rlen <= 0) {
      return writeLen;
    } else if (rlen < _SEND_FILE_STEP_) {
      write(pFileOut->fd, (void *)buffer, (uint32_t)rlen);
      return (int64_t)(writeLen + rlen);
    } else {
      write(pFileOut->fd, (void *)buffer, _SEND_FILE_STEP_);
      writeLen += _SEND_FILE_STEP_;
    }
  }

  int64_t remain = size - writeLen;
  if (remain > 0) {
    size_t rlen = read(pFileIn->fd, (void *)buffer, (size_t)remain);
    if (rlen <= 0) {
      return writeLen;
    } else {
      write(pFileOut->fd, (void *)buffer, (uint32_t)remain);
      writeLen += remain;
    }
  }
  return writeLen;

#else  // for linux

  int64_t leftbytes = size;
  int64_t sentbytes;

  while (leftbytes > 0) {
#ifdef _TD_ARM_32
    sentbytes = sendfile(pFileOut->fd, pFileIn->fd, (long int *)offset, leftbytes);
#else
    sentbytes = sendfile(pFileOut->fd, pFileIn->fd, offset, leftbytes);
#endif
    if (sentbytes == -1) {
      if (ERRNO == EINTR || ERRNO == EAGAIN || ERRNO == EWOULDBLOCK) {
        continue;
      } else {
        terrno = TAOS_SYSTEM_ERROR(ERRNO);
        return -1;
      }
    } else if (sentbytes == 0) {
      return (int64_t)(size - leftbytes);
    }

    leftbytes -= sentbytes;
  }

  return size;
#endif
}

bool lastErrorIsFileNotExist() { return terrno == TAOS_SYSTEM_ERROR(ENOENT); }

#endif  // WINDOWS

TdFilePtr taosOpenFile(const char *path, int32_t tdFileOptions) {
  if (path == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  STUB_RAND_IO_ERR(NULL)
  FILE *fp = NULL;
#ifdef WINDOWS
  HANDLE hFile = NULL;
#else
  int fd = -1;
#endif
  if (tdFileOptions & TD_FILE_STREAM) {
    fp = taosOpenFileForStream(path, tdFileOptions);
    if (fp == NULL) return NULL;
  } else {
#ifdef WINDOWS
    hFile = taosOpenFileNotStream(path, tdFileOptions);
    if (hFile == INVALID_HANDLE_VALUE) return NULL;
#else
    fd = taosOpenFileNotStream(path, tdFileOptions);
    if (fd == -1) return NULL;
#endif
  }

  TdFilePtr pFile = (TdFilePtr)taosMemoryMalloc(sizeof(TdFile));
  if (pFile == NULL) {
#ifdef WINDOWS
    if (hFile != NULL) CloseHandle(hFile);
#else
    if (fd >= 0) (void)close(fd);
#endif
    if (fp != NULL) (void)fclose(fp);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

#if FILE_WITH_LOCK
  (void)taosThreadRwlockInit(&(pFile->rwlock), NULL);
#endif
  pFile->fp = fp;

#ifdef WINDOWS
  pFile->hFile = hFile;
  pFile->tdFileOptions = tdFileOptions;
  // do nothing, since the property of pmode is set with _O_TEMPORARY; the OS will recycle
  // the file handle, as well as the space on disk.
#else
  pFile->fd = fd;
  // Remove it instantly, so when the program exits normally/abnormally, the file
  // will be automatically remove by OS.
  if (tdFileOptions & TD_FILE_AUTO_DEL) {
    if (-1 == unlink(path)) {
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
      (void)close(fd);
      taosMemoryFree(pFile);
      return NULL;
    }
  }
#endif

  return pFile;
}

int32_t taosCloseFile(TdFilePtr *ppFile) {
  int32_t code = 0;
  if (ppFile == NULL || *ppFile == NULL) {
    return 0;
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockWrlock(&((*ppFile)->rwlock));
#endif
  if ((*ppFile)->fp != NULL) {
    TAOS_UNUSED(fflush((*ppFile)->fp));
    TAOS_UNUSED(fclose((*ppFile)->fp));
    (*ppFile)->fp = NULL;
  }
#ifdef WINDOWS
  if ((*ppFile)->hFile != NULL) {
    // FlushFileBuffers((*ppFile)->hFile);
    if (!CloseHandle((*ppFile)->hFile)) {
      terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
      code = -1;
    }
    (*ppFile)->hFile = NULL;
#else
  if ((*ppFile)->fd >= 0) {
    // warning: never fsync silently in base lib
    /*fsync((*ppFile)->fd);*/
    code = close((*ppFile)->fd);
    if (-1 == code) {
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
    }
    (*ppFile)->fd = -1;
#endif
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&((*ppFile)->rwlock));
  (void)taosThreadRwlockDestroy(&((*ppFile)->rwlock));
#endif
  taosMemoryFree(*ppFile);
  *ppFile = NULL;
  return code;
}

int64_t taosPReadFile(TdFilePtr pFile, void *buf, int64_t count, int64_t offset) {
  STUB_RAND_IO_ERR(terrno)
  if (pFile == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  int32_t code = 0;

#ifdef WINDOWS
#if FILE_WITH_LOCK
  (void)taosThreadRwlockRdlock(&(pFile->rwlock));
#endif

  if (pFile->hFile == NULL) {
#if FILE_WITH_LOCK
    (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif

    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  DWORD      ret = 0;
  OVERLAPPED ol = {0};
  ol.OffsetHigh = (uint32_t)((offset & 0xFFFFFFFF00000000LL) >> 0x20);
  ol.Offset = (uint32_t)(offset & 0xFFFFFFFFLL);

  SetLastError(0);
  BOOL result = ReadFile(pFile->hFile, buf, count, &ret, &ol);
  if (!result && GetLastError() != ERROR_HANDLE_EOF) {
    code = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
    ret = -1;
  }
#else
#if FILE_WITH_LOCK
  (void)taosThreadRwlockRdlock(&(pFile->rwlock));
#endif

  if (pFile->fd < 0) {
#if FILE_WITH_LOCK
    (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
#ifndef TD_ASTRA
  int64_t ret = pread(pFile->fd, buf, count, offset);
  if (-1 == ret) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
  }
#else  // TD_ASTRA_TODO
  int64_t ret = -1;
  int64_t cur = lseek(pFile->fd, 0, SEEK_CUR);
  if (cur < 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    goto _exit;
  }
  if (lseek(pFile->fd, offset, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    goto _exit;
  }
  if ((ret = read(pFile->fd, buf, count)) < 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    goto _exit;
  }
_exit:
  if (cur >= 0 && lseek(pFile->fd, cur, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
  }
#endif
#endif
#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif

  if (code) {
    terrno = code;
    return -1;
  }

  return ret;
}

int32_t taosFsyncFile(TdFilePtr pFile) {
  if (pFile == NULL) {
    return 0;
  }

  int32_t code = 0;
  // this implementation is WRONG
  // fflush is not a replacement of fsync
  if (pFile->fp != NULL) {
    code = fflush(pFile->fp);
    if (0 != code) {
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
      return terrno;
    }

    return 0;
  }

#ifdef WINDOWS
  if (pFile->hFile != NULL) {
    if (pFile->tdFileOptions & TD_FILE_WRITE_THROUGH) {
      return 0;
    }
    bool ret = FlushFileBuffers(pFile->hFile);
    if (!ret) {
      terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
      return terrno;
    }
    return 0;
  }
#else
  if (pFile->fd >= 0) {
    code = fsync(pFile->fd);
    if (-1 == code) {
#ifndef TD_ASTRA
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
#else
      terrno = 0;  // TD_ASTRA_TODO
#endif
      return terrno;
    }
  }
#endif

  return 0;
}

void taosFprintfFile(TdFilePtr pFile, const char *format, ...) {
  if (pFile == NULL || pFile->fp == NULL) {
    return;
  }
  va_list ap;
  va_start(ap, format);
  (void)vfprintf(pFile->fp, format, ap);
  va_end(ap);
}

bool taosValidFile(TdFilePtr pFile) {
#ifdef WINDOWS
  return pFile != NULL && pFile->hFile != NULL;
#else
  return pFile != NULL && pFile->fd > 0;
#endif
}

int32_t taosUmaskFile(int32_t maskVal) {
#ifdef WINDOWS
  return 0;
#else
  return umask(maskVal);
#endif
}

int64_t taosGetLineFile(TdFilePtr pFile, char **__restrict ptrBuf) {
  int64_t ret = -1;
  int32_t code = 0;

  if (pFile == NULL || ptrBuf == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    goto END;
  }
  if (*ptrBuf != NULL) {
    taosMemoryFreeClear(*ptrBuf);
  }

  if (pFile->fp == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    goto END;
  }

#if FILE_WITH_LOCK
  (void)taosThreadRwlockRdlock(&(pFile->rwlock));
#endif

#ifdef WINDOWS
  size_t bufferSize = 512;
  *ptrBuf = taosMemoryMalloc(bufferSize);
  if (*ptrBuf == NULL) {
    goto END;
  }

  size_t bytesRead = 0;
  size_t totalBytesRead = 0;

  while (1) {
    char *result = fgets(*ptrBuf + totalBytesRead, bufferSize - totalBytesRead, pFile->fp);
    if (result == NULL) {
      if (feof(pFile->fp)) {
        break;
      } else {
        ret = -1;
        terrno = TAOS_SYSTEM_ERROR(ferror(pFile->fp));
        taosMemoryFreeClear(*ptrBuf);
        goto END;
      }
    }
    bytesRead = strlen(*ptrBuf + totalBytesRead);
    totalBytesRead += bytesRead;

    if (totalBytesRead < bufferSize - 1 || (*ptrBuf)[totalBytesRead - 1] == '\n') {
      break;
    }

    bufferSize += 512;
    void *newBuf = taosMemoryRealloc(*ptrBuf, bufferSize);
    if (newBuf == NULL) {
      taosMemoryFreeClear(*ptrBuf);
      goto END;
    }

    *ptrBuf = newBuf;
  }

  (*ptrBuf)[totalBytesRead] = '\0';
  ret = (totalBytesRead > 0 ? totalBytesRead : -1); // -1 means EOF
#elif defined(TD_ASTRA)
  size_t bufsize = 128;
  if (*ptrBuf == NULL) {
    *ptrBuf = (char *)taosMemoryMalloc(bufsize);
    if (*ptrBuf == NULL) {
      goto END;
    }
  }
  size_t pos = 0;
  int    c;
  while ((c = fgetc(pFile->fp)) != EOF) {
    if (pos + 1 >= bufsize) {
      size_t new_size = bufsize << 1;
      char  *new_line = (char *)taosMemoryRealloc(*ptrBuf, new_size);
      if (new_line == NULL) {
        goto END;
      }
      *ptrBuf = new_line;
      bufsize = new_size;
    }
    (*ptrBuf)[pos++] = (char)c;
    if (c == '\n') {
      break;
    }
  }
  if (pos == 0 && c == EOF) {
    goto END;
  }
  (*ptrBuf)[pos] = '\0';
  ret = pos;
#else
  size_t len = 0;
  ret = getline(ptrBuf, &len, pFile->fp);
  if (-1 == ret) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
  }
#endif

END:

#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif

  return ret;
}

int64_t taosGetsFile(TdFilePtr pFile, int32_t maxSize, char *__restrict buf) {
  if (pFile == NULL || buf == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  if (pFile->fp == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  if (fgets(buf, maxSize, pFile->fp) == NULL) {
    if (feof(pFile->fp)) {
      return 0;
    } else {
      terrno = TAOS_SYSTEM_ERROR(ferror(pFile->fp));
      return terrno;
    }
  }

  return strlen(buf);
}

int32_t taosEOFFile(TdFilePtr pFile) {
  if (pFile == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
  if (pFile->fp == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  return feof(pFile->fp);
}

bool taosCheckAccessFile(const char *pathname, int32_t tdFileAccessOptions) {
  if (pathname == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return false;  // invalid parameter
  }
  int flags = 0;

  if (tdFileAccessOptions & TD_FILE_ACCESS_EXIST_OK) {
    flags |= F_OK;
  }

  if (tdFileAccessOptions & TD_FILE_ACCESS_READ_OK) {
    flags |= R_OK;
  }

  if (tdFileAccessOptions & TD_FILE_ACCESS_WRITE_OK) {
    flags |= W_OK;
  }

  if (tdFileAccessOptions & TD_FILE_ACCESS_EXEC_OK) {
    flags |= X_OK;
  }
#ifdef WINDOWS
  return _access(pathname, flags) == 0;
#else
  return access(pathname, flags) == 0;
#endif
}

bool taosCheckExistFile(const char *pathname) { return taosCheckAccessFile(pathname, TD_FILE_ACCESS_EXIST_OK); };

int32_t taosCompressFile(char *srcFileName, char *destFileName) {
  OS_PARAM_CHECK(srcFileName);
  OS_PARAM_CHECK(destFileName);
  int32_t   compressSize = 163840;
  int32_t   ret = 0;
  int32_t   len = 0;
  gzFile    dstFp = NULL;
  TdFilePtr pSrcFile = NULL;

  char *data = taosMemoryMalloc(compressSize);
  if (NULL == data) {
    return terrno;
  }

  pSrcFile = taosOpenFile(srcFileName, TD_FILE_READ | TD_FILE_STREAM);
  if (pSrcFile == NULL) {
    ret = terrno;
    goto cmp_end;
  }

  int access = O_BINARY | O_WRONLY | O_TRUNC | O_CREAT;
#ifdef WINDOWS
  int32_t pmode = _S_IREAD | _S_IWRITE;
#else
  int32_t pmode = S_IRWXU | S_IRWXG | S_IRWXO;
#endif
  int fd = open(destFileName, access, pmode);
  if (-1 == fd) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    ret = terrno;
    goto cmp_end;
  }

  // Both gzclose() and fclose() will close the associated fd, so they need to have different fds.
  FileFd gzFd = dup(fd);
  if (-1 == gzFd) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    ret = terrno;
    goto cmp_end;
  }
  dstFp = gzdopen(gzFd, "wb6f");
  if (dstFp == NULL) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    ret = terrno;
    (void)close(gzFd);
    goto cmp_end;
  }

  while (!feof(pSrcFile->fp)) {
    len = (int32_t)fread(data, 1, compressSize, pSrcFile->fp);
    if (len > 0) {
      if (gzwrite(dstFp, data, len) == 0) {
        terrno = TAOS_SYSTEM_ERROR(ERRNO);
        ret = terrno;
        goto cmp_end;
      }
    }
  }

cmp_end:

  if (fd >= 0) {
    TAOS_SKIP_ERROR(close(fd));
  }
  if (pSrcFile) {
    TAOS_SKIP_ERROR(taosCloseFile(&pSrcFile));
  }

  if (dstFp) {
    TAOS_SKIP_ERROR(gzclose(dstFp));
  }

  taosMemoryFree(data);

  return ret;
}

int32_t taosSetFileHandlesLimit() {
#ifdef WINDOWS
  const int max_handles = 8192;
  int       res = _setmaxstdio(max_handles);
  return res == max_handles ? 0 : -1;
#endif
  return 0;
}

int32_t taosLinkFile(char *src, char *dst) {
#ifndef WINDOWS
  if (-1 == link(src, dst)) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }
#endif
  return 0;
}

FILE *taosOpenCFile(const char *filename, const char *mode) {
  if (filename == NULL || mode == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  STUB_RAND_IO_ERR(NULL)
  FILE *f = fopen(filename, mode);
  if (NULL == f) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
  }
  return f;
}

int taosSeekCFile(FILE *file, int64_t offset, int whence) {
  if (NULL == file) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifdef WINDOWS
  return _fseeki64(file, offset, whence);
#else
  int     code = fseeko(file, offset, whence);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    code = terrno;
  }
  return code;
#endif
}

size_t taosReadFromCFile(void *buffer, size_t size, size_t count, FILE *stream) {
  if (buffer == NULL || stream == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
  STUB_RAND_IO_ERR(terrno)
  return fread(buffer, size, count, stream);
}

#if 0
size_t taosWriteToCFile(const void *ptr, size_t size, size_t nitems, FILE *stream) {
  STUB_RAND_IO_ERR(terrno)
  return fwrite(ptr, size, nitems, stream);
}
#endif

int taosCloseCFile(FILE *f) { return fclose(f); }

int taosSetAutoDelFile(char *path) {
#ifdef WINDOWS
  bool succ = SetFileAttributes(path, FILE_ATTRIBUTE_TEMPORARY);
  if (succ) {
    return 0;
  } else {
    terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
    return terrno;
  }
#else
  if (-1 == unlink(path)) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }
  return 0;
#endif
}

int64_t taosWritevFile(TdFilePtr pFile, const TaosIOVec *iov, int iovcnt) {
  if (pFile == NULL || iov == NULL || iovcnt <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockWrlock(&(pFile->rwlock));
#endif

#ifdef __linux__
  if (pFile->fd < 0) {
#if FILE_WITH_LOCK
    (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
  ssize_t written = writev(pFile->fd, iov, iovcnt);
  if (written < 0) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  return (int64_t)written;
#elif defined(WINDOWS)
  if (pFile == NULL || pFile->hFile == NULL) {
#if FILE_WITH_LOCK
    (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
  int64_t totalWritten = 0;
  for (int i = 0; i < iovcnt; ++i) {
    TaosIOVec *vec = &iov[i];
    if (vec->iov_len <= 0) continue;
    DWORD written;
    if (!WriteFile(pFile->hFile, vec->iov_base, vec->iov_len, &written, NULL)) {
      SET_ERRNO(GetLastError());
      terrno = TAOS_SYSTEM_WINAPI_ERROR(ERRNO);
      written = -1;
      break;
    }
    totalWritten += written;
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  if (totalWritten < 0) {
    return -1;
  }
  return (int64_t)totalWritten;
#else
  if (pFile->fd < 0) {
#if FILE_WITH_LOCK
    (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }
  int64_t totalWritten = 0;
  for (int i = 0; i < iovcnt; ++i) {
    const struct iovec *vec = &iov[i];
    if (vec->iov_len <= 0) continue;
    ssize_t written = write(pFile->fd, vec->iov_base, vec->iov_len);
    if (written < 0) {
      if (ERRNO == EINTR || ERRNO == EAGAIN || ERRNO == EWOULDBLOCK) {
        continue;
      } else {
        terrno = TAOS_SYSTEM_ERROR(ERRNO);
        totalWritten = -1;
        break;
      }
    }
    totalWritten += written;
  }
#if FILE_WITH_LOCK
  (void)taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  if (totalWritten < 0) {
    return -1;
  }
  return (int64_t)totalWritten;
#endif
}
