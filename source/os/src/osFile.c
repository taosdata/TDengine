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
#include "zlib.h"

#ifdef WINDOWS
#include <io.h>
#include <WinBase.h>
#include <ktmw32.h>
#define F_OK 0
#define W_OK 2
#define R_OK 4

#define _SEND_FILE_STEP_ 1000

#else
#include <fcntl.h>
#include <sys/file.h>

#if !defined(_TD_DARWIN_64)
#include <sys/sendfile.h>
#endif
#include <sys/stat.h>
#include <unistd.h>
#define LINUX_FILE_NO_TEXT_OPTION 0
#define O_TEXT                    LINUX_FILE_NO_TEXT_OPTION

#define _SEND_FILE_STEP_ 1000
#endif

typedef int32_t FileFd;

typedef struct TdFile {
  TdThreadRwlock rwlock;
  int            refId;
  FileFd         fd;
  FILE          *fp;
} TdFile;

#define FILE_WITH_LOCK 1

void taosGetTmpfilePath(const char *inputTmpDir, const char *fileNamePrefix, char *dstPath) {
#ifdef WINDOWS
  const char *tdengineTmpFileNamePrefix = "tdengine-";
  char        tmpPath[PATH_MAX];

  int32_t len = (int32_t)strlen(inputTmpDir);
  memcpy(tmpPath, inputTmpDir, len);

  if (tmpPath[len - 1] != '/' && tmpPath[len - 1] != '\\') {
    tmpPath[len++] = '\\';
  }

  strcpy(tmpPath + len, tdengineTmpFileNamePrefix);
  if (strlen(tmpPath) + strlen(fileNamePrefix) + strlen("-%d-%s") < PATH_MAX) {
    strcat(tmpPath, fileNamePrefix);
    strcat(tmpPath, "-%d-%s");
  }

  char rand[8] = {0};
  taosRandStr(rand, tListLen(rand) - 1);
  snprintf(dstPath, PATH_MAX, tmpPath, getpid(), rand);

#else

  const char *tdengineTmpFileNamePrefix = "tdengine-";

  char    tmpPath[PATH_MAX];
  int32_t len = strlen(inputTmpDir);
  memcpy(tmpPath, inputTmpDir, len);
  static uint64_t seqId = 0;

  if (tmpPath[len - 1] != '/') {
    tmpPath[len++] = '/';
  }

  strcpy(tmpPath + len, tdengineTmpFileNamePrefix);
  if (strlen(tmpPath) + strlen(fileNamePrefix) + strlen("-%d-%s") < PATH_MAX) {
    strcat(tmpPath, fileNamePrefix);
    strcat(tmpPath, "-%d-%s");
  }

  char rand[32] = {0};

  sprintf(rand, "%" PRIu64, atomic_add_fetch_64(&seqId, 1));

  snprintf(dstPath, PATH_MAX, tmpPath, getpid(), rand);

#endif
}

int64_t taosCopyFile(const char *from, const char *to) {
#ifdef WINDOWS
  if (CopyFile(from, to, 0)) {
    return 1;
  } else {
    return -1;
  }
#else
  char    buffer[4096];
  int64_t size = 0;
  int64_t bytes;

  // fidfrom = open(from, O_RDONLY);
  TdFilePtr pFileFrom = taosOpenFile(from, TD_FILE_READ);
  if (pFileFrom == NULL) goto _err;

  // fidto = open(to, O_WRONLY | O_CREAT | O_EXCL, 0755);
  TdFilePtr pFileTo = taosOpenFile(to, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_EXCL);
  if (pFileTo == NULL) goto _err;

  while (true) {
    bytes = taosReadFile(pFileFrom, buffer, sizeof(buffer));
    if (bytes < 0) goto _err;
    if (bytes == 0) break;

    size += bytes;

    if (taosWriteFile(pFileTo, (void *)buffer, bytes) < bytes) goto _err;
    if (bytes < sizeof(buffer)) break;
  }

  int code = taosFsyncFile(pFileTo);

  taosCloseFile(&pFileFrom);
  taosCloseFile(&pFileTo);

  if (code != 0) {
    return -1;
  }
  return size;

_err:
  if (pFileFrom != NULL) taosCloseFile(&pFileFrom);
  if (pFileTo != NULL) taosCloseFile(&pFileTo);
  /* coverity[+retval] */
  taosRemoveFile(to);
  return -1;
#endif
}

TdFilePtr taosCreateFile(const char *path, int32_t tdFileOptions) {
  TdFilePtr fp = taosOpenFile(path, tdFileOptions);
  if (!fp) {
    if (errno == ENOENT) {
      // Try to create directory recursively
      char *s = taosStrdup(path);
      if (taosMulMkDir(taosDirName(s)) != 0) {
        taosMemoryFree(s);
        return NULL;
      }
      taosMemoryFree(s);
      fp = taosOpenFile(path, tdFileOptions);
      if (!fp) {
        return NULL;
      }
    }
  }
  return fp;
}

int32_t taosRemoveFile(const char *path) { return remove(path); }

int32_t taosRenameFile(const char *oldName, const char *newName) {
#ifdef WINDOWS
  bool finished = false;

  HANDLE transactionHandle = CreateTransaction(NULL, NULL, 0, 0, 0, INFINITE, NULL);
  if (transactionHandle == INVALID_HANDLE_VALUE) {
    printf("failed to rename file %s to %s, reason: CreateTransaction failed.\n", oldName, newName);
    return -1;
  }

  BOOL result = MoveFileTransacted(oldName, newName, NULL, NULL, MOVEFILE_REPLACE_EXISTING, transactionHandle);

  if (result) {
    finished = CommitTransaction(transactionHandle);
    if (!finished) {
      DWORD error = GetLastError();
      printf("failed to rename file %s to %s, reason: CommitTransaction errcode %d.\n", oldName, newName, error);
    }
  } else {
    RollbackTransaction(transactionHandle);
    DWORD error = GetLastError();
    finished = false;
    printf("failed to rename file %s to %s, reason: MoveFileTransacted errcode %d.\n", oldName, newName, error);
  }

  CloseHandle(transactionHandle);

  return finished ? 0 : -1;
#else
  int32_t code = rename(oldName, newName);
  if (code < 0) {
    printf("failed to rename file %s to %s, reason:%s\n", oldName, newName, strerror(errno));
  }

  return code;
#endif
}

int32_t taosStatFile(const char *path, int64_t *size, int32_t *mtime, int32_t *atime) {
#ifdef WINDOWS
  struct _stati64 fileStat;
  int32_t         code = _stati64(path, &fileStat);
#else
  struct stat fileStat;
  int32_t code = stat(path, &fileStat);
#endif
  if (code < 0) {
    return code;
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
int32_t taosDevInoFile(TdFilePtr pFile, int64_t *stDev, int64_t *stIno) {
  if (pFile == NULL || pFile->fd < 0) {
    return -1;
  }

#ifdef WINDOWS

  BY_HANDLE_FILE_INFORMATION bhfi;
  HANDLE                     handle = (HANDLE)_get_osfhandle(pFile->fd);
  if (GetFileInformationByHandle(handle, &bhfi) == FALSE) {
    printf("taosFStatFile get file info fail.");
    return -1;
  }

  if (stDev != NULL) {
    *stDev = (int64_t)(bhfi.dwVolumeSerialNumber);
  }

  if (stIno != NULL) {
    *stIno = (int64_t)((((uint64_t)bhfi.nFileIndexHigh) << 32) + bhfi.nFileIndexLow);
  }

#else

  struct stat fileStat;
  int32_t code = fstat(pFile->fd, &fileStat);
  if (code < 0) {
    printf("taosFStatFile run fstat fail.");
    return code;
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

TdFilePtr taosOpenFile(const char *path, int32_t tdFileOptions) {
  int   fd = -1;
  FILE *fp = NULL;
  if (tdFileOptions & TD_FILE_STREAM) {
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
    ASSERT(!(tdFileOptions & TD_FILE_EXCL));
    if (tdFileOptions & TD_FILE_EXCL) {
      return NULL;
    }
    fp = fopen(path, mode);
    if (fp == NULL) {
      return NULL;
    }
  } else {
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
#ifdef WINDOWS
    int32_t pmode = _S_IREAD | _S_IWRITE;
    if (tdFileOptions & TD_FILE_AUTO_DEL) {
      pmode |= _O_TEMPORARY;
    }
    fd = _open(path, access, pmode);
#else
    fd = open(path, access, S_IRWXU | S_IRWXG | S_IRWXO);
#endif
    if (fd == -1) {
      return NULL;
    }
  }

  TdFilePtr pFile = (TdFilePtr)taosMemoryMalloc(sizeof(TdFile));
  if (pFile == NULL) {
    if (fd >= 0) close(fd);
    if (fp != NULL) fclose(fp);
    return NULL;
  }

#if FILE_WITH_LOCK
  taosThreadRwlockInit(&(pFile->rwlock), NULL);
#endif
  pFile->fd = fd;
  pFile->fp = fp;
  pFile->refId = 0;

  if (tdFileOptions & TD_FILE_AUTO_DEL) {
#ifdef WINDOWS
    // do nothing, since the property of pmode is set with _O_TEMPORARY; the OS will recycle
    // the file handle, as well as the space on disk.
#else
    // Remove it instantly, so when the program exits normally/abnormally, the file
    // will be automatically remove by OS.
    unlink(path);
#endif
  }

  return pFile;
}

int32_t taosCloseFile(TdFilePtr *ppFile) {
  int32_t code = 0;
  if (ppFile == NULL || *ppFile == NULL) {
    return 0;
  }
#if FILE_WITH_LOCK
  taosThreadRwlockWrlock(&((*ppFile)->rwlock));
#endif
  if ((*ppFile)->fp != NULL) {
    fflush((*ppFile)->fp);
    fclose((*ppFile)->fp);
    (*ppFile)->fp = NULL;
  }
  if ((*ppFile)->fd >= 0) {
#ifdef WINDOWS
    HANDLE h = (HANDLE)_get_osfhandle((*ppFile)->fd);
    !FlushFileBuffers(h);
#else
    // warning: never fsync silently in base lib
    /*fsync((*ppFile)->fd);*/
#endif
    code = close((*ppFile)->fd);
    (*ppFile)->fd = -1;
  }
  (*ppFile)->refId = 0;
#if FILE_WITH_LOCK
  taosThreadRwlockUnlock(&((*ppFile)->rwlock));
  taosThreadRwlockDestroy(&((*ppFile)->rwlock));
#endif
  taosMemoryFree(*ppFile);
  *ppFile = NULL;
  return code;
}

int64_t taosReadFile(TdFilePtr pFile, void *buf, int64_t count) {
#if FILE_WITH_LOCK
  taosThreadRwlockRdlock(&(pFile->rwlock));
#endif
  ASSERT(pFile->fd >= 0);  // Please check if you have closed the file.
  if (pFile->fd < 0) {
#if FILE_WITH_LOCK
    taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
    return -1;
  }
  int64_t leftbytes = count;
  int64_t readbytes;
  char   *tbuf = (char *)buf;

  while (leftbytes > 0) {
#ifdef WINDOWS
    readbytes = _read(pFile->fd, (void *)tbuf, (uint32_t)leftbytes);
#else
    readbytes = read(pFile->fd, (void *)tbuf, (uint32_t)leftbytes);
#endif
    if (readbytes < 0) {
      if (errno == EINTR) {
        continue;
      } else {
#if FILE_WITH_LOCK
        taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
        return -1;
      }
    } else if (readbytes == 0) {
#if FILE_WITH_LOCK
      taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
      return (int64_t)(count - leftbytes);
    }

    leftbytes -= readbytes;
    tbuf += readbytes;
  }

#if FILE_WITH_LOCK
  taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  return count;
}

int64_t taosPReadFile(TdFilePtr pFile, void *buf, int64_t count, int64_t offset) {
  if (pFile == NULL) {
    return 0;
  }
#if FILE_WITH_LOCK
  taosThreadRwlockRdlock(&(pFile->rwlock));
#endif
  ASSERT(pFile->fd >= 0);  // Please check if you have closed the file.
  if (pFile->fd < 0) {
#if FILE_WITH_LOCK
    taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
    return -1;
  }
#ifdef WINDOWS
  size_t pos = _lseeki64(pFile->fd, 0, SEEK_CUR);
  _lseeki64(pFile->fd, offset, SEEK_SET);
  int64_t ret = _read(pFile->fd, buf, count);
  _lseeki64(pFile->fd, pos, SEEK_SET);
#else
  int64_t ret = pread(pFile->fd, buf, count, offset);
#endif
#if FILE_WITH_LOCK
  taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  return ret;
}

int64_t taosWriteFile(TdFilePtr pFile, const void *buf, int64_t count) {
  if (pFile == NULL) {
    return 0;
  }
#if FILE_WITH_LOCK
  taosThreadRwlockWrlock(&(pFile->rwlock));
#endif
  if (pFile->fd < 0) {
#if FILE_WITH_LOCK
    taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
    return 0;
  }

  int64_t nleft = count;
  int64_t nwritten = 0;
  char   *tbuf = (char *)buf;

  while (nleft > 0) {
    nwritten = write(pFile->fd, (void *)tbuf, (uint32_t)nleft);
    if (nwritten < 0) {
      if (errno == EINTR) {
        continue;
      }
#if FILE_WITH_LOCK
      taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
      return -1;
    }
    nleft -= nwritten;
    tbuf += nwritten;
  }

#if FILE_WITH_LOCK
  taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  return count;
}

int64_t taosPWriteFile(TdFilePtr pFile, const void *buf, int64_t count, int64_t offset) {
  if (pFile == NULL) {
    return 0;
  }
#if FILE_WITH_LOCK
  taosThreadRwlockWrlock(&(pFile->rwlock));
#endif
  ASSERT(pFile->fd >= 0);  // Please check if you have closed the file.
  if (pFile->fd < 0) {
#if FILE_WITH_LOCK
    taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
    return 0;
  }
#ifdef WINDOWS
  size_t pos = _lseeki64(pFile->fd, 0, SEEK_CUR);
  _lseeki64(pFile->fd, offset, SEEK_SET);
  int64_t ret = _write(pFile->fd, buf, count);
  _lseeki64(pFile->fd, pos, SEEK_SET);
#else
  int64_t ret = pwrite(pFile->fd, buf, count, offset);
#endif
#if FILE_WITH_LOCK
  taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  return ret;
}

int64_t taosLSeekFile(TdFilePtr pFile, int64_t offset, int32_t whence) {
  if (pFile == NULL || pFile->fd < 0) {
    return -1;
  }
#if FILE_WITH_LOCK
  taosThreadRwlockRdlock(&(pFile->rwlock));
#endif
  ASSERT(pFile->fd >= 0);  // Please check if you have closed the file.
#ifdef WINDOWS
  int64_t ret = _lseeki64(pFile->fd, offset, whence);
#else
  int64_t ret = lseek(pFile->fd, offset, whence);
#endif
#if FILE_WITH_LOCK
  taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  return ret;
}

int32_t taosFStatFile(TdFilePtr pFile, int64_t *size, int32_t *mtime) {
  if (pFile == NULL) {
    return 0;
  }
  ASSERT(pFile->fd >= 0);  // Please check if you have closed the file.
  if (pFile->fd < 0) {
    return -1;
  }

#ifdef WINDOWS
  struct __stat64 fileStat;
  int32_t         code = _fstat64(pFile->fd, &fileStat);
#else
  struct stat fileStat;
  int32_t code = fstat(pFile->fd, &fileStat);
#endif
  if (code < 0) {
    return code;
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
  ASSERT(pFile->fd >= 0);  // Please check if you have closed the file.
  if (pFile->fd < 0) {
    return -1;
  }
#ifdef WINDOWS
  BOOL          fSuccess = FALSE;
  LARGE_INTEGER fileSize;
  OVERLAPPED    overlapped = {0};

  HANDLE hFile = (HANDLE)_get_osfhandle(pFile->fd);

  fSuccess = LockFileEx(hFile, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                        0,           // reserved
                        ~0,          // number of bytes to lock low
                        ~0,          // number of bytes to lock high
                        &overlapped  // overlapped structure
  );
  if (!fSuccess) {
    return GetLastError();
  }
  return 0;
#else
  return (int32_t)flock(pFile->fd, LOCK_EX | LOCK_NB);
#endif
}

int32_t taosUnLockFile(TdFilePtr pFile) {
  ASSERT(pFile->fd >= 0);
  if (pFile->fd < 0) {
    return 0;
  }
#ifdef WINDOWS
  BOOL       fSuccess = FALSE;
  OVERLAPPED overlapped = {0};
  HANDLE     hFile = (HANDLE)_get_osfhandle(pFile->fd);

  fSuccess = UnlockFileEx(hFile, 0, ~0, ~0, &overlapped);
  if (!fSuccess) {
    return GetLastError();
  }
  return 0;
#else
  return (int32_t)flock(pFile->fd, LOCK_UN | LOCK_NB);
#endif
}

int32_t taosFtruncateFile(TdFilePtr pFile, int64_t l_size) {
  if (pFile == NULL) {
    return 0;
  }
  if (pFile->fd < 0) {
    printf("Ftruncate file error, fd arg was negative\n");
    return -1;
  }
#ifdef WINDOWS

  HANDLE h = (HANDLE)_get_osfhandle(pFile->fd);

  LARGE_INTEGER li_0;
  li_0.QuadPart = (int64_t)0;
  BOOL cur = SetFilePointerEx(h, li_0, NULL, FILE_CURRENT);
  if (!cur) {
    printf("SetFilePointerEx Error getting current position in file.\n");
    return -1;
  }

  LARGE_INTEGER li_size;
  li_size.QuadPart = l_size;
  BOOL cur2 = SetFilePointerEx(h, li_size, NULL, FILE_BEGIN);
  if (cur2 == 0) {
    int error = GetLastError();
    printf("SetFilePointerEx GetLastError is: %d\n", error);
    switch (error) {
      case ERROR_INVALID_HANDLE:
        errno = EBADF;
        break;
      default:
        errno = EIO;
        break;
    }
    return -1;
  }

  if (!SetEndOfFile(h)) {
    int error = GetLastError();
    printf("SetEndOfFile GetLastError is:%d", error);
    switch (error) {
      case ERROR_INVALID_HANDLE:
        errno = EBADF;
        break;
      default:
        errno = EIO;
        break;
    }
    return -1;
  }

  return 0;
#else
  return ftruncate(pFile->fd, l_size);
#endif
}

int32_t taosFsyncFile(TdFilePtr pFile) {
  if (pFile == NULL) {
    return 0;
  }

  // this implementation is WRONG
  // fflush is not a replacement of fsync
  if (pFile->fp != NULL) return fflush(pFile->fp);
  if (pFile->fd >= 0) {
#ifdef WINDOWS
    HANDLE h = (HANDLE)_get_osfhandle(pFile->fd);
    return !FlushFileBuffers(h);
#else
    return fsync(pFile->fd);
#endif
  }
  return 0;
}

int64_t taosFSendFile(TdFilePtr pFileOut, TdFilePtr pFileIn, int64_t *offset, int64_t size) {
  if (pFileOut == NULL || pFileIn == NULL) {
    return 0;
  }
  ASSERT(pFileIn->fd >= 0 && pFileOut->fd >= 0);
  if (pFileIn->fd < 0 || pFileOut->fd < 0) {
    return 0;
  }

#ifdef WINDOWS

  _lseeki64(pFileIn->fd, *offset, 0);
  int64_t writeLen = 0;
  uint8_t buffer[_SEND_FILE_STEP_] = {0};

  for (int64_t len = 0; len < (size - _SEND_FILE_STEP_); len += _SEND_FILE_STEP_) {
    size_t rlen = _read(pFileIn->fd, (void *)buffer, _SEND_FILE_STEP_);
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
    size_t rlen = _read(pFileIn->fd, (void *)buffer, (size_t)remain);
    if (rlen <= 0) {
      return writeLen;
    } else {
      write(pFileOut->fd, (void *)buffer, (uint32_t)remain);
      writeLen += remain;
    }
  }
  return writeLen;

#elif defined(_TD_DARWIN_64)

  lseek(pFileIn->fd, (int32_t)(*offset), 0);
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

#else

  int64_t leftbytes = size;
  int64_t sentbytes;

  while (leftbytes > 0) {
#ifdef _TD_ARM_32
    sentbytes = sendfile(pFileOut->fd, pFileIn->fd, (long int *)offset, leftbytes);
#else
    sentbytes = sendfile(pFileOut->fd, pFileIn->fd, offset, leftbytes);
#endif
    if (sentbytes == -1) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      } else {
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

void taosFprintfFile(TdFilePtr pFile, const char *format, ...) {
  if (pFile == NULL || pFile->fp == NULL) {
    return;
  }
  va_list ap;
  va_start(ap, format);
  vfprintf(pFile->fp, format, ap);
  va_end(ap);
}

bool taosValidFile(TdFilePtr pFile) { return pFile != NULL && pFile->fd > 0; }

int32_t taosUmaskFile(int32_t maskVal) {
#ifdef WINDOWS
  return 0;
#else
  return umask(maskVal);
#endif
}

int32_t taosGetErrorFile(TdFilePtr pFile) { return errno; }
int64_t taosGetLineFile(TdFilePtr pFile, char **__restrict ptrBuf) {
  if (pFile == NULL || ptrBuf == NULL) {
    return -1;
  }
  if (*ptrBuf != NULL) {
    taosMemoryFreeClear(*ptrBuf);
  }
  ASSERT(pFile->fp != NULL);
  if (pFile->fp == NULL) {
    return -1;
  }
#ifdef WINDOWS
  size_t bufferSize = 512;
  *ptrBuf = taosMemoryMalloc(bufferSize);
  if (*ptrBuf == NULL) return -1;

  size_t bytesRead = 0;
  size_t totalBytesRead = 0;

  while (1) {
    char *result = fgets(*ptrBuf + totalBytesRead, bufferSize - totalBytesRead, pFile->fp);
    if (result == NULL) {
      taosMemoryFreeClear(*ptrBuf);
      return -1;
    }
    bytesRead = strlen(*ptrBuf + totalBytesRead);
    totalBytesRead += bytesRead;

    if (totalBytesRead < bufferSize - 1 || (*ptrBuf)[totalBytesRead - 1] == '\n') {
      break;
    }

    bufferSize += 512;
    void* newBuf = taosMemoryRealloc(*ptrBuf, bufferSize);
    if (newBuf == NULL) {
      taosMemoryFreeClear(*ptrBuf);
      return -1;
    }

    *ptrBuf = newBuf;
  }

  (*ptrBuf)[totalBytesRead] = '\0';
  return totalBytesRead;
#else
  size_t len = 0;
  return getline(ptrBuf, &len, pFile->fp);
#endif
}

int64_t taosGetsFile(TdFilePtr pFile, int32_t maxSize, char *__restrict buf) {
  if (pFile == NULL || buf == NULL) {
    return -1;
  }
  ASSERT(pFile->fp != NULL);
  if (pFile->fp == NULL) {
    return -1;
  }
  if (fgets(buf, maxSize, pFile->fp) == NULL) {
    return -1;
  }
  return strlen(buf);
}

int32_t taosEOFFile(TdFilePtr pFile) {
  if (pFile == NULL) {
    return -1;
  }
  ASSERT(pFile->fp != NULL);
  if (pFile->fp == NULL) {
    return -1;
  }

  return feof(pFile->fp);
}

bool taosCheckAccessFile(const char *pathname, int32_t tdFileAccessOptions) {
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
#ifdef WINDOWS
  return _access(pathname, flags) == 0;
#else
  return access(pathname, flags) == 0;
#endif
}

bool taosCheckExistFile(const char *pathname) { return taosCheckAccessFile(pathname, TD_FILE_ACCESS_EXIST_OK); };

int32_t taosCompressFile(char *srcFileName, char *destFileName) {
  int32_t compressSize = 163840;
  int32_t ret = 0;
  int32_t len = 0;
  char   *data = taosMemoryMalloc(compressSize);
  gzFile  dstFp = NULL;

  TdFilePtr pFile = NULL;
  TdFilePtr pSrcFile = NULL;

  pSrcFile = taosOpenFile(srcFileName, TD_FILE_READ | TD_FILE_STREAM);
  if (pSrcFile == NULL) {
    ret = -1;
    goto cmp_end;
  }

  pFile = taosOpenFile(destFileName, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    ret = -2;
    goto cmp_end;
  }

  // Both gzclose() and fclose() will close the associated fd, so they need to have different fds.
  FileFd gzFd = dup(pFile->fd);
  if (gzFd < 0) {
    ret = -4;
    goto cmp_end;
  }
  dstFp = gzdopen(gzFd, "wb6f");
  if (dstFp == NULL) {
    ret = -3;
    close(gzFd);
    goto cmp_end;
  }

  while (!feof(pSrcFile->fp)) {
    len = (int32_t)fread(data, 1, compressSize, pSrcFile->fp);
    (void)gzwrite(dstFp, data, len);
  }

cmp_end:
  if (pFile) {
     taosCloseFile(&pFile);
  }
  if (pSrcFile) {
    taosCloseFile(&pSrcFile);
  }

  if (dstFp) {
    gzclose(dstFp);
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
