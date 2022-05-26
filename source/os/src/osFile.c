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

#ifdef WINDOWS
#include <io.h>
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
#endif

#if defined(WINDOWS)
typedef int32_t FileFd;
typedef int32_t SocketFd;
#else
typedef int32_t FileFd;
typedef int32_t SocketFd;
#endif

typedef int32_t FileFd;

typedef struct TdFile {
  TdThreadRwlock rwlock;
  int            refId;
  FileFd         fd;
  FILE          *fp;
} * TdFilePtr, TdFile;

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

  taosFsyncFile(pFileTo);

  taosCloseFile(&pFileFrom);
  taosCloseFile(&pFileTo);
  return size;

_err:
  if (pFileFrom != NULL) taosCloseFile(&pFileFrom);
  if (pFileTo != NULL) taosCloseFile(&pFileTo);
  taosRemoveFile(to);
  return -1;
#endif
}

int32_t taosRemoveFile(const char *path) { return remove(path); }

int32_t taosRenameFile(const char *oldName, const char *newName) {
#ifdef WINDOWS
  bool code = MoveFileEx(oldName, newName, MOVEFILE_REPLACE_EXISTING | MOVEFILE_COPY_ALLOWED);
  if (!code) {
    printf("failed to rename file %s to %s, reason:%s", oldName, newName, strerror(errno));
  }

  return !code;
#else
  int32_t code = rename(oldName, newName);
  if (code < 0) {
    printf("failed to rename file %s to %s, reason:%s", oldName, newName, strerror(errno));
  }

  return code;
#endif
}

int32_t taosStatFile(const char *path, int64_t *size, int32_t *mtime) {
  struct stat fileStat;
#ifdef WINDOWS
  int32_t     code = _stat(path, &fileStat);
#else
  int32_t     code = stat(path, &fileStat);
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
int32_t taosDevInoFile(TdFilePtr pFile, int64_t *stDev, int64_t *stIno) {
  if (pFile == NULL) {
    return 0;
  }
  assert(pFile->fd >= 0);  // Please check if you have closed the file.

#ifdef WINDOWS

  BY_HANDLE_FILE_INFORMATION bhfi;
  HANDLE handle = (HANDLE)_get_osfhandle(pFile->fd);
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
  int32_t     code = fstat(pFile->fd, &fileStat);
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

void autoDelFileListAdd(const char *path) { return; }

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
    assert(!(tdFileOptions & TD_FILE_EXCL));
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
    fd = _open(path, access, _S_IREAD | _S_IWRITE);
#else
    fd = open(path, access, S_IRWXU | S_IRWXG | S_IRWXO);
#endif
    if (fd == -1) {
      return NULL;
    }
  }

  if (tdFileOptions & TD_FILE_AUTO_DEL) {
    autoDelFileListAdd(path);
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
  return pFile;
}

int64_t taosCloseFile(TdFilePtr *ppFile) {
  if (ppFile == NULL || *ppFile == NULL) {
    return 0;
  }
#if FILE_WITH_LOCK
  taosThreadRwlockWrlock(&((*ppFile)->rwlock));
#endif
  if (ppFile == NULL || *ppFile == NULL) {
    return 0;
  }
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
    fsync((*ppFile)->fd);
  #endif
    close((*ppFile)->fd);
    (*ppFile)->fd = -1;
  }
  (*ppFile)->refId = 0;
#if FILE_WITH_LOCK
  taosThreadRwlockUnlock(&((*ppFile)->rwlock));
  taosThreadRwlockDestroy(&((*ppFile)->rwlock));
#endif
  taosMemoryFree(*ppFile);
  *ppFile = NULL;
  return 0;
}

int64_t taosReadFile(TdFilePtr pFile, void *buf, int64_t count) {
#if FILE_WITH_LOCK
  taosThreadRwlockRdlock(&(pFile->rwlock));
#endif
  assert(pFile->fd >= 0);  // Please check if you have closed the file.
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
  assert(pFile->fd >= 0);  // Please check if you have closed the file.
#ifdef WINDOWS
  size_t pos = _lseek(pFile->fd, 0, SEEK_CUR);
  _lseek(pFile->fd, offset, SEEK_SET);
  int64_t ret = _read(pFile->fd, buf, count);
  _lseek(pFile->fd, pos, SEEK_SET);
#else
  int64_t ret = pread(pFile->fd, buf, count, offset);
#endif
#if FILE_WITH_LOCK
  taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  return ret;
}

int64_t taosWriteFile(TdFilePtr pFile, const void *buf, int64_t count) {
#if FILE_WITH_LOCK
  taosThreadRwlockWrlock(&(pFile->rwlock));
#endif
  assert(pFile->fd >= 0);  // Please check if you have closed the file.

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

int64_t taosLSeekFile(TdFilePtr pFile, int64_t offset, int32_t whence) {
#if FILE_WITH_LOCK
  taosThreadRwlockRdlock(&(pFile->rwlock));
#endif
  assert(pFile->fd >= 0);  // Please check if you have closed the file.
#ifdef WINDOWS
  int64_t ret = _lseek(pFile->fd, offset, whence);
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
  assert(pFile->fd >= 0);  // Please check if you have closed the file.

  struct stat fileStat;
#ifdef WINDOWS
  int32_t     code = _fstat(pFile->fd, &fileStat);
#else
  int32_t     code = fstat(pFile->fd, &fileStat);
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
#ifdef WINDOWS
  return 0;
#else
  assert(pFile->fd >= 0);  // Please check if you have closed the file.

  return (int32_t)flock(pFile->fd, LOCK_EX | LOCK_NB);
#endif
}

int32_t taosUnLockFile(TdFilePtr pFile) {
#ifdef WINDOWS
  return 0;
#else
  assert(pFile->fd >= 0);  // Please check if you have closed the file.

  return (int32_t)flock(pFile->fd, LOCK_UN | LOCK_NB);
#endif
}

int32_t taosFtruncateFile(TdFilePtr pFile, int64_t l_size) {
#ifdef WINDOWS
  if (pFile->fd < 0) {
    errno = EBADF;
    printf("Ftruncate file error, fd arg was negative\n");
    return -1;
  }

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
  if (pFile == NULL) {
    return 0;
  }
  assert(pFile->fd >= 0);  // Please check if you have closed the file.

  return ftruncate(pFile->fd, l_size);
#endif
}

int32_t taosFsyncFile(TdFilePtr pFile) {
  if (pFile == NULL) {
    return 0;
  }

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
  assert(pFileIn->fd >= 0 && pFileOut->fd >= 0);

#ifdef WINDOWS

  _lseek(pFileIn->fd, (int32_t)(*offset), 0);
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

  int r = 0;
  if (offset) {
    r = fseek(in_file, *offset, SEEK_SET);
    if (r == -1) return -1;
  }
  off_t len = size;
  while (len > 0) {
    char  buf[1024 * 16];
    off_t n = sizeof(buf);
    if (len < n) n = len;
    size_t m = fread(buf, 1, n, in_file);
    if (m < n) {
      int e = ferror(in_file);
      if (e) return -1;
    }
    if (m == 0) break;
    if (m != fwrite(buf, 1, m, out_file)) {
      return -1;
    }
    len -= m;
  }
  return size - len;

#else

  int64_t leftbytes = size;
  int64_t sentbytes;

  while (leftbytes > 0) {
    sentbytes = sendfile(pFileOut->fd, pFileIn->fd, offset, leftbytes);
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
  if (pFile == NULL) {
    return;
  }
  assert(pFile->fp != NULL);

  va_list ap;
  va_start(ap, format);
  vfprintf(pFile->fp, format, ap);
  va_end(ap);
  fflush(pFile->fp);
}

bool taosValidFile(TdFilePtr pFile) { return pFile != NULL; }

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
  assert(pFile->fp != NULL);
#ifdef WINDOWS
  *ptrBuf = taosMemoryMalloc(1024);
  if (*ptrBuf == NULL) return -1;
  if (fgets(*ptrBuf, 1023, pFile->fp) == NULL) {
    taosMemoryFreeClear(*ptrBuf);
    return -1;
  }
  (*ptrBuf)[1023] = 0;
  return strlen(*ptrBuf);
#else
  size_t len = 0;
  return getline(ptrBuf, &len, pFile->fp);
#endif
}
int64_t taosGetsFile(TdFilePtr pFile, int32_t maxSize, char *__restrict buf) {
  if (pFile == NULL || buf == NULL) {
    return -1;
  }
  assert(pFile->fp != NULL);
  if (fgets(buf, maxSize, pFile->fp) == NULL) {
    return -1;
  }
  return strlen(buf);
}
int32_t taosEOFFile(TdFilePtr pFile) {
  if (pFile == NULL) {
    return 0;
  }
  assert(pFile->fp != NULL);

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
