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

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#include <io.h>

#if defined(_MSDOS)
#define open _open
#endif

#if defined(_WIN32)
extern int openA(const char *, int, ...); /* MsvcLibX ANSI version of open */
extern int openU(const char *, int, ...); /* MsvcLibX UTF-8 version of open */
#if defined(_UTF8_SOURCE) || defined(_BSD_SOURCE) || defined(_GNU_SOURCE)
#define open openU
#else /* _ANSI_SOURCE */
#define open openA
#endif /* defined(_UTF8_SOURCE) */
#endif /* defined(_WIN32) */

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
  int      refId;
  FileFd   fd;
  FILE    *fp;
} * TdFilePtr, TdFile;

#define FILE_WITH_LOCK 1

void taosGetTmpfilePath(const char *inputTmpDir, const char *fileNamePrefix, char *dstPath) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  const char *tdengineTmpFileNamePrefix = "tdengine-";
  char        tmpPath[PATH_MAX];

  int32_t len = (int32_t)strlen(inputTmpDir);
  memcpy(tmpPath, inputTmpDir, len);

  if (tmpPath[len - 1] != '/' && tmpPath[len - 1] != '\\') {
    tmpPath[len++] = '\\';
  }

  strcpy(tmpPath + len, tdengineTmpFileNamePrefix);
  strcat(tmpPath, tdengineTmpFileNamePrefix);
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
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  char    buffer[4096];
  int64_t size = 0;
  int64_t bytes;

  // fidfrom = open(from, O_RDONLY);
  TdFilePtr pFileFrom = taosOpenFile(from, TD_FILE_READ);
  if (pFileFrom == NULL) goto _err;

  // fidto = open(to, O_WRONLY | O_CREAT | O_EXCL, 0755);
  TdFilePtr pFileTo = taosOpenFile(to, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_EXCL);
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
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  int32_t code = MoveFileEx(oldName, newName, MOVEFILE_REPLACE_EXISTING | MOVEFILE_COPY_ALLOWED);
  if (code < 0) {
    // printf("failed to rename file %s to %s, reason:%s", oldName, newName, strerror(errno));
  }

  return code;
#else
  int32_t code = rename(oldName, newName);
  if (code < 0) {
    // printf("failed to rename file %s to %s, reason:%s", oldName, newName, strerror(errno));
  }

  return code;
#endif
}

int32_t taosStatFile(const char *path, int64_t *size, int32_t *mtime) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  struct stat fileStat;
  int32_t code = stat(path, &fileStat);
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
#endif
}
int32_t taosDevInoFile(const char *path, int64_t *stDev, int64_t *stIno) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  struct stat fileStat;
  int32_t code = stat(path, &fileStat);
  if (code < 0) {
    return code;
  }

  if (stDev != NULL) {
    *stDev = fileStat.st_dev;
  }

  if (stIno != NULL) {
    *stIno = fileStat.st_ino;
  }

  return 0;
#endif
}

void autoDelFileListAdd(const char *path) { return; }

TdFilePtr taosOpenFile(const char *path, int32_t tdFileOptions) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return NULL;
#else
  int fd = -1;
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
    access |= (tdFileOptions & TD_FILE_CTEATE) ? O_CREAT : 0;
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
    fd = open(path, access, S_IRWXU | S_IRWXG | S_IRWXO);
    if (fd == -1) {
      return NULL;
    }
  }

  if (tdFileOptions & TD_FILE_AUTO_DEL) {
    autoDelFileListAdd(path);
  }

  TdFilePtr pFile = (TdFilePtr)malloc(sizeof(TdFile));
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
#endif
}

int64_t taosCloseFile(TdFilePtr *ppFile) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  if (ppFile == NULL || *ppFile == NULL) {
    return 0;
  }
#if FILE_WITH_LOCK
  taosThreadRwlockWrlock(&((*ppFile)->rwlock));
#endif
  if (ppFile == NULL || *ppFile == NULL || (*ppFile)->fd == -1) {
    return 0;
  }
  if ((*ppFile)->fp != NULL) {
    fflush((*ppFile)->fp);
    fclose((*ppFile)->fp);
    (*ppFile)->fp = NULL;
  }
  if ((*ppFile)->fd >= 0) {
    fsync((*ppFile)->fd);
    close((*ppFile)->fd);
    (*ppFile)->fd = -1;
  }
  (*ppFile)->refId = 0;
#if FILE_WITH_LOCK
  taosThreadRwlockUnlock(&((*ppFile)->rwlock));
  taosThreadRwlockDestroy(&((*ppFile)->rwlock));
#endif
  free(*ppFile);
  *ppFile = NULL;
  return 0;
#endif
}

int64_t taosReadFile(TdFilePtr pFile, void *buf, int64_t count) {
  if (pFile == NULL) {
    return 0;
  }
#if FILE_WITH_LOCK
  taosThreadRwlockRdlock(&(pFile->rwlock));
#endif
  assert(pFile->fd >= 0);
  int64_t leftbytes = count;
  int64_t readbytes;
  char   *tbuf = (char *)buf;

  while (leftbytes > 0) {
    readbytes = read(pFile->fd, (void *)tbuf, (uint32_t)leftbytes);
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
  assert(pFile->fd >= 0);
  int64_t ret = pread(pFile->fd, buf, count, offset);
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
  assert(pFile->fd >= 0);

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
  if (pFile == NULL) {
    return 0;
  }
#if FILE_WITH_LOCK
  taosThreadRwlockRdlock(&(pFile->rwlock));
#endif
  assert(pFile->fd >= 0);
  int64_t ret = lseek(pFile->fd, (long)offset, whence);
#if FILE_WITH_LOCK
  taosThreadRwlockUnlock(&(pFile->rwlock));
#endif
  return ret;
}

int32_t taosFStatFile(TdFilePtr pFile, int64_t *size, int32_t *mtime) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  if (pFile == NULL) {
    return 0;
  }
  assert(pFile->fd >= 0);

  struct stat fileStat;
  int32_t code = fstat(pFile->fd, &fileStat);
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
#endif
}

int32_t taosLockFile(TdFilePtr pFile) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  if (pFile == NULL) {
    return 0;
  }
  assert(pFile->fd >= 0);

  return (int32_t)flock(pFile->fd, LOCK_EX | LOCK_NB);
#endif
}

int32_t taosUnLockFile(TdFilePtr pFile) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  if (pFile == NULL) {
    return 0;
  }
  assert(pFile->fd >= 0);

  return (int32_t)flock(pFile->fd, LOCK_UN | LOCK_NB);
#endif
}

int32_t taosFtruncateFile(TdFilePtr pFile, int64_t l_size) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  if (pFile->fd < 0) {
    errno = EBADF;
    uError("%s\n", "fd arg was negative");
    return -1;
  }

  HANDLE h = (HANDLE)_get_osfhandle(pFile->fd);

  LARGE_INTEGER li_0;
  li_0.QuadPart = (int64_t)0;
  BOOL cur = SetFilePointerEx(h, li_0, NULL, FILE_CURRENT);
  if (!cur) {
    uError("SetFilePointerEx Error getting current position in file.\n");
    return -1;
  }

  LARGE_INTEGER li_size;
  li_size.QuadPart = l_size;
  BOOL cur2 = SetFilePointerEx(h, li_size, NULL, FILE_BEGIN);
  if (cur2 == 0) {
    int error = GetLastError();
    uError("SetFilePointerEx GetLastError is: %d\n", error);
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
    uError("SetEndOfFile GetLastError is:%d", error);
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
  assert(pFile->fd >= 0);

  return ftruncate(pFile->fd, l_size);
#endif
}

int32_t taosFsyncFile(TdFilePtr pFile) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  if (pFile->fd < 0) {
    errno = EBADF;
    uError("%s\n", "fd arg was negative");
    return -1;
  }

  HANDLE h = (HANDLE)_get_osfhandle(pFile->fd);

  return FlushFileBuffers(h);
#else
  if (pFile == NULL) {
    return 0;
  }

  if (pFile->fp != NULL) return fflush(pFile->fp);
  if (pFile->fd >= 0) return fsync(pFile->fd);

  return 0;
#endif
}

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)

#define _SEND_FILE_STEP_ 1000

int64_t taosFSendFile(FILE *out_file, FILE *in_file, int64_t *offset, int64_t count) {
  fseek(in_file, (int32_t)(*offset), 0);
  int64_t writeLen = 0;
  uint8_t buffer[_SEND_FILE_STEP_] = {0};

  for (int64_t len = 0; len < (count - _SEND_FILE_STEP_); len += _SEND_FILE_STEP_) {
    size_t rlen = fread(buffer, 1, _SEND_FILE_STEP_, in_file);
    if (rlen <= 0) {
      return writeLen;
    } else if (rlen < _SEND_FILE_STEP_) {
      fwrite(buffer, 1, rlen, out_file);
      return (int64_t)(writeLen + rlen);
    } else {
      fwrite(buffer, 1, _SEND_FILE_STEP_, in_file);
      writeLen += _SEND_FILE_STEP_;
    }
  }

  int64_t remain = count - writeLen;
  if (remain > 0) {
    size_t rlen = fread(buffer, 1, (size_t)remain, in_file);
    if (rlen <= 0) {
      return writeLen;
    } else {
      fwrite(buffer, 1, (size_t)remain, out_file);
      writeLen += remain;
    }
  }

  return writeLen;
}

int64_t taosSendFile(SocketFd dfd, FileFd sfd, int64_t *offset, int64_t count) {
  if (offset != NULL) lseek(sfd, (int32_t)(*offset), 0);

  int64_t writeLen = 0;
  uint8_t buffer[_SEND_FILE_STEP_] = {0};

  for (int64_t len = 0; len < (count - _SEND_FILE_STEP_); len += _SEND_FILE_STEP_) {
    int32_t rlen = (int32_t)read(sfd, buffer, _SEND_FILE_STEP_);
    if (rlen <= 0) {
      return writeLen;
    } else if (rlen < _SEND_FILE_STEP_) {
      taosWriteSocket(dfd, buffer, rlen);
      return (int64_t)(writeLen + rlen);
    } else {
      taosWriteSocket(dfd, buffer, _SEND_FILE_STEP_);
      writeLen += _SEND_FILE_STEP_;
    }
  }

  int64_t remain = count - writeLen;
  if (remain > 0) {
    int32_t rlen = read(sfd, buffer, (int32_t)remain);
    if (rlen <= 0) {
      return writeLen;
    } else {
      taosWriteSocket(sfd, buffer, (int32_t)remain);
      writeLen += remain;
    }
  }

  return writeLen;
}

#elif defined(_TD_DARWIN_64)

int64_t taosFSendFile(FILE *out_file, FILE *in_file, int64_t *offset, int64_t count) {
  int r = 0;
  if (offset) {
    r = fseek(in_file, *offset, SEEK_SET);
    if (r == -1) return -1;
  }
  off_t len = count;
  while (len > 0) {
    char buf[1024 * 16];
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
  return count - len;
}

int64_t taosSendFile(SocketFd dfd, FileFd sfd, int64_t *offset, int64_t count) {
  int r = 0;
  if (offset) {
    r = lseek(sfd, *offset, SEEK_SET);
    if (r == -1) return -1;
  }
  off_t len = count;
  while (len > 0) {
    char buf[1024 * 16];
    off_t n = sizeof(buf);
    if (len < n) n = len;
    size_t m = read(sfd, buf, n);
    if (m == -1) return -1;
    if (m == 0) break;
    size_t l = write(dfd, buf, m);
    if (l == -1) return -1;
    len -= l;
  }
  return count - len;
}

#else

// int64_t taosSendFile(int fdDst, TdFilePtr pFileSrc, int64_t *offset, int64_t size) {
//   if (pFileSrc == NULL) {
//     return 0;
//   }
//   assert(pFileSrc->fd >= 0);

//   int64_t leftbytes = size;
//   int64_t sentbytes;

//   while (leftbytes > 0) {
//     sentbytes = sendfile(fdDst, pFileSrc->fd, offset, leftbytes);
//     if (sentbytes == -1) {
//       if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
//         continue;
//       } else {
//         return -1;
//       }
//     } else if (sentbytes == 0) {
//       return (int64_t)(size - leftbytes);
//     }

//     leftbytes -= sentbytes;
//   }

//   return size;
// }

int64_t taosFSendFile(TdFilePtr pFileOut, TdFilePtr pFileIn, int64_t *offset, int64_t size) {
  if (pFileOut == NULL || pFileIn == NULL) {
    return 0;
  }
  assert(pFileIn->fd >= 0 && pFileOut->fd >= 0);
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
}

#endif

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

#if !defined(WINDOWS)
void *taosMmapReadOnlyFile(TdFilePtr pFile, int64_t length) {
  if (pFile == NULL) {
    return NULL;
  }
  assert(pFile->fd >= 0);

  void *ptr = mmap(NULL, length, PROT_READ, MAP_SHARED, pFile->fd, 0);
  return ptr;
}
#endif

bool taosValidFile(TdFilePtr pFile) { return pFile != NULL; }

int32_t taosUmaskFile(int32_t maskVal) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  return umask(maskVal);
#endif
}

int32_t taosGetErrorFile(TdFilePtr pFile) { return errno; }
int64_t taosGetLineFile(TdFilePtr pFile, char **__restrict__ ptrBuf) {
  if (pFile == NULL) {
    return -1;
  }
  assert(pFile->fp != NULL);

  size_t len = 0;
  return getline(ptrBuf, &len, pFile->fp);
}
int32_t taosEOFFile(TdFilePtr pFile) {
  if (pFile == NULL) {
    return 0;
  }
  assert(pFile->fp != NULL);

  return feof(pFile->fp);
}

#if !defined(WINDOWS)

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

  return access(pathname, flags) == 0;
}

bool taosCheckExistFile(const char *pathname) { return taosCheckAccessFile(pathname, TD_FILE_ACCESS_EXIST_OK); };

#endif // WINDOWS
