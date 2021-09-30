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

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#else
#include <fcntl.h>
#include <sys/file.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

void taosCloseFile(FileFd fd) {
  close(fd);
  fd = FD_INITIALIZER;
}

void taosGetTmpfilePath(const char * inputTmpDir, const char *fileNamePrefix, char *dstPath) {
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

  char tmpPath[PATH_MAX];
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

int64_t taosReadFile(FileFd fd, void *buf, int64_t count) {
  int64_t leftbytes = count;
  int64_t readbytes;
  char *  tbuf = (char *)buf;

  while (leftbytes > 0) {
    readbytes = read(fd, (void *)tbuf, (uint32_t)leftbytes);
    if (readbytes < 0) {
      if (errno == EINTR) {
        continue;
      } else {
        return -1;
      }
    } else if (readbytes == 0) {
      return (int64_t)(count - leftbytes);
    }

    leftbytes -= readbytes;
    tbuf += readbytes;
  }

  return count;
}

int64_t taosWriteFile(FileFd fd, void *buf, int64_t n) {
  int64_t nleft = n;
  int64_t nwritten = 0;
  char *  tbuf = (char *)buf;

  while (nleft > 0) {
    nwritten = write(fd, (void *)tbuf, (uint32_t)nleft);
    if (nwritten < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -1;
    }
    nleft -= nwritten;
    tbuf += nwritten;
  }

  return n;
}

int64_t taosLSeekFile(FileFd fd, int64_t offset, int32_t whence) { return (int64_t)lseek(fd, (long)offset, whence); }

int64_t taosCopyFile(char *from, char *to) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  char    buffer[4096];
  int     fidto = -1, fidfrom = -1;
  int64_t size = 0;
  int64_t bytes;

  fidfrom = open(from, O_RDONLY);
  if (fidfrom < 0) goto _err;

  fidto = open(to, O_WRONLY | O_CREAT | O_EXCL, 0755);
  if (fidto < 0) goto _err;

  while (true) {
    bytes = taosReadFile(fidfrom, buffer, sizeof(buffer));
    if (bytes < 0) goto _err;
    if (bytes == 0) break;

    size += bytes;

    if (taosWriteFile(fidto, (void *)buffer, bytes) < bytes) goto _err;
    if (bytes < sizeof(buffer)) break;
  }

  taosFsyncFile(fidto);

  taosCloseFile(fidfrom);
  taosCloseFile(fidto);
  return size;

_err:
  if (fidfrom >= 0) taosCloseFile(fidfrom);
  if (fidto >= 0) taosCloseFile(fidto);
  remove(to);
  return -1;
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
    char  buf[1024 * 16];
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

int64_t taosSendFile(SocketFd dfd, FileFd sfd, int64_t *offset, int64_t size) {
  int64_t leftbytes = size;
  int64_t sentbytes;

  while (leftbytes > 0) {
    sentbytes = sendfile(dfd, sfd, offset, leftbytes);
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

int64_t taosFSendFile(FILE *outfile, FILE *infile, int64_t *offset, int64_t size) {
  return taosSendFile(fileno(outfile), fileno(infile), offset, size);
}

#endif

int32_t taosFtruncateFile(FileFd fd, int64_t l_size) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  if (fd < 0) {
    errno = EBADF;
    uError("%s\n", "fd arg was negative");
    return -1;
  }

  HANDLE h = (HANDLE)_get_osfhandle(fd);

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
  return ftruncate(fd, l_size);
#endif
}

int32_t taosFsyncFile(FileFd fd) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  if (fd < 0) {
    errno = EBADF;
    uError("%s\n", "fd arg was negative");
    return -1;
  }

  HANDLE h = (HANDLE)_get_osfhandle(fd);

  return FlushFileBuffers(h);
#else
  return fsync(fd);
#endif
}

int32_t taosRenameFile(char *oldName, char *newName) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  int32_t code = MoveFileEx(oldName, newName, MOVEFILE_REPLACE_EXISTING | MOVEFILE_COPY_ALLOWED);
  if (code < 0) {
    printf("failed to rename file %s to %s, reason:%s", oldName, newName, strerror(errno));
  }

  return code;
#else
  int32_t code = rename(oldName, newName);
  if (code < 0) {
    printf("failed to rename file %s to %s, reason:%s", oldName, newName, strerror(errno));
  }

  return code;
#endif
}


int32_t taosLockFile(int32_t fd) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  return (int32_t)flock(fd, LOCK_EX | LOCK_NB);
#endif
}

int32_t taosUnLockFile(int32_t fd) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  return (int32_t)flock(fd, LOCK_UN | LOCK_NB);
#endif
}

int32_t taosUmaskFile(int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  return umask(val);
#endif
}

int32_t taosStatFile(const char *path, int64_t *size, int32_t *mtime) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  struct stat fileStat;
  int32_t     code = stat(path, &fileStat);
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

int32_t taosFStatFile(int32_t fd, int64_t *size, int32_t *mtime) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  struct stat fileStat;
  int32_t     code = fstat(fd, &fileStat);
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

int32_t taosOpenFileWrite(const char *path) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  return open(path, O_WRONLY, S_IRWXU | S_IRWXG | S_IRWXO);
#endif
}

FileFd taosOpenFileRead(const char *path) {
  #if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  return open(path, O_RDONLY);
#endif
}

int32_t taosOpenFileCreateWrite(const char *path) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  return open(path, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
#endif
}

int32_t taosOpenFileTruncCreateWrite(const char *path) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  return open(path, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
#endif
}