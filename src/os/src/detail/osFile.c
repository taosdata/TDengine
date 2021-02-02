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
#include "os.h"
#include "tglobal.h"

#ifndef TAOS_OS_FUNC_FILE_GETTMPFILEPATH

void taosGetTmpfilePath(const char *fileNamePrefix, char *dstPath) {
  const char *tdengineTmpFileNamePrefix = "tdengine-";

  char  tmpPath[PATH_MAX];
  int32_t len = strlen(tsTempDir);
  memcpy(tmpPath, tsTempDir, len);

  if (tmpPath[len - 1] != '/') {
      tmpPath[len++] = '/';
  }

  strcpy(tmpPath + len, tdengineTmpFileNamePrefix);
  if (strlen(tmpPath) + strlen(fileNamePrefix) + strlen("-%d-%s") < PATH_MAX) {
    strcat(tmpPath, fileNamePrefix);
    strcat(tmpPath, "-%d-%s");
  }

  char rand[8] = {0};
  taosRandStr(rand, tListLen(rand) - 1);
  snprintf(dstPath, PATH_MAX, tmpPath, getpid(), rand);
}

#endif

int32_t taosRenameFile(char *fullPath, char *suffix, char delimiter, char **dstPath) {
  int32_t ts = taosGetTimestampSec();

  char fname[PATH_MAX] = {0};  // max file name length must be less than 255

  char *delimiterPos = strrchr(fullPath, delimiter);
  if (delimiterPos == NULL) return -1;

  int32_t fileNameLen = 0;
  if (suffix) {
    fileNameLen = snprintf(fname, PATH_MAX, "%s.%d.%s", delimiterPos + 1, ts, suffix);
  } else {
    fileNameLen = snprintf(fname, PATH_MAX, "%s.%d", delimiterPos + 1, ts);
  }

  int32_t len = (int32_t)((delimiterPos - fullPath) + fileNameLen + 1);
  if (*dstPath == NULL) {
    *dstPath = calloc(1, len + 1);
    if (*dstPath == NULL) return -1;
  }

  strncpy(*dstPath, fullPath, (size_t)(delimiterPos - fullPath + 1));
  strncat(*dstPath, fname, (size_t)fileNameLen);
  (*dstPath)[len] = 0;

  return rename(fullPath, *dstPath);
}

int64_t taosReadImp(int32_t fd, void *buf, int64_t count) {
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

int64_t taosWriteImp(int32_t fd, void *buf, int64_t n) {
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

int64_t taosLSeekImp(int32_t fd, int64_t offset, int32_t whence) {
  return (int64_t)lseek(fd, (long)offset, whence);
}

int64_t taosCopy(char *from, char *to) {
  char    buffer[4096];
  int     fidto = -1, fidfrom = -1;
  int64_t size = 0;
  int64_t bytes;

  fidfrom = open(from, O_RDONLY | O_BINARY);
  if (fidfrom < 0) goto _err;

  fidto = open(to, O_WRONLY | O_CREAT | O_EXCL | O_BINARY, 0755);
  if (fidto < 0) goto _err;

  while (true) {
    bytes = taosRead(fidfrom, buffer, sizeof(buffer));
    if (bytes < 0) goto _err;
    if (bytes == 0) break;

    size += bytes;

    if (taosWrite(fidto, (void *)buffer, bytes) < bytes) goto _err;
    if (bytes < sizeof(buffer)) break;
  }

  close(fidfrom);
  close(fidto);
  return size;

_err:
  if (fidfrom >= 0) close(fidfrom);
  if (fidto >= 0) close(fidto);
  remove(to);
  return -1;
}

#ifndef TAOS_OS_FUNC_FILE_SENDIFLE

int64_t taosSendFile(SOCKET dfd, int32_t sfd, int64_t *offset, int64_t size) {
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

#ifndef TAOS_OS_FUNC_FILE_FTRUNCATE

int32_t taosFtruncate(int32_t fd, int64_t length) {
  return ftruncate(fd, length);
}

#endif