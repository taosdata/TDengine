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
#include "osSocket.h"
#include "tglobal.h"
#include "tulog.h"

void taosGetTmpfilePath(const char *fileNamePrefix, char *dstPath) {
  const char *tdengineTmpFileNamePrefix = "tdengine-";
  char        tmpPath[PATH_MAX];

  int32_t len = (int32_t)strlen(tsTempDir);
  memcpy(tmpPath, tsTempDir, len);

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
}

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

int64_t taosSendFile(SOCKET dfd, int32_t sfd, int64_t *offset, int64_t count) {
  lseek(sfd, (int32_t)(*offset), 0);
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

int32_t taosFtruncate(int32_t fd, int64_t l_size) {
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
}


int fsync(int filedes) { 
  if (filedes < 0) {
    errno = EBADF;
    uError("%s\n", "fd arg was negative");
    return -1;
  }

  HANDLE h = (HANDLE)_get_osfhandle(filedes);
  FlushFileBuffers(h);

  return 0;
 }
