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
#include "tulog.h"

int64_t taosFSendFile(FILE *out_file, FILE *in_file, int64_t *offset, int64_t count) {
  int r = 0;
  if (offset) {
    r = fseek(in_file, *offset, SEEK_SET);
    if (r==-1) return -1;
  }
  off_t len = count;
  while (len>0) {
    char buf[1024*16];
    off_t n = sizeof(buf);
    if (len<n) n = len;
    size_t m = fread(buf, 1, n, in_file);
    if (m<n) {
      int e = ferror(in_file);
      if (e) return -1;
    }
    if (m==0) break;
    if (m!=fwrite(buf, 1, m, out_file)) {
      return -1;
    }
    len -= m;
  }
  return count - len;
}

int64_t taosSendFile(SOCKET dfd, int32_t sfd, int64_t* offset, int64_t count) {
  int r = 0;
  if (offset) {
    r = lseek(sfd, *offset, SEEK_SET);
    if (r==-1) return -1;
  }
  off_t len = count;
  while (len>0) {
    char buf[1024*16];
    off_t n = sizeof(buf);
    if (len<n) n = len;
    size_t m = read(sfd, buf, n);
    if (m==-1) return -1;
    if (m==0) break;
    size_t l = write(dfd, buf, m);
    if (l==-1) return -1;
    len -= l;
  }
  return count - len;
}

