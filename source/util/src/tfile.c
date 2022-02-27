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
#include "taoserror.h"
#include "tref.h"
#include "tutil.h"
#include "tlog.h"

static int32_t tsFileRsetId = -1;

static int8_t tfInited = 0;

// static void tfCloseFile(void *p) { taosCloseFile((TdFilePtr)(uintptr_t)p); }

int32_t tfInit() {
  // int8_t old = atomic_val_compare_exchange_8(&tfInited, 0, 1);
  // if (old == 1) return 0;
  // tsFileRsetId = taosOpenRef(2000, tfCloseFile);
  // if (tsFileRsetId > 0) {
  //   return 0;
  // } else {
  //   atomic_store_8(&tfInited, 0);
  //   return -1;
  // }
}

void tfCleanup() {
  // atomic_store_8(&tfInited, 0);
  // if (tsFileRsetId >= 0) taosCloseRef(tsFileRsetId);
  // tsFileRsetId = -1;
}

// static int64_t tfOpenImp(TdFilePtr pFile) {
//   if (pFile == NULL) {
//     terrno = TAOS_SYSTEM_ERROR(errno);
//     return -1;
//   }

//   void *  p = (void *)(int64_t)pFile;
//   int64_t rid = taosAddRef(tsFileRsetId, p);
//   if (rid < 0) taosCloseFile(&pFile);

//   return rid;
// }

// int64_t tfOpenRead(const char *pathname, int32_t flags) {
//   int32_t pFile = taosOpenFile(pathname, TD_FILE_READ);
//   return tfOpenImp(fd);
// }

// int64_t tfOpenReadWrite(const char *pathname, int32_t flags) {
//   int32_t pFile = taosOpenFile(pathname, TD_FILE_READ | TD_FILE_WRITE);
//   return tfOpenImp(fd);
// }

// int64_t tfOpenCreateWrite(const char *pathname, int32_t flags, mode_t mode) {
//   int32_t pFile = taosOpenFile(pathname, TD_FILE_CTEATE | TD_FILE_WRITE);
//   return tfOpenImp(fd);
// }

// int64_t tfOpenCreateWriteAppend(const char *pathname, int32_t flags, mode_t mode) {
//   int32_t pFile = taosOpenFile(pathname, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_APPEND);
//   return tfOpenImp(fd);
// }

// int64_t tfClose(int64_t tfd) { return taosRemoveRef(tsFileRsetId, tfd); }

// int64_t tfWrite(int64_t tfd, void *buf, int64_t count) {
//   void *p = taosAcquireRef(tsFileRsetId, tfd);
//   if (p == NULL) return -1;

//   int32_t pFile = (TdFilePtr)(uintptr_t)p;

//   int64_t ret = taosWriteFile(pFile, buf, count);
//   if (ret < 0) terrno = TAOS_SYSTEM_ERROR(errno);

//   taosReleaseRef(tsFileRsetId, tfd);
//   return ret;
// }

// int64_t tfRead(int64_t tfd, void *buf, int64_t count) {
//   void *p = taosAcquireRef(tsFileRsetId, tfd);
//   if (p == NULL) return -1;

//   int32_t pFile = (TdFilePtr)(uintptr_t)p;

//   int64_t ret = taosReadFile(pFile, buf, count);
//   if (ret < 0) terrno = TAOS_SYSTEM_ERROR(errno);

//   taosReleaseRef(tsFileRsetId, tfd);
//   return ret;
// }

// int64_t tfPread(int64_t tfd, void *buf, int64_t count, int32_t offset) {
//   void *p = taosAcquireRef(tsFileRsetId, tfd);
//   if (p == NULL) return -1;

//   int32_t pFile = (TdFilePtr)(uintptr_t)p;

//   int64_t ret = pread(fd, buf, count, offset);
//   if (ret < 0) terrno = TAOS_SYSTEM_ERROR(errno);

//   taosReleaseRef(tsFileRsetId, tfd);
//   return ret;
// }

// int32_t tfFsync(int64_t tfd) {
//   void *p = taosAcquireRef(tsFileRsetId, tfd);
//   if (p == NULL) return -1;

//   int32_t pFile = (TdFilePtr)(uintptr_t)p;
//   int32_t code = taosFsyncFile(pFile);

//   taosReleaseRef(tsFileRsetId, tfd);
//   return code;
// }

// bool tfValid(int64_t tfd) {
//   void *p = taosAcquireRef(tsFileRsetId, tfd);
//   if (p == NULL) return false;

//   taosReleaseRef(tsFileRsetId, tfd);
//   return true;
// }

// int64_t tfLseek(int64_t tfd, int64_t offset, int32_t whence) {
//   void *p = taosAcquireRef(tsFileRsetId, tfd);
//   if (p == NULL) return -1;

//   int32_t pFile = (TdFilePtr)(uintptr_t)p;
//   int64_t ret = taosLSeekFile(fd, offset, whence);

//   taosReleaseRef(tsFileRsetId, tfd);
//   return ret;
// }

// int32_t tfFtruncate(int64_t tfd, int64_t length) {
//   void *p = taosAcquireRef(tsFileRsetId, tfd);
//   if (p == NULL) return -1;

//   int32_t pFile = (TdFilePtr)(uintptr_t)p;
//   int32_t code = taosFtruncateFile(fd, length);

//   taosReleaseRef(tsFileRsetId, tfd);
//   return code;
// }

// void *tfMmapReadOnly(int64_t tfd, int64_t length) {
//   void *p = taosAcquireRef(tsFileRsetId, tfd);
//   if (p == NULL) return NULL;
//   int32_t pFile = (TdFilePtr)(uintptr_t)p;

//   void *ptr = mmap(NULL, length, PROT_READ, MAP_SHARED, fd, 0);
//   taosReleaseRef(tsFileRsetId, tfd);
//   return ptr;
// }
