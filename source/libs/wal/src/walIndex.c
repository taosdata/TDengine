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
#include "tfile.h"
#include "walInt.h"

int walSetCurVerImpl(SWal *pWal, int64_t ver) {
  //close old file
  //iterate all files
  //open right file
  //set cur version, cur file version and cur status
  return 0;
}

int walSetCurVer(SWal *pWal, int64_t ver) {
  if(ver > pWal->lastVersion + 1) {
    //TODO: some records are skipped
    return -1;
  }
  if(ver < pWal->firstVersion) {
    //TODO: try to seek pruned log
    return -1;
  }
  if(ver < pWal->snapshotVersion) {
    //TODO: seek snapshotted log
  }
  if(ver < pWal->curFileFirstVersion || (pWal->curFileLastVersion != -1 && ver > pWal->curFileLastVersion)) {
    //back up to avoid inconsistency
    int64_t curVersion = pWal->curVersion;
    int64_t curOffset = pWal->curOffset;
    int64_t curFileFirstVersion = pWal->curFileFirstVersion;
    int64_t curFileLastVersion = pWal->curFileLastVersion;
    if(walSetCurVerImpl(pWal, ver) < 0) {
      //TODO: errno
      pWal->curVersion = curVersion;
      pWal->curOffset = curOffset;
      pWal->curFileFirstVersion = curFileFirstVersion;
      pWal->curFileLastVersion = curFileLastVersion;
      return -1;
    }
  }
  
  return 0;
}

int walWriteIndex(SWal *pWal, int64_t ver, int64_t offset) {
  int code = 0;
  //get index file
  if(!tfValid(pWal->curIdxTfd)) {
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%"PRId64".idx, failed to open since %s", pWal->vgId, pWal->curFileFirstVersion, strerror(errno));
  }
  if(pWal->curVersion != ver) {
    if(walSetCurVer(pWal, ver) != 0) {
      //TODO: some records are skipped
      return -1;
    }
  }
  //check file checksum
  //append index
  return 0;
}

int walRotateIndex(SWal *pWal) {
  //check file checksum
  //create new file
  //switch file
  return 0;
}
