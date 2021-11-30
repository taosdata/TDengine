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

int walSeekVerImpl(SWal *pWal, int64_t ver) {
  //close old file
  int code = 0;
  code = tfClose(pWal->curLogTfd);
  if(code != 0) {
   //TODO 
  }
  code = tfClose(pWal->curIdxTfd);
  if(code != 0) {
   //TODO 
  }
  //bsearch in fileSet
  int fName = 0;//TODO
  //open the right file
  char fNameStr[WAL_FILE_LEN];
  sprintf(fNameStr, "%d."WAL_INDEX_SUFFIX, fName);
  bool closed = 1; //TODO:read only
  int64_t idxTfd = tfOpenReadWrite(fNameStr);
  sprintf(fNameStr, "%d."WAL_LOG_SUFFIX, fName);
  int64_t logTfd = tfOpenReadWrite(fNameStr);
  //seek position
  int64_t offset = (ver - fName) * WAL_IDX_ENTRY_SIZE;
  tfLseek(idxTfd, offset, SEEK_SET);
  //set cur version, cur file version and cur status
  pWal->curFileFirstVersion = fName;
  pWal->curFileLastVersion = 1;//TODO
  pWal->curLogTfd = logTfd;
  pWal->curIdxTfd = idxTfd;
  pWal->curVersion = ver;
  pWal->curOffset = offset;
  pWal->curStatus = 0;//TODO
  return code;
}

int walSeekVer(SWal *pWal, int64_t ver) {
  if(ver > pWal->lastVersion) {
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
  if(ver >= pWal->curFileFirstVersion
      && ((pWal->curFileLastVersion == -1 && ver <= pWal->lastVersion) || (ver <= pWal->curFileLastVersion))) {

  }
  if(ver < pWal->curFileFirstVersion || (pWal->curFileLastVersion != -1 && ver > pWal->curFileLastVersion)) {
    int index = 0;
    index = 1;
    //back up to avoid inconsistency
    int64_t curVersion = pWal->curVersion;
    int64_t curOffset = pWal->curOffset;
    int64_t curFileFirstVersion = pWal->curFileFirstVersion;
    int64_t curFileLastVersion = pWal->curFileLastVersion;
    if(walSeekVerImpl(pWal, ver) < 0) {
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
    if(walSeekVer(pWal, ver) != 0) {
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
