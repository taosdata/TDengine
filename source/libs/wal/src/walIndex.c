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

static int walSeekFilePos(SWal* pWal, int64_t ver) {
  int code = 0;

  int64_t idxTfd = pWal->curIdxTfd;
  int64_t logTfd = pWal->curLogTfd;
  
  //seek position
  int64_t offset = (ver - walGetCurFileFirstVer(pWal)) * WAL_IDX_ENTRY_SIZE;
  code = tfLseek(idxTfd, offset, SEEK_SET);
  if(code != 0) {

  }
  int64_t readBuf[2];
  code = tfRead(idxTfd, readBuf, sizeof(readBuf));
  if(code != 0) {

  }
  //TODO:deserialize
  ASSERT(readBuf[0] == ver);
  code = tfLseek(logTfd, readBuf[1], SEEK_CUR);
  if (code != 0) {

  }
  /*pWal->curLogOffset = readBuf[1];*/
  pWal->curVersion = ver;
  return code;
}

static int walChangeFile(SWal *pWal, int64_t ver) {
  int code = 0;
  int64_t idxTfd, logTfd;
  char fnameStr[WAL_FILE_LEN];
  code = tfClose(pWal->curLogTfd);
  if(code != 0) {
   //TODO 
  }
  code = tfClose(pWal->curIdxTfd);
  if(code != 0) {
   //TODO 
  }
  WalFileInfo tmpInfo;
  tmpInfo.firstVer = ver;
  //bsearch in fileSet
  WalFileInfo* pRet = taosArraySearch(pWal->fileInfoSet, &tmpInfo, compareWalFileInfo, TD_LE);
  ASSERT(pRet != NULL);
  int64_t fileFirstVer = pRet->firstVer;
  //closed
  if(taosArrayGetLast(pWal->fileInfoSet) != pRet) {
    pWal->curStatus &= ~WAL_CUR_FILE_WRITABLE;
    walBuildIdxName(pWal, fileFirstVer, fnameStr);
    idxTfd = tfOpenRead(fnameStr);
    walBuildLogName(pWal, fileFirstVer, fnameStr);
    logTfd = tfOpenRead(fnameStr);
  } else {
    pWal->curStatus |= WAL_CUR_FILE_WRITABLE;
    walBuildIdxName(pWal, fileFirstVer, fnameStr);
    idxTfd = tfOpenReadWrite(fnameStr);
    walBuildLogName(pWal, fileFirstVer, fnameStr);
    logTfd = tfOpenReadWrite(fnameStr);
  }

  pWal->curLogTfd = logTfd;
  pWal->curIdxTfd = idxTfd;
  return code;
}

int walSeekVer(SWal *pWal, int64_t ver) {
  if((!(pWal->curStatus & WAL_CUR_FAILED))
      && ver == pWal->curVersion) {
    return 0;
  }
  if(ver > pWal->lastVersion) {
    //TODO: some records are skipped
    return -1;
  }
  if(ver < pWal->firstVersion) {
    //TODO: try to seek pruned log
    return -1;
  }
  if(ver < pWal->snapshotVersion) {
    //TODO: seek snapshotted log, invalid in some cases
  }
  if(ver < walGetCurFileFirstVer(pWal) || (ver > walGetCurFileLastVer(pWal))) {
    walChangeFile(pWal, ver);
  }
  walSeekFilePos(pWal, ver);
  
  return 0;
}
