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
#include "walInt.h"

static int walSeekWritePos(SWal* pWal, int64_t ver) {
  int code = 0;

  TdFilePtr pIdxTFile = pWal->pWriteIdxTFile;
  TdFilePtr pLogTFile = pWal->pWriteLogTFile;

  // seek position
  int64_t idxOff = walGetVerIdxOffset(pWal, ver);
  code = taosLSeekFile(pIdxTFile, idxOff, SEEK_SET);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  SWalIdxEntry entry;
  // TODO:deserialize
  code = taosReadFile(pIdxTFile, &entry, sizeof(SWalIdxEntry));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  ASSERT(entry.ver == ver);
  code = taosLSeekFile(pLogTFile, entry.offset, SEEK_SET);
  if (code < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  return code;
}

int walSetWrite(SWal* pWal) {
  TdFilePtr     pIdxTFile, pLogTFile;
  SWalFileInfo* pRet = taosArrayGetLast(pWal->fileInfoSet);
  ASSERT(pRet != NULL);
  int64_t fileFirstVer = pRet->firstVer;

  char fnameStr[WAL_FILE_LEN];
  walBuildIdxName(pWal, fileFirstVer, fnameStr);
  pIdxTFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pIdxTFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  walBuildLogName(pWal, fileFirstVer, fnameStr);
  pLogTFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pLogTFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  // switch file
  pWal->pWriteIdxTFile = pIdxTFile;
  pWal->pWriteLogTFile = pLogTFile;
  return 0;
}

int walChangeWrite(SWal* pWal, int64_t ver) {
  int       code;
  TdFilePtr pIdxTFile, pLogTFile;
  char      fnameStr[WAL_FILE_LEN];
  if (pWal->pWriteLogTFile != NULL) {
    code = taosCloseFile(&pWal->pWriteLogTFile);
    if (code != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }
  if (pWal->pWriteIdxTFile != NULL) {
    code = taosCloseFile(&pWal->pWriteIdxTFile);
    if (code != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }

  SWalFileInfo tmpInfo;
  tmpInfo.firstVer = ver;
  // bsearch in fileSet
  int32_t idx = taosArraySearchIdx(pWal->fileInfoSet, &tmpInfo, compareWalFileInfo, TD_LE);
  ASSERT(idx != -1);
  SWalFileInfo* pFileInfo = taosArrayGet(pWal->fileInfoSet, idx);
  /*ASSERT(pFileInfo != NULL);*/

  int64_t fileFirstVer = pFileInfo->firstVer;
  walBuildIdxName(pWal, fileFirstVer, fnameStr);
  pIdxTFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pIdxTFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    pWal->pWriteIdxTFile = NULL;
    return -1;
  }
  walBuildLogName(pWal, fileFirstVer, fnameStr);
  pLogTFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pLogTFile == NULL) {
    taosCloseFile(&pIdxTFile);
    terrno = TAOS_SYSTEM_ERROR(errno);
    pWal->pWriteLogTFile = NULL;
    return -1;
  }

  pWal->pWriteLogTFile = pLogTFile;
  pWal->pWriteIdxTFile = pIdxTFile;
  pWal->writeCur = idx;
  return fileFirstVer;
}

int walSeekWriteVer(SWal* pWal, int64_t ver) {
  int code;
  if (ver == pWal->vers.lastVer) {
    return 0;
  }
  if (ver > pWal->vers.lastVer || ver < pWal->vers.firstVer) {
    terrno = TSDB_CODE_WAL_INVALID_VER;
    return -1;
  }
  if (ver < pWal->vers.snapshotVer) {
  }
  if (ver < walGetCurFileFirstVer(pWal) || (ver > walGetCurFileLastVer(pWal))) {
    code = walChangeWrite(pWal, ver);
    if (code != 0) {
      return -1;
    }
  }
  code = walSeekWritePos(pWal, ver);
  if (code != 0) {
    return -1;
  }

  return 0;
}
