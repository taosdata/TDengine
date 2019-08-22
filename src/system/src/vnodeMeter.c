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

#include <arpa/inet.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "trpc.h"
#include "tschemautil.h"
#include "ttime.h"
#include "tutil.h"
#include "vnode.h"
#include "vnodeMgmt.h"
#include "vnodeShell.h"
#include "vnodeUtil.h"

#pragma GCC diagnostic ignored "-Wpointer-sign"

#define VALID_TIMESTAMP(key, curKey, prec) (((key) >= 0) && ((key) <= ((curKey) + 36500 * tsMsPerDay[prec])))

int  tsMeterSizeOnFile;
void vnodeUpdateMeter(void *param, void *tmdId);
void vnodeRecoverMeterObjectFile(int vnode);

int (*vnodeProcessAction[])(SMeterObj *, char *, int, char, void *, int, int *) = {vnodeInsertPoints,
                                                                                   vnodeImportPoints};

void vnodeFreeMeterObj(SMeterObj *pObj) {
  if (pObj == NULL) return;

  dTrace("vid:%d sid:%d id:%s, meter is cleaned up", pObj->vnode, pObj->sid, pObj->meterId);

  vnodeFreeCacheInfo(pObj);
  if (vnodeList[pObj->vnode].meterList != NULL) {
    vnodeList[pObj->vnode].meterList[pObj->sid] = NULL;
  }

  memset(pObj->meterId, 0, tListLen(pObj->meterId));
  tfree(pObj);
}

int vnodeUpdateVnodeStatistic(FILE *fp, SVnodeObj *pVnode) {
  fseek(fp, TSDB_FILE_HEADER_VERSION_SIZE, SEEK_SET);
  fwrite(&(pVnode->vnodeStatistic), sizeof(SVnodeStatisticInfo), 1, fp);

  return 0;
}

void vnodeUpdateVnodeFileHeader(FILE *fp, SVnodeObj *pVnode) {
  fseek(fp, TSDB_FILE_HEADER_LEN * 1 / 4, SEEK_SET);
  fprintf(fp, "%ld %ld %ld ", pVnode->lastCreate, pVnode->lastRemove, pVnode->version);
  fprintf(fp, "%ld %d %d ", pVnode->lastKeyOnFile, pVnode->fileId, pVnode->numOfFiles);
}

int vnodeCreateMeterObjFile(int vnode) {
  FILE *  fp;
  char    fileName[TSDB_FILENAME_LEN];
  int32_t size;
  // SMeterObj *pObj;

  sprintf(fileName, "%s/vnode%d/meterObj.v%d", tsDirectory, vnode, vnode);
  fp = fopen(fileName, "w+");
  if (fp == NULL) {
    dError("failed to create vnode:%d file:%s", vnode, fileName);
    return -1;
  } else {
    vnodeCreateFileHeader(fp);
    vnodeUpdateVnodeFileHeader(fp, vnodeList + vnode);
    fseek(fp, TSDB_FILE_HEADER_LEN, SEEK_SET);

    size = sizeof(SMeterObjHeader) * vnodeList[vnode].cfg.maxSessions + sizeof(TSCKSUM);
    tfree(vnodeList[vnode].meterIndex);
    vnodeList[vnode].meterIndex = calloc(1, size);
    taosCalcChecksumAppend(0, (uint8_t *)(vnodeList[vnode].meterIndex), size);
    fwrite(vnodeList[vnode].meterIndex, size, 1, fp);

    fclose(fp);
  }

  return 0;
}

FILE *vnodeOpenMeterObjFile(int vnode) {
  FILE *      fp;
  char        fileName[TSDB_FILENAME_LEN];
  struct stat fstat;

  // check if directory exists
  sprintf(fileName, "%s/vnode%d", tsDirectory, vnode);
  if (stat(fileName, &fstat) < 0) return NULL;

  sprintf(fileName, "%s/vnode%d/meterObj.v%d", tsDirectory, vnode, vnode);
  if (stat(fileName, &fstat) < 0) return NULL;

  fp = fopen(fileName, "r+");
  if (fp != NULL) {
    if (vnodeCheckFileIntegrity(fp) < 0) {
      dError("file:%s is corrupted, need to restore it first", fileName);
      fclose(fp);

      // todo: how to recover
      exit(1);
    }
  } else {
    dError("failed to open %s, reason:%s", fileName, strerror(errno));
  }

  return fp;
}

int vnodeSaveMeterObjToFile(SMeterObj *pObj) {
  int64_t    offset, length, new_length, new_offset;
  FILE *     fp;
  SVnodeObj *pVnode = &vnodeList[pObj->vnode];
  char *     buffer = NULL;

  fp = vnodeOpenMeterObjFile(pObj->vnode);
  if (fp == NULL) return -1;

  buffer = (char *)malloc(tsMeterSizeOnFile);
  if (buffer == NULL) {
    dError("Failed to allocate memory while saving meter object to file, meterId", pObj->meterId);
    fclose(fp);
    return -1;
  }

  offset = pVnode->meterIndex[pObj->sid].offset;
  length = pVnode->meterIndex[pObj->sid].length;

  new_length = offsetof(SMeterObj, reserved) + pObj->numOfColumns * sizeof(SColumn) + pObj->sqlLen + sizeof(TSCKSUM);

  memcpy(buffer, pObj, offsetof(SMeterObj, reserved));
  memcpy(buffer + offsetof(SMeterObj, reserved), pObj->schema, pObj->numOfColumns * sizeof(SColumn));
  memcpy(buffer + offsetof(SMeterObj, reserved) + pObj->numOfColumns * sizeof(SColumn), pObj->pSql, pObj->sqlLen);
  taosCalcChecksumAppend(0, (uint8_t *)buffer, new_length);

  if (offset == 0 || length < new_length) {  // New, append to file end
    fseek(fp, 0, SEEK_END);
    new_offset = ftell(fp);
    fwrite(buffer, new_length, 1, fp);
    pVnode->meterIndex[pObj->sid].offset = new_offset;
    pVnode->meterIndex[pObj->sid].length = new_length;
  } else if (offset < 0) {  // deleted meter, append to end of file
    fseek(fp, -offset, SEEK_SET);
    fwrite(buffer, new_length, 1, fp);
    pVnode->meterIndex[pObj->sid].offset = -offset;
    pVnode->meterIndex[pObj->sid].length = new_length;
  } else {  // meter exists, overwrite it, offset > 0
    fseek(fp, offset, SEEK_SET);
    fwrite(buffer, new_length, 1, fp);
    pVnode->meterIndex[pObj->sid].offset = (pObj->meterId[0] == 0) ? -offset : offset;
    pVnode->meterIndex[pObj->sid].length = new_length;
  }
  // taosCalcChecksumAppend(0, pVnode->meterIndex, sizeof(SMeterObjHeader)*pVnode->cfg.maxSessions+sizeof(TSCKSUM));
  // NOTE: no checksum, since it makes creating table slow
  fseek(fp, TSDB_FILE_HEADER_LEN + sizeof(SMeterObjHeader) * pObj->sid, SEEK_SET);
  fwrite(&(pVnode->meterIndex[pObj->sid]), sizeof(SMeterObjHeader), 1, fp);
  // update checksum
  // fseek(fp, TSDB_FILE_HEADER_LEN+sizeof(SMeterObjHeader)*(pVnode->cfg.maxSessions), SEEK_SET);
  // fwrite(((char *)(pVnode->meterIndex) + sizeof(SMeterObjHeader)*(pVnode->cfg.maxSessions)), sizeof(TSCKSUM), 1, fp);

  tfree(buffer);

  vnodeUpdateVnodeStatistic(fp, pVnode);
  vnodeUpdateVnodeFileHeader(fp, pVnode);
  /* vnodeUpdateFileCheckSum(fp); */
  fclose(fp);

  return 0;
}

int vnodeSaveAllMeterObjToFile(int vnode) {
  int64_t    offset, length, new_length, new_offset;
  FILE *     fp;
  SMeterObj *pObj;
  SVnodeObj *pVnode = &vnodeList[vnode];
  char *     buffer = NULL;

  fp = vnodeOpenMeterObjFile(vnode);
  if (fp == NULL) return -1;

  buffer = (char *)malloc(tsMeterSizeOnFile);
  if (buffer == NULL) {
    dError("Failed to allocate memory while saving all meter objects to file");
    return -1;
  }

  for (int sid = 0; sid < pVnode->cfg.maxSessions; ++sid) {
    pObj = pVnode->meterList[sid];
    if (pObj == NULL) continue;

    offset = pVnode->meterIndex[sid].offset;
    length = pVnode->meterIndex[sid].length;

    new_length = offsetof(SMeterObj, reserved) + pObj->numOfColumns * sizeof(SColumn) + pObj->sqlLen + sizeof(TSCKSUM);

    memcpy(buffer, pObj, offsetof(SMeterObj, reserved));
    memcpy(buffer + offsetof(SMeterObj, reserved), pObj->schema, pObj->numOfColumns * sizeof(SColumn));
    memcpy(buffer + offsetof(SMeterObj, reserved) + pObj->numOfColumns * sizeof(SColumn), pObj->pSql, pObj->sqlLen);
    taosCalcChecksumAppend(0, (uint8_t *)buffer, new_length);

    if (offset == 0 || length > new_length) {  // New, append to file end
      new_offset = fseek(fp, 0, SEEK_END);
      fwrite(buffer, new_length, 1, fp);
      pVnode->meterIndex[sid].offset = new_offset;
      pVnode->meterIndex[sid].length = new_length;
    } else if (offset < 0) {  // deleted meter, append to end of file
      fseek(fp, -offset, SEEK_SET);
      fwrite(buffer, new_length, 1, fp);
      pVnode->meterIndex[sid].offset = -offset;
      pVnode->meterIndex[sid].length = new_length;
    } else {  // meter exists, overwrite it, offset > 0
      fseek(fp, offset, SEEK_SET);
      fwrite(buffer, new_length, 1, fp);
      pVnode->meterIndex[sid].offset = offset;
      pVnode->meterIndex[sid].length = new_length;
    }
  }
  // taosCalcChecksumAppend(0, pVnode->meterIndex, sizeof(SMeterObjHeader)*pVnode->cfg.maxSessions+sizeof(TSCKSUM));
  fseek(fp, TSDB_FILE_HEADER_LEN, SEEK_SET);
  fwrite(pVnode->meterIndex, sizeof(SMeterObjHeader) * pVnode->cfg.maxSessions + sizeof(TSCKSUM), 1, fp);

  tfree(buffer);

  vnodeUpdateVnodeStatistic(fp, pVnode);
  vnodeUpdateVnodeFileHeader(fp, pVnode);
  /* vnodeUpdateFileCheckSum(fp); */
  fclose(fp);

  return 0;
}

int vnodeSaveVnodeCfg(int vnode, SVnodeCfg *pCfg, SVPeerDesc *pDesc) {
  FILE *fp;

  fp = vnodeOpenMeterObjFile(vnode);
  if (fp == NULL) {
    dError("failed to open vnode:%d file", vnode);
    return -1;
  }

  fseek(fp, TSDB_FILE_HEADER_LEN * 2 / 4, SEEK_SET);
  fwrite(pCfg, sizeof(SVnodeCfg), 1, fp);

  char temp[TSDB_FILE_HEADER_LEN / 4];
  memset(temp, 0, sizeof(temp));
  fseek(fp, TSDB_FILE_HEADER_LEN * 3 / 4, SEEK_SET);
  fwrite(temp, sizeof(temp), 1, fp);

  if (pCfg->replications >= 1) {
    fseek(fp, TSDB_FILE_HEADER_LEN * 3 / 4, SEEK_SET);
    fwrite(pDesc, sizeof(SVPeerDesc), pCfg->replications, fp);
  }

  /* vnodeUpdateFileCheckSum(fp); */
  fclose(fp);

  return 0;
}

int vnodeSaveVnodeInfo(int vnode) {
  FILE *     fp;
  SVnodeObj *pVnode = &vnodeList[vnode];

  fp = vnodeOpenMeterObjFile(vnode);
  if (fp == NULL) return -1;

  vnodeUpdateVnodeFileHeader(fp, pVnode);
  /* vnodeUpdateFileCheckSum(fp); */
  fclose(fp);

  return 0;
}

int vnodeRestoreMeterObj(char *buffer, int64_t length) {
  SMeterObj *pSavedObj, *pObj;
  int        size;

  pSavedObj = (SMeterObj *)buffer;
  if (pSavedObj->vnode < 0 || pSavedObj->vnode >= TSDB_MAX_VNODES) {
    dTrace("vid:%d is out of range, corrupted meter obj file", pSavedObj->vnode);
    return -1;
  }

  SVnodeCfg *pCfg = &vnodeList[pSavedObj->vnode].cfg;
  if (pSavedObj->sid < 0 || pSavedObj->sid >= pCfg->maxSessions) {
    dTrace("vid:%d, sid:%d is larger than max:%d", pSavedObj->vnode, pSavedObj->sid, pCfg->maxSessions);
    return -1;
  }

  if (pSavedObj->meterId[0] == 0) return TSDB_CODE_SUCCESS;

  size = sizeof(SMeterObj) + pSavedObj->sqlLen + 1;
  pObj = (SMeterObj *)malloc(size);
  if (pObj == NULL) {
    dError("vid:%d sid:%d, no memory to allocate", pSavedObj->vnode, pSavedObj->sid);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  memcpy(pObj, pSavedObj, offsetof(SMeterObj, reserved));
  vnodeList[pSavedObj->vnode].meterList[pSavedObj->sid] = pObj;
  pObj->numOfQueries = 0;
  pObj->pCache = vnodeAllocateCacheInfo(pObj);
  pObj->pStream = NULL;
  pObj->schema = (SColumn *)malloc(pSavedObj->numOfColumns * sizeof(SColumn));
  memcpy(pObj->schema, buffer + offsetof(SMeterObj, reserved), pSavedObj->numOfColumns * sizeof(SColumn));
  pObj->state = TSDB_METER_STATE_READY;

  if (pObj->sqlLen > 0)
    memcpy((char *)pObj + sizeof(SMeterObj),
           ((char *)pSavedObj) + offsetof(SMeterObj, reserved) + sizeof(SColumn) * pSavedObj->numOfColumns,
           pSavedObj->sqlLen);
  pObj->pSql = (char *)pObj + sizeof(SMeterObj);

  pObj->lastKey = pObj->lastKeyOnFile;
  if (pObj->lastKey > vnodeList[pObj->vnode].lastKey) vnodeList[pObj->vnode].lastKey = pObj->lastKey;

  //  taosSetSecurityInfo(pObj->vnode, pObj->sid, pObj->meterId, pObj->spi, pObj->encrypt, pObj->secret, pObj->cipheringKey);

  dTrace("vid:%d sid:%d id:%s, meter is restored, uid:%ld", pObj->vnode, pObj->sid, pObj->meterId, pObj->uid);
  return TSDB_CODE_SUCCESS;
}

int vnodeOpenMetersVnode(int vnode) {
  FILE *     fp;
  char *     buffer;
  int64_t    sid;
  int64_t    offset, length;
  SVnodeObj *pVnode = &vnodeList[vnode];

  fp = vnodeOpenMeterObjFile(vnode);
  if (fp == NULL) return 0;

  fseek(fp, TSDB_FILE_HEADER_VERSION_SIZE, SEEK_SET);
  fread(&(pVnode->vnodeStatistic), sizeof(SVnodeStatisticInfo), 1, fp);

  fseek(fp, TSDB_FILE_HEADER_LEN * 1 / 4, SEEK_SET);
  fscanf(fp, "%ld %ld %ld ", &(pVnode->lastCreate), &(pVnode->lastRemove), &(pVnode->version));
  fscanf(fp, "%ld %d %d ", &(pVnode->lastKeyOnFile), &(pVnode->fileId), &(pVnode->numOfFiles));

  fseek(fp, TSDB_FILE_HEADER_LEN * 2 / 4, SEEK_SET);
  fread(&pVnode->cfg, sizeof(SVnodeCfg), 1, fp);
  if (vnodeIsValidVnodeCfg(&pVnode->cfg) == false) {
    pVnode->cfg.maxSessions = 0;  // error in vnode file
    return 0;
  }

  fseek(fp, TSDB_FILE_HEADER_LEN * 3 / 4, SEEK_SET);
  fread(&pVnode->vpeers, sizeof(SVPeerDesc), TSDB_VNODES_SUPPORT, fp);

  fseek(fp, TSDB_FILE_HEADER_LEN, SEEK_SET);

  tsMeterSizeOnFile = sizeof(SMeterObj) + TSDB_MAX_COLUMNS * sizeof(SColumn) + TSDB_MAX_SAVED_SQL_LEN + sizeof(TSCKSUM);

  int size = sizeof(SMeterObj *) * pVnode->cfg.maxSessions;
  pVnode->meterList = (void *)malloc(size);
  if (pVnode->meterList == NULL) return -1;
  memset(pVnode->meterList, 0, size);
  size = sizeof(SMeterObjHeader) * pVnode->cfg.maxSessions + sizeof(TSCKSUM);
  pVnode->meterIndex = (SMeterObjHeader *)calloc(1, size);
  if (pVnode->meterIndex == NULL) {
    tfree(pVnode->meterList);
    return -1;
  }

  // Read SMeterObjHeader list from file
  if (fread(pVnode->meterIndex, size, 1, fp) < 0) return -1;
  // if (!taosCheckChecksumWhole(pVnode->meterIndex, size)) {
  //   dError("vid: %d meter obj file header is broken since checksum mismatch", vnode);
  //   return -1;
  // }

  // Read the meter object from file and recover the structure
  buffer = malloc(tsMeterSizeOnFile);
  memset(buffer, 0, tsMeterSizeOnFile);
  for (sid = 0; sid < pVnode->cfg.maxSessions; ++sid) {
    offset = pVnode->meterIndex[sid].offset;
    length = pVnode->meterIndex[sid].length;
    if (offset <= 0 || length <= 0) continue;

    fseek(fp, offset, SEEK_SET);
    if (fread(buffer, length, 1, fp) <= 0) break;
    if (taosCheckChecksumWhole((uint8_t *)buffer, length)) {
      vnodeRestoreMeterObj(buffer, length - sizeof(TSCKSUM));
    } else {
      dError("meter object file is broken since checksum mismatch, vnode: %d sid: %d, try to recover", vnode, sid);
      continue;
      /* vnodeRecoverMeterObjectFile(vnode); */
    }
  }

  tfree(buffer);
  fclose(fp);

  return 0;
}

void vnodeCloseMetersVnode(int vnode) {
  SVnodeObj *pVnode = vnodeList + vnode;
  SMeterObj *pObj;

  if (pVnode->meterList) {
    for (int sid = 0; sid < pVnode->cfg.maxSessions; ++sid) {
      pObj = pVnode->meterList[sid];
      if (pObj == NULL) continue;
      vnodeFreeCacheInfo(pObj);
      tfree(pObj->schema);
      tfree(pObj);
    }

    tfree(pVnode->meterList);
  }

  pVnode->meterList = NULL;
}

int vnodeCreateMeterObj(SMeterObj *pNew, SConnSec *pSec) {
  SMeterObj *pObj;
  int        code;

  pObj = vnodeList[pNew->vnode].meterList[pNew->sid];
  code = TSDB_CODE_SUCCESS;

  if (pObj && pObj->uid == pNew->uid) {
    if (pObj->sversion == pNew->sversion) {
      dTrace("vid:%d sid:%d id:%s sversion:%d, identical meterObj, ignore create", pNew->vnode, pNew->sid,
             pNew->meterId, pNew->sversion);
      return -1;
    }

    dTrace("vid:%d sid:%d id:%s, update schema", pNew->vnode, pNew->sid, pNew->meterId);
    if (!vnodeIsMeterState(pObj, TSDB_METER_STATE_UPDATING)) vnodeUpdateMeter(pNew, NULL);
    return TSDB_CODE_SUCCESS;
  }

  if (pObj) {
    dWarn("vid:%d sid:%d id:%s, old meter is there, remove it", pNew->vnode, pNew->sid, pNew->meterId);
    vnodeRemoveMeterObj(pNew->vnode, pNew->sid);
  }

  pNew->pCache = vnodeAllocateCacheInfo(pNew);
  if (pNew->pCache == NULL) {
    code = TSDB_CODE_NO_RESOURCE;
  } else {
    vnodeList[pNew->vnode].meterList[pNew->sid] = pNew;
    pNew->state = TSDB_METER_STATE_READY;
    if (pNew->timeStamp > vnodeList[pNew->vnode].lastCreate) vnodeList[pNew->vnode].lastCreate = pNew->timeStamp;
    vnodeSaveMeterObjToFile(pNew);
    // vnodeCreateMeterMgmt(pNew, pSec);
    vnodeCreateStream(pNew);
    dTrace("vid:%d sid:%d id:%s, meterObj is created, uid:%ld", pNew->vnode, pNew->sid, pNew->meterId, pNew->uid);
  }

  return code;
}

int vnodeRemoveMeterObj(int vnode, int sid) {
  SMeterObj *pObj;

  if (vnode < 0 || vnode >= TSDB_MAX_VNODES) {
    dError("vid:%d is out of range", vnode);
    return 0;
  }

  SVnodeCfg *pCfg = &vnodeList[vnode].cfg;
  if (sid < 0 || sid >= pCfg->maxSessions) {
    dError("vid:%d, sid:%d is larger than max:%d or less than 0", vnode, sid, pCfg->maxSessions);
    return 0;
  }

  // vnode has been closed, no meters in this vnode
  if (vnodeList[vnode].meterList == NULL) return 0;

  pObj = vnodeList[vnode].meterList[sid];
  if (pObj == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (!vnodeIsSafeToDeleteMeter(&vnodeList[vnode], sid)) {
    return TSDB_CODE_ACTION_IN_PROGRESS;
  }

  // after remove this meter, change its state to DELETED
  pObj->state = TSDB_METER_STATE_DELETED;
  pObj->timeStamp = taosGetTimestampMs();
  vnodeList[vnode].lastRemove = pObj->timeStamp;

  vnodeRemoveStream(pObj);
  vnodeSaveMeterObjToFile(pObj);
  vnodeFreeMeterObj(pObj);

  return 0;
}

int vnodeInsertPoints(SMeterObj *pObj, char *cont, int contLen, char source, void *param, int sversion,
                      int *numOfInsertPoints) {
  int         expectedLen, i;
  short       numOfPoints;
  SSubmitMsg *pSubmit = (SSubmitMsg *)cont;
  char *      pData;
  TSKEY       tsKey;
  int         cfile;
  int         points = 0;
  int         code = TSDB_CODE_SUCCESS;
  SVnodeObj * pVnode = vnodeList + pObj->vnode;

  numOfPoints = htons(pSubmit->numOfRows);
  expectedLen = numOfPoints * pObj->bytesPerPoint + sizeof(pSubmit->numOfRows);
  if (expectedLen != contLen) {
    dError("vid:%d sid:%d id:%s, invalid submit msg length:%d, expected:%d, bytesPerPoint: %d",
           pObj->vnode, pObj->sid, pObj->meterId, contLen, expectedLen, pObj->bytesPerPoint);
    code = TSDB_CODE_WRONG_MSG_SIZE;
    goto _over;
  }

  // to guarantee time stamp is the same for all vnodes
  pData = pSubmit->payLoad;
  tsKey = taosGetTimestamp(pVnode->cfg.precision);
  cfile = tsKey/pVnode->cfg.daysPerFile/tsMsPerDay[pVnode->cfg.precision];
  if (*((TSKEY *)pData) == 0) {
    for (i = 0; i < numOfPoints; ++i) {
      *((TSKEY *)pData) = tsKey++;
      pData += pObj->bytesPerPoint;
    }
  }

  if (numOfPoints >= (pVnode->cfg.blocksPerMeter - 2) * pObj->pointsPerBlock) {
    code = TSDB_CODE_BATCH_SIZE_TOO_BIG;
    dError("vid:%d sid:%d id:%s, batch size too big, it shall be smaller than:%d", pObj->vnode, pObj->sid,
           pObj->meterId, (pVnode->cfg.blocksPerMeter - 2) * pObj->pointsPerBlock);
    return code;
  }

  SCachePool *pPool = (SCachePool *)pVnode->pCachePool;
  if (pObj->freePoints < numOfPoints || pObj->freePoints < (pObj->pointsPerBlock << 1) ||
      pPool->notFreeSlots > pVnode->cfg.cacheNumOfBlocks.totalBlocks - 2) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
    dTrace("vid:%d sid:%d id:%s, cache is full, freePoints:%d, notFreeSlots:%d", pObj->vnode, pObj->sid, pObj->meterId,
           pObj->freePoints, pPool->notFreeSlots);
    vnodeProcessCommitTimer(pVnode, NULL);
    return TSDB_CODE_ACTION_IN_PROGRESS;
  }

  // FIXME: Here should be after the comparison of sversions.
  if (pVnode->cfg.commitLog && source != TSDB_DATA_SOURCE_LOG) {
    if (pVnode->logFd < 0) return TSDB_CODE_INVALID_COMMIT_LOG;
    code = vnodeWriteToCommitLog(pObj, TSDB_ACTION_INSERT, cont, contLen, sversion);
    if (code != 0) return code;
  }

  if (pObj->sversion < sversion) {
    dTrace("vid:%d sid:%d id:%s, schema is changed, new:%d old:%d", pObj->vnode, pObj->sid, pObj->meterId, sversion,
           pObj->sversion);
    vnodeSendMeterCfgMsg(pObj->vnode, pObj->sid);
    code = TSDB_CODE_ACTION_IN_PROGRESS;
    return code;
  }

  pData = pSubmit->payLoad;
  code = 0;

  TSKEY firstKey = *((TSKEY *)pData);
  int firstId = firstKey/pVnode->cfg.daysPerFile/tsMsPerDay[pVnode->cfg.precision];
  int lastId  = (*(TSKEY *)(pData + pObj->bytesPerPoint * (numOfPoints - 1)))/pVnode->cfg.daysPerFile/tsMsPerDay[pVnode->cfg.precision];
  if ((firstId <= cfile - pVnode->maxFiles) || (firstId > cfile + 1) || (lastId <= cfile - pVnode->maxFiles) || (lastId > cfile + 1)) {
    dError("vid:%d sid:%d id:%s, invalid timestamp to insert, firstKey: %ld lastKey: %ld ", pObj->vnode, pObj->sid,
           pObj->meterId, firstKey, (*(TSKEY *)(pData + pObj->bytesPerPoint * (numOfPoints - 1))));
    return TSDB_CODE_TIMESTAMP_OUT_OF_RANGE;
  }

  for (i = 0; i < numOfPoints; ++i) {
    // meter will be dropped, abort current insertion
    if (pObj->state >= TSDB_METER_STATE_DELETING) {
      dWarn("vid:%d sid:%d id:%s, meter is dropped, abort insert, state:%d", pObj->vnode, pObj->sid, pObj->meterId,
            pObj->state);

      code = TSDB_CODE_NOT_ACTIVE_SESSION;
      break;
    }

    if (*((TSKEY *)pData) <= pObj->lastKey) {
      dWarn("vid:%d sid:%d id:%s, received key:%ld not larger than lastKey:%ld", pObj->vnode, pObj->sid, pObj->meterId,
            *((TSKEY *)pData), pObj->lastKey);
      pData += pObj->bytesPerPoint;
      continue;
    }

    if (!VALID_TIMESTAMP(*((TSKEY *)pData), tsKey, pVnode->cfg.precision)) {
      code = TSDB_CODE_TIMESTAMP_OUT_OF_RANGE;
      break;
    }

    if (vnodeInsertPointToCache(pObj, pData) < 0) {
      code = TSDB_CODE_ACTION_IN_PROGRESS;
      break;
    }

    pObj->lastKey = *((TSKEY *)pData);
    pData += pObj->bytesPerPoint;
    points++;
  }
  __sync_fetch_and_add(&(pVnode->vnodeStatistic.pointsWritten), points * (pObj->numOfColumns - 1));
  __sync_fetch_and_add(&(pVnode->vnodeStatistic.totalStorage), points * pObj->bytesPerPoint);

  pthread_mutex_lock(&(pVnode->vmutex));

  if (pObj->lastKey > pVnode->lastKey) pVnode->lastKey = pObj->lastKey;

  if (firstKey < pVnode->firstKey) pVnode->firstKey = firstKey;

  pVnode->version++;

  pthread_mutex_unlock(&(pVnode->vmutex));

_over:
  dTrace("vid:%d sid:%d id:%s, %d out of %d points are inserted, lastKey:%ld source:%d, vnode total storage: %ld",
         pObj->vnode, pObj->sid, pObj->meterId, points, numOfPoints, pObj->lastKey, source,
         pVnode->vnodeStatistic.totalStorage);

  *numOfInsertPoints = points;
  return code;
}

void vnodeProcessUpdateSchemaTimer(void *param, void *tmrId) {
  SMeterObj * pObj = (SMeterObj *)param;
  SVnodeObj * pVnode = vnodeList + pObj->vnode;
  SCachePool *pPool = (SCachePool *)pVnode->pCachePool;

  pthread_mutex_lock(&pPool->vmutex);
  if (pPool->commitInProcess) {
    dTrace("vid:%d sid:%d mid:%s, committing in process, commit later", pObj->vnode, pObj->sid, pObj->meterId);
    if (taosTmrStart(vnodeProcessUpdateSchemaTimer, 10, pObj, vnodeTmrCtrl) == NULL) {
      vnodeClearMeterState(pObj, TSDB_METER_STATE_UPDATING);
    }

    pthread_mutex_unlock(&pPool->vmutex);
    return;
  }

  pPool->commitInProcess = 1;
  pthread_mutex_unlock(&pPool->vmutex);

  vnodeCommitMultiToFile(pVnode, pObj->sid, pObj->sid);
}

void vnodeUpdateMeter(void *param, void *tmrId) {
  SMeterObj *pNew = (SMeterObj *)param;
  if (pNew == NULL || pNew->vnode < 0 || pNew->sid < 0) return;

  SVnodeObj* pVnode = &vnodeList[pNew->vnode];

  if (pVnode->meterList == NULL) {
    dTrace("vid:%d sid:%d id:%s, vnode is deleted, abort update schema", pNew->vnode, pNew->sid, pNew->meterId);
    free(pNew->schema);
    free(pNew);
    return;
  }

  SMeterObj *pObj = pVnode->meterList[pNew->sid];
  if (pObj == NULL || vnodeIsMeterState(pObj, TSDB_METER_STATE_DELETING)) {
    dTrace("vid:%d sid:%d id:%s, meter is deleted, abort update schema", pNew->vnode, pNew->sid, pNew->meterId);
    free(pNew->schema);
    free(pNew);
    return;
  }

  int32_t state = vnodeSetMeterState(pObj, TSDB_METER_STATE_UPDATING);
  if (state >= TSDB_METER_STATE_DELETING) {
    dError("vid:%d sid:%d id:%s, meter is deleted, failed to update, state:%d",
           pObj->vnode, pObj->sid, pObj->meterId, state);
    return;
  }

  int32_t num = 0;
  pthread_mutex_lock(&pVnode->vmutex);
  num = pObj->numOfQueries;
  pthread_mutex_unlock(&pVnode->vmutex);

  if (num > 0 || state != TSDB_METER_STATE_READY) {
    dTrace("vid:%d sid:%d id:%s, update failed, retry later, numOfQueries:%d, state:%d",
           pNew->vnode, pNew->sid, pNew->meterId, num, state);

    // retry update meter in 50ms
    if (taosTmrStart(vnodeUpdateMeter, 50, pNew, vnodeTmrCtrl) == NULL) {
      dError("vid:%d sid:%d id:%s, failed to start update timer, no retry", pNew->vnode, pNew->sid, pNew->meterId);
      free(pNew->schema);
      free(pNew);
    }
    return;
  }

  // commit first
  if (!vnodeIsCacheCommitted(pObj)) {
    // commit data first
    if (taosTmrStart(vnodeProcessUpdateSchemaTimer, 0, pObj, vnodeTmrCtrl) == NULL) {
      dError("vid:%d sid:%d id:%s, failed to start commit timer", pObj->vnode, pObj->sid, pObj->meterId);
      vnodeClearMeterState(pObj, TSDB_METER_STATE_UPDATING);
      free(pNew->schema);
      free(pNew);
      return;
    }

    if (taosTmrStart(vnodeUpdateMeter, 50, pNew, vnodeTmrCtrl) == NULL) {
      dError("vid:%d sid:%d id:%s, failed to start update timer", pNew->vnode, pNew->sid, pNew->meterId);
      vnodeClearMeterState(pObj, TSDB_METER_STATE_UPDATING);
      free(pNew->schema);
      free(pNew);
    }

    dTrace("vid:%d sid:%d meterId:%s, there are data in cache, commit first, update later",
           pNew->vnode, pNew->sid, pNew->meterId);
    vnodeClearMeterState(pObj, TSDB_METER_STATE_UPDATING);
    return;
  }

  strcpy(pObj->meterId, pNew->meterId);
  pObj->numOfColumns = pNew->numOfColumns;
  pObj->timeStamp = pNew->timeStamp;
  pObj->bytesPerPoint = pNew->bytesPerPoint;
  pObj->maxBytes = pNew->maxBytes;
  if (pObj->timeStamp > vnodeList[pObj->vnode].lastCreate) vnodeList[pObj->vnode].lastCreate = pObj->timeStamp;

  tfree(pObj->schema);
  pObj->schema = pNew->schema;

  vnodeFreeCacheInfo(pObj);
  pObj->pCache = vnodeAllocateCacheInfo(pObj);

  pObj->sversion = pNew->sversion;
  vnodeSaveMeterObjToFile(pObj);
  vnodeClearMeterState(pObj, TSDB_METER_STATE_UPDATING);

  dTrace("vid:%d sid:%d id:%s, schema is updated", pNew->vnode, pNew->sid, pNew->meterId);
  free(pNew);
}

void vnodeRecoverMeterObjectFile(int vnode) {
  // TODO: start the recovery process
  assert(0);
}
