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
#include <fcntl.h>
#include <libgen.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "tscompression.h"
#include "tutil.h"
#include "vnode.h"
#include "vnodeFile.h"
#include "vnodeUtil.h"

#define FILE_QUERY_NEW_BLOCK -5  // a special negative number

const int16_t vnodeFileVersion = 0;

int (*pCompFunc[])(const char *const input, int inputSize, const int elements, char *const output, int outputSize,
                   char algorithm, char *const buffer, int bufferSize) = {NULL,
                                          tsCompressBool,
                                          tsCompressTinyint,
                                          tsCompressSmallint,
                                          tsCompressInt,
                                          tsCompressBigint,
                                          tsCompressFloat,
                                          tsCompressDouble,
                                          tsCompressString,
                                          tsCompressTimestamp,
                                          tsCompressString};

int (*pDecompFunc[])(const char *const input, int compressedSize, const int elements, char *const output,
                     int outputSize, char algorithm, char *const buffer, int bufferSize) = {NULL,
                                                            tsDecompressBool,
                                                            tsDecompressTinyint,
                                                            tsDecompressSmallint,
                                                            tsDecompressInt,
                                                            tsDecompressBigint,
                                                            tsDecompressFloat,
                                                            tsDecompressDouble,
                                                            tsDecompressString,
                                                            tsDecompressTimestamp,
                                                            tsDecompressString};

int vnodeUpdateFileMagic(int vnode, int fileId);
int vnodeRecoverCompHeader(int vnode, int fileId);
int vnodeRecoverHeadFile(int vnode, int fileId);
int vnodeRecoverDataFile(int vnode, int fileId);
int vnodeForwardStartPosition(SQuery *pQuery, SCompBlock *pBlock, int32_t slotIdx, SVnodeObj *pVnode, SMeterObj *pObj);

int64_t tsendfile(int dfd, int sfd, int64_t bytes) {
  int64_t leftbytes = bytes;
  off_t   offset = 0;
  int64_t sentbytes;

  while (leftbytes > 0) {
    sentbytes = (leftbytes > 1000000000) ? 1000000000 : leftbytes;
    sentbytes = sendfile(dfd, sfd, &offset, sentbytes);
    if (sentbytes < 0) {
      dError("send file failed,reason:%s", strerror(errno));
      return -1;
    }

    leftbytes -= sentbytes;
    // dTrace("sentbytes:%ld leftbytes:%ld", sentbytes, leftbytes);
  }

  return bytes;
}

void vnodeGetHeadDataLname(char *headName, char *dataName, char *lastName, int vnode, int fileId) {
  if (headName != NULL) sprintf(headName, "%s/vnode%d/db/v%df%d.head", tsDirectory, vnode, vnode, fileId);
  if (dataName != NULL) sprintf(dataName, "%s/vnode%d/db/v%df%d.data", tsDirectory, vnode, vnode, fileId);
  if (lastName != NULL) sprintf(lastName, "%s/vnode%d/db/v%df%d.last", tsDirectory, vnode, vnode, fileId);
}

void vnodeGetHeadDataDname(char *dHeadName, char *dDataName, char *dLastName, int vnode, int fileId, char *path) {
  if (dHeadName != NULL) sprintf(dHeadName, "%s/data/vnode%d/v%df%d.head0", path, vnode, vnode, fileId);
  if (dDataName != NULL) sprintf(dDataName, "%s/data/vnode%d/v%df%d.data", path, vnode, vnode, fileId);
  if (dLastName != NULL) sprintf(dLastName, "%s/data/vnode%d/v%df%d.last0", path, vnode, vnode, fileId);
}

void vnodeGetDnameFromLname(char *lhead, char *ldata, char *llast, char *dhead, char *ddata, char *dlast) {
  if (lhead != NULL) {
    assert(dhead != NULL);
    readlink(lhead, dhead, TSDB_FILENAME_LEN);
  }

  if (ldata != NULL) {
    assert(ddata != NULL);
    readlink(ldata, ddata, TSDB_FILENAME_LEN);
  }

  if (llast != NULL) {
    assert(dlast != NULL);
    readlink(llast, dlast, TSDB_FILENAME_LEN);
  }
}

void vnodeGetHeadTname(char *nHeadName, char *nLastName, int vnode, int fileId) {
  sprintf(nHeadName, "%s/vnode%d/db/v%df%d.t", tsDirectory, vnode, vnode, fileId);
  sprintf(nLastName, "%s/vnode%d/db/v%df%d.l", tsDirectory, vnode, vnode, fileId);
}

void vnodeCreateDataDirIfNeeded(int vnode, char *path) {
  char directory[TSDB_FILENAME_LEN] = "\0";

  sprintf(directory, "%s/data/vnode%d", path, vnode);

  if (access(directory, F_OK) != 0) mkdir(directory, 0755);
}

int vnodeCreateHeadDataFile(int vnode, int fileId, char *headName, char *dataName, char *lastName) {
  char dHeadName[TSDB_FILENAME_LEN];
  char dDataName[TSDB_FILENAME_LEN];
  char dLastName[TSDB_FILENAME_LEN];

  vnodeCreateDataDirIfNeeded(vnode, dataDir);

  vnodeGetHeadDataLname(headName, dataName, lastName, vnode, fileId);
  vnodeGetHeadDataDname(dHeadName, dDataName, dLastName, vnode, fileId, dataDir);
  if (symlink(dHeadName, headName) != 0) return -1;
  if (symlink(dDataName, dataName) != 0) return -1;
  if (symlink(dLastName, lastName) != 0) return -1;

  dTrace("vid:%d, fileId:%d, empty header file:%s dataFile:%s lastFile:%s on disk:%s is created ",
      vnode, fileId, headName, dataName, lastName, tsDirectory);

  return 0;
}

int vnodeCreateEmptyCompFile(int vnode, int fileId) {
  char  headName[TSDB_FILENAME_LEN];
  char  dataName[TSDB_FILENAME_LEN];
  char  lastName[TSDB_FILENAME_LEN];
  int   tfd;
  char *temp;

  if (vnodeCreateHeadDataFile(vnode, fileId, headName, dataName, lastName) < 0) {
    dError("failed to create head data file, vnode: %d, fileId: %d", vnode, fileId);
    return -1;
  }

  tfd = open(headName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  if (tfd < 0) {
    dError("failed to create head file:%s, reason:%s", headName, strerror(errno));
    return -1;
  }

  vnodeCreateFileHeaderFd(tfd);
  int size = sizeof(SCompHeader) * vnodeList[vnode].cfg.maxSessions + sizeof(TSCKSUM);
  temp = malloc(size);
  memset(temp, 0, size);
  taosCalcChecksumAppend(0, (uint8_t *)temp, size);

  lseek(tfd, TSDB_FILE_HEADER_LEN, SEEK_SET);
  write(tfd, temp, size);
  free(temp);
  close(tfd);

  tfd = open(dataName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  if (tfd < 0) {
    dError("failed to create data file:%s, reason:%s", dataName, strerror(errno));
    return -1;
  }
  vnodeCreateFileHeaderFd(tfd);
  close(tfd);

  tfd = open(lastName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  if (tfd < 0) {
    dError("failed to create last file:%s, reason:%s", lastName, strerror(errno));
    return -1;
  }
  vnodeCreateFileHeaderFd(tfd);
  close(tfd);

  return 0;
}

int vnodeOpenCommitFiles(SVnodeObj *pVnode, int noTempLast) {
  char        name[TSDB_FILENAME_LEN];
  char        dHeadName[TSDB_FILENAME_LEN] = "\0";
  char        dLastName[TSDB_FILENAME_LEN] = "\0";
  int         len = 0;
  struct stat filestat;
  int         vnode = pVnode->vnode;
  int         fileId, numOfFiles, filesAdded = 0;
  SVnodeCfg * pCfg = &pVnode->cfg;

  if (pVnode->lastKeyOnFile == 0) {
    if (pCfg->daysPerFile == 0) pCfg->daysPerFile = 10;
    pVnode->fileId = pVnode->firstKey / tsMsPerDay[pVnode->cfg.precision] / pCfg->daysPerFile;
    pVnode->lastKeyOnFile = (long)(pVnode->fileId + 1) * pCfg->daysPerFile * tsMsPerDay[pVnode->cfg.precision] - 1;
    pVnode->numOfFiles = 1;
    vnodeCreateEmptyCompFile(vnode, pVnode->fileId);
  }

  numOfFiles = (pVnode->lastKeyOnFile - pVnode->commitFirstKey) / tsMsPerDay[pVnode->cfg.precision] / pCfg->daysPerFile;
  if (pVnode->commitFirstKey > pVnode->lastKeyOnFile) numOfFiles = -1;

  dTrace("vid:%d, commitFirstKey:%ld lastKeyOnFile:%ld numOfFiles:%d fileId:%d vnodeNumOfFiles:%d",
      vnode, pVnode->commitFirstKey, pVnode->lastKeyOnFile, numOfFiles, pVnode->fileId, pVnode->numOfFiles);

  if (numOfFiles >= pVnode->numOfFiles) {
    // create empty header files backward
    filesAdded = numOfFiles - pVnode->numOfFiles + 1;
    for (int i = 0; i < filesAdded; ++i) {
      fileId = pVnode->fileId - pVnode->numOfFiles - i;
      if (vnodeCreateEmptyCompFile(vnode, fileId) < 0) return -1;
    }
  } else if (numOfFiles < 0) {
    // create empty header files forward
    pVnode->fileId++;
    if (vnodeCreateEmptyCompFile(vnode, pVnode->fileId) < 0) return -1;
    pVnode->lastKeyOnFile += (long)tsMsPerDay[pVnode->cfg.precision] * pCfg->daysPerFile;
    filesAdded = 1;
    numOfFiles = 0;  // hacker way
  }

  fileId = pVnode->fileId - numOfFiles;
  pVnode->commitLastKey =
      pVnode->lastKeyOnFile - (long)numOfFiles * tsMsPerDay[pVnode->cfg.precision] * pCfg->daysPerFile;
  pVnode->commitFirstKey = pVnode->commitLastKey - (long)tsMsPerDay[pVnode->cfg.precision] * pCfg->daysPerFile + 1;
  pVnode->commitFileId = fileId;
  pVnode->numOfFiles = pVnode->numOfFiles + filesAdded;

  dTrace("vid:%d, commit fileId:%d, commitLastKey:%ld, vnodeLastKey:%ld, lastKeyOnFile:%ld numOfFiles:%d",
      vnode, fileId, pVnode->commitLastKey, pVnode->lastKey, pVnode->lastKeyOnFile, pVnode->numOfFiles);

  int minSize = sizeof(SCompHeader) * pVnode->cfg.maxSessions + sizeof(TSCKSUM) + TSDB_FILE_HEADER_LEN;

  vnodeGetHeadDataLname(pVnode->cfn, name, pVnode->lfn, vnode, fileId);
  readlink(pVnode->cfn, dHeadName, TSDB_FILENAME_LEN);
  readlink(pVnode->lfn, dLastName, TSDB_FILENAME_LEN);
  len = strlen(dHeadName);
  if (dHeadName[len - 1] == 'd') {
    dHeadName[len] = '0';
    dHeadName[len + 1] = '\0';
  } else {
    dHeadName[len - 1] = '0' + (dHeadName[len - 1] + 1 - '0') % 2;
  }
  len = strlen(dLastName);
  if (dLastName[len - 1] == 't') {
    dLastName[len] = '0';
    dLastName[len + 1] = '\0';
  } else {
    dLastName[len - 1] = '0' + (dLastName[len - 1] + 1 - '0') % 2;
  }
  vnodeGetHeadTname(pVnode->nfn, pVnode->tfn, vnode, fileId);
  symlink(dHeadName, pVnode->nfn);
  if (!noTempLast) symlink(dLastName, pVnode->tfn);

  // open head file
  pVnode->hfd = open(pVnode->cfn, O_RDONLY);
  if (pVnode->hfd < 0) {
    dError("vid:%d, failed to open head file:%s, reason:%s", vnode, pVnode->cfn, strerror(errno));
    taosLogError("vid:%d, failed to open head file:%s, reason:%s", vnode, pVnode->cfn, strerror(errno));
    goto _error;
  }

  // verify head file, check size
  fstat(pVnode->hfd, &filestat);
  if (filestat.st_size < minSize) {
    dError("vid:%d, head file:%s corrupted", vnode, pVnode->cfn);
    taosLogError("vid:%d, head file:%s corrupted", vnode, pVnode->cfn);
    goto _error;
  }

  // open a new header file
  pVnode->nfd = open(pVnode->nfn, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  if (pVnode->nfd < 0) {
    dError("vid:%d, failed to open new head file:%s, reason:%s", vnode, pVnode->nfn, strerror(errno));
    taosLogError("vid:%d, failed to open new head file:%s, reason:%s", vnode, pVnode->nfn, strerror(errno));
    goto _error;
  }
  vnodeCreateFileHeaderFd(pVnode->nfd);

  // open existing data file
  pVnode->dfd = open(name, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  if (pVnode->dfd < 0) {
    dError("vid:%d, failed to open data file:%s, reason:%s", vnode, name, strerror(errno));
    taosLogError("vid:%d, failed to open data file:%s, reason:%s", vnode, name, strerror(errno));
    goto _error;
  }

  // verify data file, check size
  fstat(pVnode->dfd, &filestat);
  if (filestat.st_size < TSDB_FILE_HEADER_LEN) {
    dError("vid:%d, data file:%s corrupted", vnode, name);
    taosLogError("vid:%d, data file:%s corrupted", vnode, name);
    goto _error;
  } else {
    dTrace("vid:%d, data file:%s is opened to write", vnode, name);
  }

  // open last file
  pVnode->lfd = open(pVnode->lfn, O_RDWR);
  if (pVnode->lfd < 0) {
    dError("vid:%d, failed to open last file:%s, reason:%s", vnode, pVnode->lfn, strerror(errno));
    taosLogError("vid:%d, failed to open last file:%s, reason:%s", vnode, pVnode->lfn, strerror(errno));
    goto _error;
  }

  // verify last file, check size
  fstat(pVnode->lfd, &filestat);
  if (filestat.st_size < TSDB_FILE_HEADER_LEN) {
    dError("vid:%d, last file:%s corrupted", vnode, pVnode->lfn);
    taosLogError("vid:%d, last file:%s corrupted", vnode, pVnode->lfn);
    goto _error;
  }

  // open a new last file
  if (noTempLast) {
    pVnode->tfd = -1;  // do not open temporary last file
  } else {
    pVnode->tfd = open(pVnode->tfn, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
    if (pVnode->tfd < 0) {
      dError("vid:%d, failed to open new last file:%s, reason:%s", vnode, pVnode->tfn, strerror(errno));
      taosLogError("vid:%d, failed to open new last file:%s, reason:%s", vnode, pVnode->tfn, strerror(errno));
      goto _error;
    }
    vnodeCreateFileHeaderFd(pVnode->tfd);
    pVnode->lfSize = lseek(pVnode->tfd, 0, SEEK_END);
  }

  int   size = sizeof(SCompHeader) * pVnode->cfg.maxSessions + sizeof(TSCKSUM);
  char *temp = malloc(size);
  memset(temp, 0, size);
  taosCalcChecksumAppend(0, (uint8_t *)temp, size);
  write(pVnode->nfd, temp, size);
  free(temp);

  pVnode->dfSize = lseek(pVnode->dfd, 0, SEEK_END);

  return 0;

_error:
  if (pVnode->dfd > 0) close(pVnode->dfd);
  pVnode->dfd = 0;

  if (pVnode->hfd > 0) close(pVnode->hfd);
  pVnode->hfd = 0;

  if (pVnode->nfd > 0) close(pVnode->nfd);
  pVnode->nfd = 0;

  if (pVnode->lfd > 0) close(pVnode->lfd);
  pVnode->lfd = 0;

  if (pVnode->tfd > 0) close(pVnode->tfd);
  pVnode->tfd = 0;

  return -1;
}

void vnodeRemoveFile(int vnode, int fileId) {
  char           headName[TSDB_FILENAME_LEN] = "\0";
  char           dataName[TSDB_FILENAME_LEN] = "\0";
  char           lastName[TSDB_FILENAME_LEN] = "\0";
  char           dHeadName[TSDB_FILENAME_LEN] = "\0";
  char           dDataName[TSDB_FILENAME_LEN] = "\0";
  char           dLastName[TSDB_FILENAME_LEN] = "\0";
  SVnodeObj *    pVnode = NULL;
  SVnodeHeadInfo headInfo;

  pVnode = vnodeList + vnode;

  vnodeGetHeadDataLname(headName, dataName, lastName, vnode, fileId);
  vnodeGetDnameFromLname(headName, dataName, lastName, dHeadName, dDataName, dLastName);

  int fd = open(headName, O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd > 0) {
    vnodeGetHeadFileHeaderInfo(fd, &headInfo);
    __sync_fetch_and_add(&(pVnode->vnodeStatistic.totalStorage), -headInfo.totalStorage);
    close(fd);
  }

  remove(headName);
  remove(dataName);
  remove(lastName);
  remove(dHeadName);
  remove(dDataName);
  remove(dLastName);

  dTrace("vid:%d fileId:%d on disk: %s is removed, numOfFiles:%d maxFiles:%d", vnode, fileId, tsDirectory,
         pVnode->numOfFiles, pVnode->maxFiles);
}

void vnodeCloseCommitFiles(SVnodeObj *pVnode) {
  char dpath[TSDB_FILENAME_LEN] = "\0";
  int  fileId;
  int  ret;

  close(pVnode->nfd);
  pVnode->nfd = 0;

  close(pVnode->hfd);
  pVnode->hfd = 0;

  close(pVnode->dfd);
  pVnode->dfd = 0;

  close(pVnode->lfd);
  pVnode->lfd = 0;

  if (pVnode->tfd > 0) close(pVnode->tfd);

  pthread_mutex_lock(&(pVnode->vmutex));

  readlink(pVnode->cfn, dpath, TSDB_FILENAME_LEN);
  ret = rename(pVnode->nfn, pVnode->cfn);
  if (ret < 0) {
    dError("vid:%d, failed to rename:%s, reason:%s", pVnode->vnode, pVnode->nfn, strerror(errno));
  }
  remove(dpath);

  if (pVnode->tfd > 0) {
    memset(dpath, 0, TSDB_FILENAME_LEN);
    readlink(pVnode->lfn, dpath, TSDB_FILENAME_LEN);
    ret = rename(pVnode->tfn, pVnode->lfn);
    if (ret < 0) {
      dError("vid:%d, failed to rename:%s, reason:%s", pVnode->vnode, pVnode->tfn, strerror(errno));
    }
    remove(dpath);
  }

  pthread_mutex_unlock(&(pVnode->vmutex));

  pVnode->tfd = 0;

  dTrace("vid:%d, %s and %s is saved", pVnode->vnode, pVnode->cfn, pVnode->lfn);

  // Retention policy here
  fileId = pVnode->fileId - pVnode->numOfFiles + 1;
  int cfile = taosGetTimestamp(pVnode->cfg.precision)/pVnode->cfg.daysPerFile/tsMsPerDay[pVnode->cfg.precision];
  while (fileId <= cfile - pVnode->maxFiles) {
    vnodeRemoveFile(pVnode->vnode, fileId);
    pVnode->numOfFiles--;
    fileId++;
  }

  vnodeSaveAllMeterObjToFile(pVnode->vnode);

  return;
}

void vnodeBroadcastStatusToUnsyncedPeer(SVnodeObj *pVnode);

void *vnodeCommitMultiToFile(SVnodeObj *pVnode, int ssid, int esid) {
  int              vnode = pVnode->vnode;
  SData *          data[TSDB_MAX_COLUMNS], *cdata[TSDB_MAX_COLUMNS];  // first 4 bytes are length
  char *           buffer = NULL, *dmem = NULL, *cmem = NULL, *hmem = NULL, *tmem = NULL;
  SMeterObj *      pObj = NULL;
  SCompInfo        compInfo = {0};
  SCompHeader *    pHeader;
  SMeterInfo *     meterInfo = NULL, *pMeter = NULL;
  SQuery           query;
  SColumnFilter    colList[TSDB_MAX_COLUMNS] = {0};
  SSqlFunctionExpr pExprs[TSDB_MAX_COLUMNS] = {0};
  int              commitAgain;
  int              headLen, sid, col;
  long             pointsRead;
  long             pointsReadLast;
  SCompBlock *     pCompBlock = NULL;
  SVnodeCfg *      pCfg = &pVnode->cfg;
  TSCKSUM          chksum;
  SVnodeHeadInfo   headInfo;
  uint8_t *        pOldCompBlocks;

  dPrint("vid:%d, committing to file, firstKey:%ld lastKey:%ld ssid:%d esid:%d", vnode, pVnode->firstKey,
         pVnode->lastKey, ssid, esid);
  if (pVnode->lastKey == 0) goto _over;

  vnodeRenewCommitLog(vnode);

  // get the MAX consumption buffer for this vnode
  int32_t maxBytesPerPoint = 0;
  int32_t minBytesPerPoint = INT32_MAX;
  for (sid = ssid; sid <= esid; ++sid) {
    pObj = (SMeterObj *)(pVnode->meterList[sid]);
    if ((pObj == NULL) || (pObj->pCache == NULL)) continue;

    if (maxBytesPerPoint < pObj->bytesPerPoint) {
      maxBytesPerPoint = pObj->bytesPerPoint;
    }
    if (minBytesPerPoint > pObj->bytesPerPoint) {
      minBytesPerPoint = pObj->bytesPerPoint;
    }
  }

  // buffer to hold the temp head
  int tcachblocks = pCfg->cacheBlockSize / (minBytesPerPoint * pCfg->rowsInFileBlock);

  int hmsize =
      (pCfg->cacheNumOfBlocks.totalBlocks * (MAX(tcachblocks, 1) + 1) + pCfg->maxSessions) * sizeof(SCompBlock);

  // buffer to hold the uncompressed data
  int dmsize =
      maxBytesPerPoint * pCfg->rowsInFileBlock + (sizeof(SData) + EXTRA_BYTES + sizeof(TSCKSUM)) * TSDB_MAX_COLUMNS;

  // buffer to hold the compressed data
  int cmsize =
      maxBytesPerPoint * pCfg->rowsInFileBlock + (sizeof(SData) + EXTRA_BYTES + sizeof(TSCKSUM)) * TSDB_MAX_COLUMNS;

  // buffer to hold compHeader
  int tmsize = sizeof(SCompHeader) * pCfg->maxSessions + sizeof(TSCKSUM);

  // buffer to hold meterInfo
  int misize = pVnode->cfg.maxSessions * sizeof(SMeterInfo);

  int totalSize = hmsize + dmsize + cmsize + misize + tmsize;
  buffer = malloc(totalSize);
  if (buffer == NULL) {
    dError("no enough memory for committing buffer");
    return NULL;
  }

  hmem = buffer;
  dmem = hmem + hmsize;
  cmem = dmem + dmsize;
  tmem = cmem + cmsize;
  meterInfo = (SMeterInfo *)(tmem + tmsize);

  pthread_mutex_lock(&(pVnode->vmutex));
  pVnode->commitFirstKey = pVnode->firstKey;
  pVnode->firstKey = pVnode->lastKey + 1;
  pthread_mutex_unlock(&(pVnode->vmutex));

_again:
  pVnode->commitInProcess = 1;
  commitAgain = 0;
  memset(hmem, 0, totalSize);
  memset(&query, 0, sizeof(query));

  if (vnodeOpenCommitFiles(pVnode, ssid) < 0) goto _over;
  dTrace("vid:%d, start to commit, commitFirstKey:%ld commitLastKey:%ld", vnode, pVnode->commitFirstKey,
         pVnode->commitLastKey);

  headLen = 0;
  vnodeGetHeadFileHeaderInfo(pVnode->hfd, &headInfo);
  int maxOldBlocks = 1;

  // read head info
  if (pVnode->hfd) {
    lseek(pVnode->hfd, TSDB_FILE_HEADER_LEN, SEEK_SET);
    if (read(pVnode->hfd, tmem, tmsize) <= 0) {
      dError("vid:%d, failed to read old header file:%s", vnode, pVnode->cfn);
      taosLogError("vid:%d, failed to read old header file:%s", vnode, pVnode->cfn);
      goto _over;
    } else {
      if (!taosCheckChecksumWhole((uint8_t *)tmem, tmsize)) {
        dError("vid:%d, failed to read old header file:%s since comp header offset is broken", vnode, pVnode->cfn);
        taosLogError("vid:%d, failed to read old header file:%s since comp header offset is broken",
                     vnode, pVnode->cfn);
        goto _over;
      }
    }
  }

  // read compInfo
  for (sid = 0; sid < pCfg->maxSessions; ++sid) {
    if (pVnode->meterList == NULL) {  // vnode is being freed, abort
      goto _over;
    }

    pObj = (SMeterObj *)(pVnode->meterList[sid]);
    if (pObj == NULL) {
      continue;
    }

    // meter is going to be deleted, abort
    if (vnodeIsMeterState(pObj, TSDB_METER_STATE_DELETING)) {
      dWarn("vid:%d sid:%d is dropped, ignore this meter", vnode, sid);
      continue;
    }

    pMeter = meterInfo + sid;
    pHeader = ((SCompHeader *)tmem) + sid;

    if (pVnode->hfd > 0) {
      if (pHeader->compInfoOffset > 0) {
        lseek(pVnode->hfd, pHeader->compInfoOffset, SEEK_SET);
        if (read(pVnode->hfd, &compInfo, sizeof(compInfo)) == sizeof(compInfo)) {
          if (!taosCheckChecksumWhole((uint8_t *)(&compInfo), sizeof(SCompInfo))) {
            dError("vid:%d sid:%d id:%s, failed to read compinfo in file:%s since checksum mismatch",
                   vnode, sid, pObj->meterId, pVnode->cfn);
            taosLogError("vid:%d sid:%d id:%s, failed to read compinfo in file:%s since checksum mismatch",
                         vnode, sid, pObj->meterId, pVnode->cfn);
            goto _over;
          } else {
            if (pObj->uid == compInfo.uid) {
              pMeter->oldNumOfBlocks = compInfo.numOfBlocks;
              pMeter->oldCompBlockOffset = pHeader->compInfoOffset + sizeof(SCompInfo);
              pMeter->last = compInfo.last;
              if (compInfo.numOfBlocks > maxOldBlocks) maxOldBlocks = compInfo.numOfBlocks;
              if (pMeter->last) {
                lseek(pVnode->hfd, sizeof(SCompBlock) * (compInfo.numOfBlocks - 1), SEEK_CUR);
                read(pVnode->hfd, &pMeter->lastBlock, sizeof(SCompBlock));
              }
            } else {
              dTrace("vid:%d sid:%d id:%s, uid:%ld is not matched w/ old:%ld, old data will be thrown away",
                     vnode, sid, pObj->meterId, pObj->uid, compInfo.uid);
              pMeter->oldNumOfBlocks = 0;
            }
          }
        } else {
          dError("vid:%d sid:%d id:%s, failed to read compinfo in file:%s", vnode, sid, pObj->meterId, pVnode->cfn);
          goto _over;
        }
      }
    }
  }
  // Loop To write data to fileId
  for (sid = ssid; sid <= esid; ++sid) {
    pObj = (SMeterObj *)(pVnode->meterList[sid]);
    if ((pObj == NULL) || (pObj->pCache == NULL)) continue;

    data[0] = (SData *)dmem;
    cdata[0] = (SData *)cmem;
    for (col = 1; col < pObj->numOfColumns; ++col) {
      data[col] = (SData *)(((char *)data[col - 1]) + sizeof(SData) +
                            pObj->pointsPerFileBlock * pObj->schema[col - 1].bytes + EXTRA_BYTES + sizeof(TSCKSUM));
      cdata[col] = (SData *)(((char *)cdata[col - 1]) + sizeof(SData) +
                             pObj->pointsPerFileBlock * pObj->schema[col - 1].bytes + EXTRA_BYTES + sizeof(TSCKSUM));
    }

    pMeter = meterInfo + sid;
    pMeter->tempHeadOffset = headLen;

    memset(&query, 0, sizeof(query));
    query.colList = colList;
    query.pSelectExpr = pExprs;

    query.ekey = pVnode->commitLastKey;
    query.skey = pVnode->commitFirstKey;
    query.lastKey = query.skey;

    query.sdata = data;
    vnodeSetCommitQuery(pObj, &query);

    dTrace("vid:%d sid:%d id:%s, start to commit, startKey:%lld slot:%d pos:%d", pObj->vnode, pObj->sid, pObj->meterId,
           pObj->lastKeyOnFile, query.slot, query.pos);

    pointsRead = 0;
    pointsReadLast = 0;

    // last block is at last file
    if (pMeter->last) {
      if (pMeter->lastBlock.sversion != pObj->sversion) {
        // TODO : Check the correctness of this code. write the last block to
        // .data file
        pCompBlock = (SCompBlock *)(hmem + headLen);
        assert(dmem - (char *)pCompBlock >= sizeof(SCompBlock));
        *pCompBlock = pMeter->lastBlock;
        pCompBlock->last = 0;
        pCompBlock->offset = lseek(pVnode->dfd, 0, SEEK_END);
        lseek(pVnode->lfd, pMeter->lastBlock.offset, SEEK_SET);
        sendfile(pVnode->dfd, pVnode->lfd, NULL, pMeter->lastBlock.len);
        pVnode->dfSize = pCompBlock->offset + pMeter->lastBlock.len;

        headLen += sizeof(SCompBlock);
        pMeter->newNumOfBlocks++;
      } else {
        // read last block into memory
        if (vnodeReadLastBlockToMem(pObj, &pMeter->lastBlock, data) < 0) goto _over;
        pointsReadLast = pMeter->lastBlock.numOfPoints;
        query.over = 0;
        headInfo.totalStorage -= (pointsReadLast * pObj->bytesPerPoint);

        dTrace("vid:%d sid:%d id:%s, points:%d in last block will be merged to new block",
            pObj->vnode, pObj->sid, pObj->meterId, pointsReadLast);
      }

      pMeter->changed = 1;
      pMeter->last = 0;
      pMeter->oldNumOfBlocks--;
    }

    while (query.over == 0) {
      pCompBlock = (SCompBlock *)(hmem + headLen);
      assert(dmem - (char *)pCompBlock >= sizeof(SCompBlock));
      pointsRead += pointsReadLast;

      while (pointsRead < pObj->pointsPerFileBlock) {
        query.pointsToRead = pObj->pointsPerFileBlock - pointsRead;
        query.pointsOffset = pointsRead;
        pointsRead += vnodeQueryFromCache(pObj, &query);
        if (query.over) break;
      }

      if (pointsRead == 0) break;

      headInfo.totalStorage += ((pointsRead - pointsReadLast) * pObj->bytesPerPoint);
      pCompBlock->last = 1;
      if (vnodeWriteBlockToFile(pObj, pCompBlock, data, cdata, pointsRead) < 0) goto _over;
      if (pCompBlock->keyLast > pObj->lastKeyOnFile) pObj->lastKeyOnFile = pCompBlock->keyLast;
      pMeter->last = pCompBlock->last;

      // write block info into header buffer
      headLen += sizeof(SCompBlock);
      pMeter->newNumOfBlocks++;
      pMeter->committedPoints += (pointsRead - pointsReadLast);

      dTrace("vid:%d sid:%d id:%s, pointsRead:%d, pointsReadLast:%d lastKey:%lld, slot:%d pos:%d newNumOfBlocks:%d headLen:%d",
             pObj->vnode, pObj->sid, pObj->meterId, pointsRead, pointsReadLast, pObj->lastKeyOnFile, query.slot,
             query.pos, pMeter->newNumOfBlocks, headLen);

      if (pointsRead < pObj->pointsPerFileBlock || query.keyIsMet) break;

      pointsRead = 0;
      pointsReadLast = 0;
    }

    dTrace("vid:%d sid:%d id:%s, %d points are committed, lastKey:%lld slot:%d pos:%d newNumOfBlocks:%d",
        pObj->vnode, pObj->sid, pObj->meterId, pMeter->committedPoints, pObj->lastKeyOnFile, query.slot, query.pos,
        pMeter->newNumOfBlocks);

    if (pMeter->committedPoints > 0) {
      pMeter->commitSlot = query.slot;
      pMeter->commitPos = query.pos;
    }

    TSKEY nextKey = 0;
    if (pObj->lastKey > pVnode->commitLastKey)
      nextKey = pVnode->commitLastKey + 1;
    else if (pObj->lastKey > pObj->lastKeyOnFile)
      nextKey = pObj->lastKeyOnFile + 1;

    pthread_mutex_lock(&(pVnode->vmutex));
    if (nextKey < pVnode->firstKey && nextKey > 1) pVnode->firstKey = nextKey;
    pthread_mutex_unlock(&(pVnode->vmutex));
  }

  if (pVnode->lastKey > pVnode->commitLastKey) commitAgain = 1;

  dTrace("vid:%d, finish appending the data file", vnode);

  // calculate the new compInfoOffset
  int compInfoOffset = TSDB_FILE_HEADER_LEN + tmsize;
  for (sid = 0; sid < pCfg->maxSessions; ++sid) {
    pObj = (SMeterObj *)(pVnode->meterList[sid]);
    pHeader = ((SCompHeader *)tmem) + sid;
    if (pObj == NULL) {
      pHeader->compInfoOffset = 0;
      continue;
    }

    pMeter = meterInfo + sid;
    pMeter->compInfoOffset = compInfoOffset;
    pMeter->finalNumOfBlocks = pMeter->oldNumOfBlocks + pMeter->newNumOfBlocks;

    if (pMeter->finalNumOfBlocks > 0) {
      pHeader->compInfoOffset = pMeter->compInfoOffset;
      compInfoOffset += sizeof(SCompInfo) + pMeter->finalNumOfBlocks * sizeof(SCompBlock) + sizeof(TSCKSUM);
    }
    dTrace("vid:%d sid:%d id:%s, oldBlocks:%d numOfBlocks:%d compInfoOffset:%d", pObj->vnode, pObj->sid, pObj->meterId,
           pMeter->oldNumOfBlocks, pMeter->finalNumOfBlocks, compInfoOffset);
  }

  // write the comp header into new file
  vnodeUpdateHeadFileHeader(pVnode->nfd, &headInfo);
  lseek(pVnode->nfd, TSDB_FILE_HEADER_LEN, SEEK_SET);
  taosCalcChecksumAppend(0, (uint8_t *)tmem, tmsize);
  if (write(pVnode->nfd, tmem, tmsize) <= 0) {
    dError("vid:%d sid:%d id:%s, failed to write:%s, error:%s", vnode, sid, pObj->meterId, pVnode->nfn,
           strerror(errno));
    goto _over;
  }

  pOldCompBlocks = (uint8_t *)malloc(sizeof(SCompBlock) * maxOldBlocks);

  // write the comp block list in new file
  for (sid = 0; sid < pCfg->maxSessions; ++sid) {
    pObj = (SMeterObj *)(pVnode->meterList[sid]);
    if (pObj == NULL) continue;

    pMeter = meterInfo + sid;
    if (pMeter->finalNumOfBlocks <= 0) continue;

    compInfo.last = pMeter->last;
    compInfo.uid = pObj->uid;
    compInfo.numOfBlocks = pMeter->finalNumOfBlocks;
    /* compInfo.compBlockLen = pMeter->finalCompBlockLen; */
    compInfo.delimiter = TSDB_VNODE_DELIMITER;
    taosCalcChecksumAppend(0, (uint8_t *)(&compInfo), sizeof(SCompInfo));
    lseek(pVnode->nfd, pMeter->compInfoOffset, SEEK_SET);
    if (write(pVnode->nfd, &compInfo, sizeof(compInfo)) <= 0) {
      dError("vid:%d sid:%d id:%s, failed to write:%s, reason:%s", vnode, sid, pObj->meterId, pVnode->nfn,
             strerror(errno));
      goto _over;
    }

    // write the old comp blocks
    chksum = 0;
    if (pVnode->hfd && pMeter->oldNumOfBlocks) {
      lseek(pVnode->hfd, pMeter->oldCompBlockOffset, SEEK_SET);
      if (pMeter->changed) {
        int compBlockLen = pMeter->oldNumOfBlocks * sizeof(SCompBlock);
        read(pVnode->hfd, pOldCompBlocks, compBlockLen);
        write(pVnode->nfd, pOldCompBlocks, compBlockLen);
        chksum = taosCalcChecksum(0, pOldCompBlocks, compBlockLen);
      } else {
        sendfile(pVnode->nfd, pVnode->hfd, NULL, pMeter->oldNumOfBlocks * sizeof(SCompBlock));
        read(pVnode->hfd, &chksum, sizeof(TSCKSUM));
      }
    }

    if (pMeter->newNumOfBlocks) {
      chksum = taosCalcChecksum(chksum, (uint8_t *)(hmem + pMeter->tempHeadOffset),
                                pMeter->newNumOfBlocks * sizeof(SCompBlock));
      if (write(pVnode->nfd, hmem + pMeter->tempHeadOffset, pMeter->newNumOfBlocks * sizeof(SCompBlock)) <= 0) {
        dError("vid:%d sid:%d id:%s, failed to write:%s, reason:%s", vnode, sid, pObj->meterId, pVnode->nfn,
               strerror(errno));
        goto _over;
      }
    }
    write(pVnode->nfd, &chksum, sizeof(TSCKSUM));
  }

  tfree(pOldCompBlocks);
  dTrace("vid:%d, finish writing the new header file:%s", vnode, pVnode->nfn);
  vnodeCloseCommitFiles(pVnode);

  for (sid = ssid; sid <= esid; ++sid) {
    pObj = (SMeterObj *)(pVnode->meterList[sid]);
    if (pObj == NULL) continue;

    pMeter = meterInfo + sid;
    if (pMeter->finalNumOfBlocks <= 0) continue;

    if (pMeter->committedPoints > 0) {
      vnodeUpdateCommitInfo(pObj, pMeter->commitSlot, pMeter->commitPos, pMeter->commitCount);
    }
  }

  if (commitAgain) {
    pVnode->commitFirstKey = pVnode->commitLastKey + 1;
    goto _again;
  }

  vnodeRemoveCommitLog(vnode);

_over:
  pVnode->commitInProcess = 0;
  vnodeCommitOver(pVnode);
  memset(&(vnodeList[vnode].commitThread), 0, sizeof(vnodeList[vnode].commitThread));
  tfree(buffer);
  tfree(pOldCompBlocks);

  dPrint("vid:%d, committing is over", vnode);

  return pVnode;
}

void *vnodeCommitToFile(void *param) {
  SVnodeObj *pVnode = (SVnodeObj *)param;

  return vnodeCommitMultiToFile(pVnode, 0, pVnode->cfg.maxSessions - 1);
}

int vnodeGetCompBlockInfo(SMeterObj *pObj, SQuery *pQuery) {
  char        prefix[TSDB_FILENAME_LEN];
  char        fileName[TSDB_FILENAME_LEN];
  SCompHeader compHeader;
  SCompInfo   compInfo;
  struct stat fstat;
  SVnodeObj * pVnode = &vnodeList[pObj->vnode];
  char *      buffer = NULL;
  TSCKSUM     chksum;

  vnodeFreeFields(pQuery);
  tfree(pQuery->pBlock);

  pQuery->numOfBlocks = 0;
  SVnodeCfg *pCfg = &vnodeList[pObj->vnode].cfg;

  if (pQuery->hfd > 0) close(pQuery->hfd);
  sprintf(prefix, "%s/vnode%d/db/v%df%d", tsDirectory, pObj->vnode, pObj->vnode, pQuery->fileId);

  sprintf(fileName, "%s.head", prefix);
  pthread_mutex_lock(&(pVnode->vmutex));
  pQuery->hfd = open(fileName, O_RDONLY);
  pthread_mutex_unlock(&(pVnode->vmutex));

  if (pQuery->hfd < 0) {
    dError("vid:%d sid:%d id:%s, failed to open head file:%s, reason:%s",
           pObj->vnode, pObj->sid, pObj->meterId, fileName, strerror(errno));
    return -TSDB_CODE_FILE_CORRUPTED;
  }

  int tmsize = sizeof(SCompHeader) * pCfg->maxSessions + sizeof(TSCKSUM);
  buffer = (char *)calloc(1, tmsize);
  if (buffer == NULL) {
    dError("vid:%d sid:%d id:%s, failed to allocate memory to buffer", pObj->vnode, pObj->sid, pObj->meterId);
    return -TSDB_CODE_APP_ERROR;
  }

  lseek(pQuery->hfd, TSDB_FILE_HEADER_LEN, SEEK_SET);
  if (read(pQuery->hfd, buffer, tmsize) != tmsize) {
    dError("vid:%d sid:%d id:%s, file:%s failed to read comp header, reason:%s", pObj->vnode, pObj->sid, pObj->meterId,
           fileName, strerror(errno));
    taosLogError("vid:%d sid:%d id:%s, file:%s failed to read comp header", pObj->vnode, pObj->sid, pObj->meterId,
                 fileName);
    tfree(buffer);
    return -TSDB_CODE_FILE_CORRUPTED;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buffer, tmsize)) {
    dError("vid:%d sid:%d id:%s, file:%s comp header offset is broken", pObj->vnode, pObj->sid, pObj->meterId,
           fileName);
    taosLogError("vid:%d sid:%d id:%s, file:%s comp header offset is broken", pObj->vnode, pObj->sid, pObj->meterId,
                 fileName);
    tfree(buffer);
    return -TSDB_CODE_FILE_CORRUPTED;
  }
  compHeader = ((SCompHeader *)buffer)[pObj->sid];
  tfree(buffer);
  if (compHeader.compInfoOffset == 0) return 0;

  lseek(pQuery->hfd, compHeader.compInfoOffset, SEEK_SET);
  read(pQuery->hfd, &compInfo, sizeof(SCompInfo));
  if (!taosCheckChecksumWhole((uint8_t *)(&compInfo), sizeof(SCompInfo))) {
    dError("vid:%d sid:%d id:%s, file:%s compInfo checksum mismatch",
           pObj->vnode, pObj->sid, pObj->meterId, fileName);
    taosLogError("vid:%d sid:%d id:%s, file:%s compInfo checksum mismatch",
                 pObj->vnode, pObj->sid, pObj->meterId, fileName);
    return -TSDB_CODE_FILE_CORRUPTED;
  }
  if (compInfo.numOfBlocks <= 0) return 0;
  if (compInfo.uid != pObj->uid) return 0;

  pQuery->numOfBlocks = compInfo.numOfBlocks;
  pQuery->pBlock = (SCompBlock *)calloc(1, (sizeof(SCompBlock) + sizeof(SField *)) * compInfo.numOfBlocks);
  pQuery->pFields = (SField **)((char *)pQuery->pBlock + sizeof(SCompBlock) * compInfo.numOfBlocks);

  /* char *pBlock = (char *)pQuery->pBlockFields + sizeof(SCompBlockFields)*compInfo.numOfBlocks; */
  read(pQuery->hfd, pQuery->pBlock, compInfo.numOfBlocks * sizeof(SCompBlock));
  read(pQuery->hfd, &chksum, sizeof(TSCKSUM));
  if (chksum != taosCalcChecksum(0, (uint8_t *)(pQuery->pBlock),
                                 compInfo.numOfBlocks * sizeof(SCompBlock))) {
    dError("vid:%d sid:%d id:%s, head file comp block broken, fileId: %d",
           pObj->vnode, pObj->sid, pObj->meterId, pQuery->fileId);
    taosLogError("vid:%d sid:%d id:%s, head file comp block broken, fileId: %d",
                 pObj->vnode, pObj->sid, pObj->meterId, pQuery->fileId);
    return -TSDB_CODE_FILE_CORRUPTED;
  }

  close(pQuery->hfd);
  pQuery->hfd = -1;

  sprintf(fileName, "%s.data", prefix);
  if (stat(fileName, &fstat) < 0) {
    dError("vid:%d sid:%d id:%s, data file:%s not there!", pObj->vnode,
           pObj->sid, pObj->meterId, fileName);
    return -TSDB_CODE_FILE_CORRUPTED;
  }

  if (pQuery->dfd > 0) close(pQuery->dfd);
  pQuery->dfd = open(fileName, O_RDONLY);
  if (pQuery->dfd < 0) {
    dError("vid:%d sid:%d id:%s, failed to open data file:%s, reason:%s",
           pObj->vnode, pObj->sid, pObj->meterId, fileName, strerror(errno));
    return -TSDB_CODE_FILE_CORRUPTED;
  }

  sprintf(fileName, "%s.last", prefix);
  if (stat(fileName, &fstat) < 0) {
    dError("vid:%d sid:%d id:%s, last file:%s not there!", pObj->vnode,
           pObj->sid, pObj->meterId, fileName);
    return -TSDB_CODE_FILE_CORRUPTED;
  }

  if (pQuery->lfd > 0) close(pQuery->lfd);
  pQuery->lfd = open(fileName, O_RDONLY);
  if (pQuery->lfd < 0) {
    dError("vid:%d sid:%d id:%s, failed to open last file:%s, reason:%s",
           pObj->vnode, pObj->sid, pObj->meterId, fileName, strerror(errno));
    return -TSDB_CODE_FILE_CORRUPTED;
  }

  return pQuery->numOfBlocks;
}

int vnodeReadColumnToMem(int fd, SCompBlock *pBlock, SField **fields, int col, char *data, int dataSize,
                         char *temp, char *buffer, int bufferSize) {
  int     len = 0, size = 0;
  SField *tfields = NULL;
  TSCKSUM chksum = 0;

  if (*fields == NULL) {
    size = sizeof(SField) * (pBlock->numOfCols) + sizeof(TSCKSUM);
    *fields = (SField *)calloc(1, size);
    lseek(fd, pBlock->offset, SEEK_SET);
    read(fd, *fields, size);
    if (!taosCheckChecksumWhole((uint8_t *)(*fields), size)) {
      dError("SField checksum error, col: %d", col);
      taosLogError("SField checksum error, col: %d", col);
      return -1;
    }
  }

  tfields = *fields;

  /* If data is NULL, that means only to read SField content. So no need to read data part. */
  if (data == NULL) return 0;

  lseek(fd, pBlock->offset + tfields[col].offset, SEEK_SET);

  if (pBlock->algorithm) {
    len = read(fd, temp, tfields[col].len);
    read(fd, &chksum, sizeof(TSCKSUM));
    if (chksum != taosCalcChecksum(0, (uint8_t *)temp, tfields[col].len)) {
      dError("data column checksum error, col: %d", col);
      taosLogError("data column checksum error, col: %d", col);
      return -1;
    }

    (*pDecompFunc[tfields[col].type])(temp, tfields[col].len, pBlock->numOfPoints, data, dataSize,
                                      pBlock->algorithm, buffer, bufferSize);

  } else {
    len = read(fd, data, tfields[col].len);
    read(fd, &chksum, sizeof(TSCKSUM));
    if (chksum != taosCalcChecksum(0, (uint8_t *)data, tfields[col].len)) {
      dError("data column checksum error, col: %d", col);
      taosLogError("data column checksum error, col: %d", col);
      return -1;
    }
  }

  if (len <= 0) {
    dError("failed to read col:%d, offset:%ld, reason:%s", col, tfields[col].offset, strerror(errno));
    return -1;
  }

  return 0;
}

int vnodeReadCompBlockToMem(SMeterObj *pObj, SQuery *pQuery, SData *sdata[]) {
  char *      temp = NULL;
  int         i = 0, col = 0, code = 0;
  SCompBlock *pBlock = NULL;
  SField **   pFields = NULL;
  char *      buffer = NULL;
  int         bufferSize = 0;
  int         dfd = pQuery->dfd;

  tfree(pQuery->pFields[pQuery->slot]);

  pBlock = pQuery->pBlock + pQuery->slot;
  pFields = pQuery->pFields + pQuery->slot;
  temp = malloc(pObj->bytesPerPoint * (pBlock->numOfPoints + 1));

  if (pBlock->last) dfd = pQuery->lfd;

  if (pBlock->algorithm == TWO_STAGE_COMP) {
    bufferSize = pObj->maxBytes * pBlock->numOfPoints + EXTRA_BYTES;
    buffer = (char *)calloc(1, bufferSize);
  }

  if (pQuery->colList[0].colIdx != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    // load timestamp column first in any cases.
    code = vnodeReadColumnToMem(dfd, pBlock, pFields, PRIMARYKEY_TIMESTAMP_COL_INDEX,
                                pQuery->tsData->data + pQuery->pointsOffset * TSDB_KEYSIZE,
                                TSDB_KEYSIZE*pBlock->numOfPoints, temp, buffer, bufferSize);
    col = 1;
  } else {
    // Read the SField data for this block first, if timestamp column is retrieved in this query, we ignore this process
    code = vnodeReadColumnToMem(dfd, pBlock, pFields, 0, NULL, 0, NULL, buffer, bufferSize);
  }

  if (code < 0) goto _over;

  while (col < pBlock->numOfCols && i < pQuery->numOfCols) {
    SColumnFilterMsg *pColFilterMsg = &pQuery->colList[i].data;
    if ((*pFields)[col].colId < pColFilterMsg->colId) {
      ++col;
    } else if ((*pFields)[col].colId == pColFilterMsg->colId) {
      code = vnodeReadColumnToMem(dfd, pBlock, pFields, col, sdata[i]->data, pColFilterMsg->bytes*pBlock->numOfPoints,
                                  temp, buffer, bufferSize);
      if (code < 0) goto _over;
      ++i;
      ++col;
    } else {
      /*
       * pQuery->colList[i].colIdx < (*pFields)[col].colId, this column is not existed in current block,
       * fill space with NULL value
       */
      char *  output = sdata[i]->data;
      int32_t bytes = pQuery->colList[i].data.bytes;
      int32_t type = pQuery->colList[i].data.type;

      setNullN(output, type, bytes, pBlock->numOfPoints);
      ++i;
    }
  }

  if (col >= pBlock->numOfCols && i < pQuery->numOfCols) {
    // remain columns need to set null value
    while (i < pQuery->numOfCols) {
      char *  output = sdata[i]->data;
      int32_t bytes = pQuery->colList[i].data.bytes;
      int32_t type = pQuery->colList[i].data.type;

      setNullN(output, type, bytes, pBlock->numOfPoints);
      ++i;
    }
  }

_over:
  tfree(buffer);
  tfree(temp);
  if ( code < 0 ) code = -TSDB_CODE_FILE_CORRUPTED;
  return code;
}

int vnodeReadLastBlockToMem(SMeterObj *pObj, SCompBlock *pBlock, SData *sdata[]) {
  char *  temp = NULL;
  int     col = 0, code = 0;
  SField *pFields = NULL;
  char *  buffer = NULL;
  int     bufferSize = 0;

  SVnodeObj *pVnode = vnodeList + pObj->vnode;
  temp = malloc(pObj->bytesPerPoint * (pBlock->numOfPoints + 1));
  if (pBlock->algorithm == TWO_STAGE_COMP) {
    bufferSize = pObj->maxBytes*pBlock->numOfPoints+EXTRA_BYTES;
    buffer = (char *)calloc(1, pObj->maxBytes * pBlock->numOfPoints + EXTRA_BYTES);
  }

  for (col = 0; col < pBlock->numOfCols; ++col) {
    code = vnodeReadColumnToMem(pVnode->lfd, pBlock, &pFields, col, sdata[col]->data, 
                                pObj->pointsPerFileBlock*pObj->schema[col].bytes+EXTRA_BYTES, temp, buffer, bufferSize);
    if (code < 0) break;
    sdata[col]->len = pObj->schema[col].bytes * pBlock->numOfPoints;
  }

  tfree(buffer);
  tfree(temp);
  tfree(pFields);
  return code;
}

int vnodeWriteBlockToFile(SMeterObj *pObj, SCompBlock *pCompBlock, SData *data[], SData *cdata[], int points) {
  SVnodeObj *pVnode = &vnodeList[pObj->vnode];
  SVnodeCfg *pCfg = &pVnode->cfg;
  int        wlen = 0;
  SField *   fields = NULL;
  int        size = sizeof(SField) * pObj->numOfColumns + sizeof(TSCKSUM);
  int32_t    offset = size;
  char *     buffer = NULL;
  int        bufferSize = 0;

  int dfd = pVnode->dfd;

  if (pCompBlock->last && (points < pObj->pointsPerFileBlock * tsFileBlockMinPercent)) {
    dTrace("vid:%d sid:%d id:%s, points:%d are written to last block, block stime: %ld, block etime: %ld",
           pObj->vnode, pObj->sid, pObj->meterId, points, *((TSKEY *)(data[0]->data)),
           *((TSKEY * )(data[0]->data + (points - 1) * pObj->schema[0].bytes)));
    pCompBlock->last = 1;
    dfd = pVnode->tfd > 0 ? pVnode->tfd : pVnode->lfd;
  } else {
    pCompBlock->last = 0;
  }

  pCompBlock->offset = lseek(dfd, 0, SEEK_END);
  pCompBlock->len = 0;

  fields = (SField *)calloc(1, size);
  if (fields == NULL) return -1;

  if (pCfg->compression == TWO_STAGE_COMP){
    bufferSize = pObj->maxBytes * points + EXTRA_BYTES;
    buffer = (char *)malloc(bufferSize);
  } 

  for (int i = 0; i < pObj->numOfColumns; ++i) {
    fields[i].colId = pObj->schema[i].colId;
    fields[i].type = pObj->schema[i].type;
    fields[i].bytes = pObj->schema[i].bytes;
    fields[i].offset = offset;
    // assert(data[i]->len == points*pObj->schema[i].bytes);

    if (pCfg->compression) {
      cdata[i]->len = (*pCompFunc[pObj->schema[i].type])(data[i]->data, points * pObj->schema[i].bytes, points,
                                                         cdata[i]->data, pObj->schema[i].bytes*pObj->pointsPerFileBlock+EXTRA_BYTES, 
                                                         pCfg->compression, buffer, bufferSize);
      fields[i].len = cdata[i]->len;
      taosCalcChecksumAppend(0, (uint8_t *)(cdata[i]->data), cdata[i]->len + sizeof(TSCKSUM));
      offset += (cdata[i]->len + sizeof(TSCKSUM));

    } else {
      fields[i].len = data[i]->len;
      taosCalcChecksumAppend(0, (uint8_t *)(data[i]->data), data[i]->len + sizeof(TSCKSUM));
      offset += (data[i]->len + sizeof(TSCKSUM));
    }

    getStatistics(data[0]->data, data[i]->data, pObj->schema[i].bytes, points, pObj->schema[i].type, &fields[i].min,
                  &fields[i].max, &fields[i].sum, &fields[i].wsum, &fields[i].numOfNullPoints);
  }

  tfree(buffer);

  // Write SField part
  taosCalcChecksumAppend(0, (uint8_t *)fields, size);
  wlen = write(dfd, fields, size);
  if (wlen <= 0) {
    tfree(fields);
    dError("vid:%d sid:%d id:%s, failed to write block, wlen:%d reason:%s", pObj->vnode, pObj->sid, pObj->meterId, wlen,
           strerror(errno));
    return -1;
  }
  pVnode->vnodeStatistic.compStorage += wlen;
  pVnode->dfSize += wlen;
  pCompBlock->len += wlen;
  tfree(fields);

  // Write data part
  for (int i = 0; i < pObj->numOfColumns; ++i) {
    if (pCfg->compression) {
      wlen = write(dfd, cdata[i]->data, cdata[i]->len + sizeof(TSCKSUM));
    } else {
      wlen = write(dfd, data[i]->data, data[i]->len + sizeof(TSCKSUM));
    }

    if (wlen <= 0) {
      dError("vid:%d sid:%d id:%s, failed to write block, wlen:%d points:%d reason:%s",
             pObj->vnode, pObj->sid, pObj->meterId, wlen, points, strerror(errno));
      return -TSDB_CODE_FILE_CORRUPTED;
    }

    pVnode->vnodeStatistic.compStorage += wlen;
    pVnode->dfSize += wlen;
    pCompBlock->len += wlen;
  }

  dTrace("vid: %d vnode compStorage size is: %ld", pObj->vnode, pVnode->vnodeStatistic.compStorage);

  pCompBlock->algorithm = pCfg->compression;
  pCompBlock->numOfPoints = points;
  pCompBlock->numOfCols = pObj->numOfColumns;
  pCompBlock->keyFirst = *((TSKEY *)(data[0]->data));  // hack way to get the key
  pCompBlock->keyLast = *((TSKEY *)(data[0]->data + (points - 1) * pObj->schema[0].bytes));
  pCompBlock->sversion = pObj->sversion;

  return 0;
}

static int forwardInFile(SQuery *pQuery, int32_t midSlot, int32_t step, SVnodeObj *pVnode, SMeterObj *pObj);

int vnodeSearchPointInFile(SMeterObj *pObj, SQuery *pQuery) {
  TSKEY       latest, oldest;
  int         ret = 0;
  long        delta = 0;
  int         firstSlot, lastSlot, midSlot;
  int         numOfBlocks;
  char *      temp = NULL, *data = NULL;
  SCompBlock *pBlock = NULL;
  SVnodeObj * pVnode = &vnodeList[pObj->vnode];
  int         step;
  char *      buffer = NULL;
  int         bufferSize = 0;
  int         dfd;

  // if file is broken, pQuery->slot = -2; if not found, pQuery->slot = -1;

  pQuery->slot = -1;
  pQuery->pos = -1;
  if (pVnode->numOfFiles <= 0) return 0;

  SVnodeCfg *pCfg = &pVnode->cfg;
  delta = (long)pCfg->daysPerFile * tsMsPerDay[pVnode->cfg.precision];
  latest = pObj->lastKeyOnFile;
  oldest = (pVnode->fileId - pVnode->numOfFiles + 1) * delta;

  if (latest < oldest) return 0;

  if (!QUERY_IS_ASC_QUERY(pQuery)) {
    if (pQuery->skey < oldest) return 0;
    if (pQuery->ekey > latest) return 0;
    if (pQuery->skey > latest) pQuery->skey = latest;
  } else {
    if (pQuery->skey > latest) return 0;
    if (pQuery->ekey < oldest) return 0;
    if (pQuery->skey < oldest) pQuery->skey = oldest;
  }

  dTrace("vid:%d sid:%d id:%s, skey:%ld ekey:%ld oldest:%ld latest:%ld fileId:%d numOfFiles:%d",
         pObj->vnode, pObj->sid, pObj->meterId, pQuery->skey, pQuery->ekey, oldest, latest, pVnode->fileId,
         pVnode->numOfFiles);

  step = QUERY_IS_ASC_QUERY(pQuery) ? 1 : -1;

  pQuery->fileId = pQuery->skey / delta;  // starting fileId
  pQuery->fileId -= step;                 // hacker way to make while loop below works

  bufferSize = pCfg->rowsInFileBlock*sizeof(TSKEY)+EXTRA_BYTES;
  buffer = (char *)calloc(1, bufferSize);

  while (1) {
    pQuery->fileId += step;

    if ((pQuery->fileId > pVnode->fileId) || (pQuery->fileId < pVnode->fileId - pVnode->numOfFiles + 1)) {
      tfree(buffer);
      return 0;
    }

    ret = vnodeGetCompBlockInfo(pObj, pQuery);
    if (ret == 0) continue;
    if (ret < 0) break;  // file broken

    pBlock = pQuery->pBlock;

    firstSlot = 0;
    lastSlot = pQuery->numOfBlocks - 1;
    numOfBlocks = pQuery->numOfBlocks;
    if (QUERY_IS_ASC_QUERY(pQuery) && pBlock[lastSlot].keyLast < pQuery->skey) continue;
    if (!QUERY_IS_ASC_QUERY(pQuery) && pBlock[firstSlot].keyFirst > pQuery->skey) continue;

    while (1) {
      numOfBlocks = lastSlot - firstSlot + 1;
      midSlot = (firstSlot + (numOfBlocks >> 1));

      if (numOfBlocks == 1) break;

      if (pQuery->skey > pBlock[midSlot].keyLast) {
        if (numOfBlocks == 2) break;
        if (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->skey < pBlock[midSlot + 1].keyFirst)) break;
        firstSlot = midSlot + 1;
      } else if (pQuery->skey < pBlock[midSlot].keyFirst) {
        if (QUERY_IS_ASC_QUERY(pQuery) && (pQuery->skey > pBlock[midSlot - 1].keyLast)) break;
        lastSlot = midSlot - 1;
      } else {
        break;  // got the slot
      }
    }

    pQuery->slot = midSlot;
    if (!QUERY_IS_ASC_QUERY(pQuery)) {
      if (pQuery->skey < pBlock[midSlot].keyFirst) break;

      if (pQuery->ekey > pBlock[midSlot].keyLast) {
        pQuery->slot = midSlot + 1;
        break;
      }
    } else {
      if (pQuery->skey > pBlock[midSlot].keyLast) {
        pQuery->slot = midSlot + 1;
        break;
      }

      if (pQuery->ekey < pBlock[midSlot].keyFirst) break;
    }

    temp = malloc(pObj->pointsPerFileBlock * TSDB_KEYSIZE + EXTRA_BYTES);  // only first column
    data = malloc(pObj->pointsPerFileBlock * TSDB_KEYSIZE + EXTRA_BYTES);  // only first column
    dfd = pBlock[midSlot].last ? pQuery->lfd : pQuery->dfd;
    ret = vnodeReadColumnToMem(dfd, pBlock + midSlot, pQuery->pFields + midSlot, 0, data,
                               pObj->pointsPerFileBlock*TSDB_KEYSIZE+EXTRA_BYTES,
                               temp, buffer, bufferSize);
    if (ret < 0) {
      ret = -TSDB_CODE_FILE_CORRUPTED;
      break;
    }  // file broken

    pQuery->pos = (*vnodeSearchKeyFunc[pObj->searchAlgorithm])(data, pBlock[midSlot].numOfPoints, pQuery->skey,
                                                               pQuery->order.order);
    pQuery->key = *((TSKEY *)(data + pObj->schema[0].bytes * pQuery->pos));

    ret = vnodeForwardStartPosition(pQuery, pBlock, midSlot, pVnode, pObj);
    break;
  }

  tfree(buffer);
  tfree(temp);
  tfree(data);

  return ret;
}

int vnodeForwardStartPosition(SQuery *pQuery, SCompBlock *pBlock, int32_t slotIdx, SVnodeObj *pVnode, SMeterObj *pObj) {
  int step = QUERY_IS_ASC_QUERY(pQuery) ? 1 : -1;

  if (pQuery->limit.offset > 0 && pQuery->numOfFilterCols == 0) {
    int maxReads = QUERY_IS_ASC_QUERY(pQuery) ? pBlock->numOfPoints - pQuery->pos : pQuery->pos + 1;

    if (pQuery->limit.offset < maxReads) {  // start position in current block
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        pQuery->pos += pQuery->limit.offset;
      } else {
        pQuery->pos -= pQuery->limit.offset;
      }

      pQuery->limit.offset = 0;

    } else {
      pQuery->limit.offset -= maxReads;
      slotIdx += step;

      return forwardInFile(pQuery, slotIdx, step, pVnode, pObj);
    }
  }

  return pQuery->numOfBlocks;
}

int forwardInFile(SQuery *pQuery, int32_t slotIdx, int32_t step, SVnodeObj *pVnode, SMeterObj *pObj) {
  SCompBlock *pBlock = pQuery->pBlock;

  while (slotIdx < pQuery->numOfBlocks && slotIdx >= 0 && pQuery->limit.offset >= pBlock[slotIdx].numOfPoints) {
    pQuery->limit.offset -= pBlock[slotIdx].numOfPoints;
    slotIdx += step;
  }

  if (slotIdx < pQuery->numOfBlocks && slotIdx >= 0) {
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      pQuery->pos = pQuery->limit.offset;
    } else {
      pQuery->pos = pBlock[slotIdx].numOfPoints - pQuery->limit.offset - 1;
    }
    pQuery->slot = slotIdx;
    pQuery->limit.offset = 0;

    return pQuery->numOfBlocks;
  } else {  // continue in next file, forward pQuery->limit.offset points
    int ret = 0;
    pQuery->slot = -1;
    pQuery->pos = -1;

    while (1) {
      pQuery->fileId += step;
      if ((pQuery->fileId > pVnode->fileId) || (pQuery->fileId < pVnode->fileId - pVnode->numOfFiles + 1)) {
        pQuery->lastKey = pObj->lastKeyOnFile;
        pQuery->skey = pQuery->lastKey + 1;
        return 0;
      }

      ret = vnodeGetCompBlockInfo(pObj, pQuery);
      if (ret == 0) continue;
      if (ret > 0) break;  // qualified file
    }

    if (ret > 0) {
      int startSlot = QUERY_IS_ASC_QUERY(pQuery) ? 0 : pQuery->numOfBlocks - 1;
      return forwardInFile(pQuery, startSlot, step, pVnode, pObj);
    } else {
      return ret;
    }
  }
}

static FORCE_INLINE TSKEY vnodeGetTSInDataBlock(SQuery *pQuery, int32_t pos, int32_t factor) {
  return *(TSKEY *)(pQuery->tsData->data + (pQuery->pointsOffset * factor + pos) * TSDB_KEYSIZE);
}

int vnodeQueryFromFile(SMeterObj *pObj, SQuery *pQuery) {
  int numOfReads = 0;

  int         lastPos = -1, startPos;
  int         col, step, code = 0;
  char *      pRead, *pData;
  char *      buffer;
  SData *     sdata[TSDB_MAX_COLUMNS];
  SCompBlock *pBlock = NULL;
  SVnodeObj * pVnode = &vnodeList[pObj->vnode];
  pQuery->pointsRead = 0;
  int keyLen = TSDB_KEYSIZE;

  if (pQuery->over) return 0;

  if (pQuery->slot < 0)  // it means a new query, we need to find the point first
    code = vnodeSearchPointInFile(pObj, pQuery);

  if (code < 0 || pQuery->slot < 0 || pQuery->pos == -1) {
    pQuery->over = 1;
    return code;
  }

  step = QUERY_IS_ASC_QUERY(pQuery) ? -1 : 1;
  pBlock = pQuery->pBlock + pQuery->slot;

  if (pQuery->pos == FILE_QUERY_NEW_BLOCK) {
    if (!QUERY_IS_ASC_QUERY(pQuery)) {
      if (pQuery->ekey > pBlock->keyLast) pQuery->over = 1;
      if (pQuery->skey < pBlock->keyFirst) pQuery->over = 1;
    } else {
      if (pQuery->ekey < pBlock->keyFirst) pQuery->over = 1;
      if (pQuery->skey > pBlock->keyLast) pQuery->over = 1;
    }

    pQuery->pos = QUERY_IS_ASC_QUERY(pQuery) ? 0 : pBlock->numOfPoints - 1;
  }

  if (pQuery->over) return 0;

  // allocate memory more efficiently
  buffer = calloc(1, pQuery->dataRowSize * pBlock->numOfPoints + sizeof(SData) * pQuery->numOfCols);
  sdata[0] = (SData *)buffer;
  for (col = 1; col < pQuery->numOfCols; ++col)
    sdata[col] =
        (SData *)(((char *)sdata[col - 1]) + sizeof(SData) + pBlock->numOfPoints * pQuery->colList[col - 1].data.bytes);

  /*
   * timestamp column is fetched in any cases. Therefore, if the query does not fetch primary column,
   * we allocate tsData buffer with twice size of the other ordinary pQuery->sdata.
   * Otherwise, the query function may over-write buffer area while retrieve function has not packed the results into
   * message to send to client yet.
   *
   * So the startPositionFactor is needed to denote which half part is used to store the result, and which
   * part is available for keep data during query process.
   *
   * Note: the startPositionFactor must be used in conjunction with pQuery->pointsOffset
   */
  int32_t startPositionFactor = 1;
  if (pQuery->colList[0].colIdx == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    pQuery->tsData = sdata[0];
    startPositionFactor = 0;
  }

  code = vnodeReadCompBlockToMem(pObj, pQuery, sdata);
  if (code < 0) {
    dError("vid:%d sid:%d id:%s, failed to read block:%d numOfPoints:%d", pObj->vnode, pObj->sid, pObj->meterId,
           pQuery->slot, pBlock->numOfPoints);
    goto _next;
  }

  int maxReads = QUERY_IS_ASC_QUERY(pQuery) ? pBlock->numOfPoints - pQuery->pos : pQuery->pos + 1;

  TSKEY startKey = vnodeGetTSInDataBlock(pQuery, 0, startPositionFactor);
  TSKEY endKey = vnodeGetTSInDataBlock(pQuery, pBlock->numOfPoints - 1, startPositionFactor);

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    if (endKey < pQuery->ekey) {
      numOfReads = maxReads;
    } else {
      lastPos = (*vnodeSearchKeyFunc[pObj->searchAlgorithm])(
          pQuery->tsData->data + keyLen * (pQuery->pos + pQuery->pointsOffset * startPositionFactor), maxReads,
          pQuery->ekey, TSQL_SO_DESC);
      numOfReads = (lastPos >= 0) ? lastPos + 1 : 0;
    }
  } else {
    if (startKey > pQuery->ekey) {
      numOfReads = maxReads;
    } else {
      lastPos = (*vnodeSearchKeyFunc[pObj->searchAlgorithm])(
          pQuery->tsData->data + keyLen * pQuery->pointsOffset * startPositionFactor, maxReads, pQuery->ekey,
          TSQL_SO_ASC);
      numOfReads = (lastPos >= 0) ? pQuery->pos - lastPos + 1 : 0;
    }
  }

  if (numOfReads > pQuery->pointsToRead - pQuery->pointsRead) {
    numOfReads = pQuery->pointsToRead - pQuery->pointsRead;
  } else {
    if (lastPos >= 0 || numOfReads == 0) {
      pQuery->keyIsMet = 1;
      pQuery->over = 1;
    }
  }

  startPos = QUERY_IS_ASC_QUERY(pQuery) ? pQuery->pos : pQuery->pos - numOfReads + 1;

  int32_t numOfQualifiedPoints = 0;
  int32_t numOfActualRead = numOfReads;

  // copy data to result buffer
  if (pQuery->numOfFilterCols == 0) {
    // no filter condition on ordinary columns
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      int16_t colBufferIndex = pQuery->pSelectExpr[i].pBase.colInfo.colIdxInBuf;
      int32_t bytes = GET_COLUMN_BYTES(pQuery, i);

      pData = pQuery->sdata[i]->data + pQuery->pointsOffset * bytes;
      pRead = sdata[colBufferIndex]->data + startPos * bytes;

      memcpy(pData, pRead, numOfReads * bytes);
    }

    numOfQualifiedPoints = numOfReads;

  } else {
    // check each data one by one set the input column data
    for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
      SColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];
      pFilterInfo->pData = sdata[pFilterInfo->pFilter.colIdxInBuf]->data;
    }

    int32_t *ids = calloc(1, numOfReads * sizeof(int32_t));
    numOfActualRead = 0;

    if (QUERY_IS_ASC_QUERY(pQuery)) {
      for (int32_t j = startPos; j < pBlock->numOfPoints; j -= step) {
        TSKEY key = vnodeGetTSInDataBlock(pQuery, j, startPositionFactor);
        if (key < startKey || key > endKey) {
          dError("vid:%d sid:%d id:%s, timestamp in file block disordered. slot:%d, pos:%d, ts:%lld, block "
                 "range:%lld-%lld", pObj->vnode, pObj->sid, pObj->meterId, pQuery->slot, j, key, startKey, endKey);
          tfree(ids);
          return -TSDB_CODE_FILE_BLOCK_TS_DISORDERED;
        }

        // out of query range, quit
        if (key > pQuery->ekey) {
          break;
        }

        if (!vnodeFilterData(pQuery, &numOfActualRead, j)) {
          continue;
        }

        ids[numOfQualifiedPoints] = j;
        if (++numOfQualifiedPoints == numOfReads) {
          // qualified data are enough
          break;
        }
      }
    } else {
      for (int32_t j = pQuery->pos; j >= 0; --j) {
        TSKEY key = vnodeGetTSInDataBlock(pQuery, j, startPositionFactor);
        if (key < startKey || key > endKey) {
          dError("vid:%d sid:%d id:%s, timestamp in file block disordered. slot:%d, pos:%d, ts:%lld, block "
                 "range:%lld-%lld", pObj->vnode, pObj->sid, pObj->meterId, pQuery->slot, j, key, startKey, endKey);
          tfree(ids);
          return -TSDB_CODE_FILE_BLOCK_TS_DISORDERED;
        }

        // out of query range, quit
        if (key < pQuery->ekey) {
          break;
        }

        if (!vnodeFilterData(pQuery, &numOfActualRead, j)) {
          continue;
        }

        ids[numOfReads - numOfQualifiedPoints - 1] = j;
        if (++numOfQualifiedPoints == numOfReads) {
          // qualified data are enough
          break;
        }
      }
    }

    int32_t start = QUERY_IS_ASC_QUERY(pQuery) ? 0 : numOfReads - numOfQualifiedPoints;
    for (int32_t j = 0; j < numOfQualifiedPoints; ++j) {
      for (int32_t col = 0; col < pQuery->numOfOutputCols; ++col) {
        int16_t colIndexInBuffer = pQuery->pSelectExpr[col].pBase.colInfo.colIdxInBuf;
        int32_t bytes = GET_COLUMN_BYTES(pQuery, col);
        pData = pQuery->sdata[col]->data + (pQuery->pointsOffset + j) * bytes;
        pRead = sdata[colIndexInBuffer]->data + ids[j + start] * bytes;

        memcpy(pData, pRead, bytes);
      }
    }

    tfree(ids);
    assert(numOfQualifiedPoints <= numOfReads);
  }

  // Note: numOfQualifiedPoints may be 0, since no data in this block are qualified
  assert(pQuery->pointsRead == 0);

  pQuery->pointsRead += numOfQualifiedPoints;
  for (col = 0; col < pQuery->numOfOutputCols; ++col) {
    int16_t bytes = GET_COLUMN_BYTES(pQuery, col);
    pQuery->sdata[col]->len = bytes * (pQuery->pointsOffset + pQuery->pointsRead);
  }
  pQuery->pos -= numOfActualRead * step;

  // update the lastkey/skey
  int32_t lastAccessPos = pQuery->pos + step;
  pQuery->lastKey = vnodeGetTSInDataBlock(pQuery, lastAccessPos, startPositionFactor);
  pQuery->skey = pQuery->lastKey - step;

_next:
  if ((pQuery->pos < 0 || pQuery->pos >= pBlock->numOfPoints || numOfReads == 0) && (pQuery->over == 0)) {
    pQuery->slot = pQuery->slot - step;
    pQuery->pos = FILE_QUERY_NEW_BLOCK;
  }

  if ((pQuery->slot < 0 || pQuery->slot >= pQuery->numOfBlocks) && (pQuery->over == 0)) {
    int ret;

    while (1) {
      ret = -1;
      pQuery->fileId -= step;  // jump to next file

      if (QUERY_IS_ASC_QUERY(pQuery)) {
        if (pQuery->fileId > pVnode->fileId) {
          // to do:
          // check if file is updated, if updated, open again and check if this Meter is updated
          // if meter is updated, read in new block info, and
          break;
        }
      } else {
        if ((pVnode->fileId - pQuery->fileId + 1) > pVnode->numOfFiles) break;
      }

      ret = vnodeGetCompBlockInfo(pObj, pQuery);
      if (ret > 0) break;
      if (ret < 0) code = ret;
    }

    if (ret <= 0) pQuery->over = 1;

    pQuery->slot = QUERY_IS_ASC_QUERY(pQuery) ? 0 : pQuery->numOfBlocks - 1;
  }

  tfree(buffer);

  return code;
}

int vnodeUpdateFileMagic(int vnode, int fileId) {
  struct stat fstat;
  char        fileName[256];

  SVnodeObj *pVnode = vnodeList + vnode;
  uint64_t   magic = 0;

  vnodeGetHeadDataLname(fileName, NULL, NULL, vnode, fileId);
  if (stat(fileName, &fstat) != 0) {
    dError("vid:%d, head file:%s is not there", vnode, fileName);
    return -1;
  }

  int size = sizeof(SCompHeader) * pVnode->cfg.maxSessions + sizeof(TSCKSUM) + TSDB_FILE_HEADER_LEN;
  if (fstat.st_size < size) {
    dError("vid:%d, head file:%s is corrupted", vnode, fileName);
    return -1;
  }

  if (fstat.st_size == size) return 0;

  vnodeGetHeadDataLname(NULL, fileName, NULL, vnode, fileId);
  if (stat(fileName, &fstat) == 0) {
    magic = fstat.st_size;
  } else {
    dError("vid:%d, data file:%s is not there", vnode, fileName);
    return -1;
  }

  vnodeGetHeadDataLname(NULL, NULL, fileName, vnode, fileId);
  if (stat(fileName, &fstat) == 0) {
    magic += fstat.st_size;
  }

  int slot = fileId % pVnode->maxFiles;
  pVnode->fmagic[slot] = magic;

  return 0;
}

int vnodeInitFile(int vnode) {
  int        code = 0;
  SVnodeObj *pVnode = vnodeList + vnode;

  pVnode->maxFiles = pVnode->cfg.daysToKeep / pVnode->cfg.daysPerFile + 1;
  pVnode->maxFile1 = pVnode->cfg.daysToKeep1 / pVnode->cfg.daysPerFile;
  pVnode->maxFile2 = pVnode->cfg.daysToKeep2 / pVnode->cfg.daysPerFile;
  pVnode->fmagic = (uint64_t *)calloc(pVnode->maxFiles + 1, sizeof(uint64_t));
  int fileId = pVnode->fileId;

  for (int i = 0; i < pVnode->numOfFiles; ++i) {
    if (vnodeUpdateFileMagic(vnode, fileId) < 0) {
      if (pVnode->cfg.replications > 1) {
        pVnode->badFileId = fileId;
      }
      dError("vid:%d fileId:%d is corrupted", vnode, fileId);
    } else {
      dTrace("vid:%d fileId:%d is checked", vnode, fileId);
    }

    fileId--;
  }

  return code;
}

int vnodeRecoverCompHeader(int vnode, int fileId) {
  // TODO: try to recover SCompHeader part
  dTrace("starting to recover vnode head file comp header part, vnode: %d fileId: %d", vnode, fileId);
  assert(0);
  return 0;
}

int vnodeRecoverHeadFile(int vnode, int fileId) {
  // TODO: try to recover SCompHeader part
  dTrace("starting to recover vnode head file, vnode: %d, fileId: %d", vnode, fileId);
  assert(0);
  return 0;
}

int vnodeRecoverDataFile(int vnode, int fileId) {
  // TODO: try to recover SCompHeader part
  dTrace("starting to recover vnode data file, vnode: %d, fileId: %d", vnode, fileId);
  assert(0);
  return 0;
}
