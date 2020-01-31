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
#include <arpa/inet.h>
#include <assert.h>
#include <fcntl.h>
#include <libgen.h>
#include <sys/stat.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "vnode.h"
#include "ttier.h"

int  vnodeUpdateFileMagic(int vnode, int fileId);
void vnodeGetHeadDataLname(char *headName, char *dataName, char *lastName, int vnode, int fileId);
void vnodeGetHeadDataDname(char *dHeadName, char *dDataName, char *dLastName, int vnode, int fileId, char *path);
int  vnodeCreateHeadDataFile(int vnode, int fileId, char *headName, char *dataName, char *lastName);
void vnodeCreateDataDirIfNeeded(int vnode, char *path);
void vnodeRemoveFile(int vnode, int fileId);


#define TSDB_SYNC_FILE_EMPTY        0
#define TSDB_SYNC_FILE_NOT_EXIST   -1
#define TSDB_SYNC_FILE_FINISHED    -2
#define TSDB_SYNC_FILE_NEED_REMOVE -3

TIERID vnodeGetTierIdByFileId(int vnode, int fileId) {
  SVnodeObj *pVnode = vnodeList + vnode;

  if (pVnode->numOfFiles == 0 || pVnode->fileId - pVnode->maxFile1 < fileId) {
    return 0;
  }

  if (pVnode->fileId - pVnode->maxFile2 < fileId) {
    if (diskTier.numOfTiers >= 2)
      return 1;
    else
      return 0;
  }

  if (diskTier.numOfTiers >= 3) {
    return 2;
  }

  if (diskTier.numOfTiers >= 2) {
    return 1;
  }

  return 0;
}

char* vnodeGetDataDir(int vnode, int fileId) {
  TIERID tid = vnodeGetTierIdByFileId(vnode, fileId);
  assert(tid < diskTier.numOfTiers);
  DISKID did = taosAllocDiskOnTier(tid);
  SDisk *disk = taosGetDiskByID(tid, did);
  if (disk == NULL) {
    return NULL;
  }
  return disk->path;
}

char* vnodeGetDiskFromHeadFile(char *headName) {
  SDisk *        disk = NULL;
  disk = taosGetDiskFromHeadFile(headName);
  if (disk == NULL) {
    return NULL;
  }

  __sync_fetch_and_sub(&(disk->numOfFiles), 1);
  return disk->path;
}

void vnodeMoveFileBetweenTier(int vnode, int fileId, TIERID tierTo) {
  char        headName[TSDB_FILENAME_LEN] = "\0";
  char        dataName[TSDB_FILENAME_LEN] = "\0";
  char        lastName[TSDB_FILENAME_LEN] = "\0";
  char        dHeadName[TSDB_FILENAME_LEN] = "\0";
  char        dDataName[TSDB_FILENAME_LEN] = "\0";
  char        dLastName[TSDB_FILENAME_LEN] = "\0";
  int         fdFrom, fdTo;
  DISKID      did;
  struct stat fileStat;

  if (tierTo >= diskTier.numOfTiers) {
    dError("vid: %d trying to move file fileId %d to tier %d, numOfTiers %d", vnode, fileId, tierTo, diskTier.numOfTiers);
    return;
  }

  vnodeGetHeadDataLname(headName, dataName, lastName, vnode, fileId);
  SDisk *disk = taosGetDiskFromHeadFile(headName);
  if (disk == NULL) return;

  assert(disk->diskId.tid <= tierTo);
  if (disk->diskId.tid == tierTo) return;

  did = taosAllocDiskOnTier(tierTo);
  if (did == -1) return;

  SDisk *diskTo = taosGetDiskByID(tierTo, did);
  vnodeCreateDataDirIfNeeded(vnode, diskTo->path);

  vnodeGetHeadDataDname(dHeadName, dDataName, dLastName, vnode, fileId, diskTo->path);

  fdFrom = open(headName, O_RDONLY);
  fdTo = open(dHeadName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  fstat(fdFrom, &fileStat);
  tsendfile(fdTo, fdFrom, NULL, fileStat.st_size);
  close(fdFrom);
  close(fdTo);

  fdFrom = open(dataName, O_RDONLY);
  fdTo = open(dDataName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  fstat(fdFrom, &fileStat);
  tsendfile(fdTo, fdFrom, NULL, fileStat.st_size);
  close(fdFrom);
  close(fdTo);

  fdFrom = open(lastName, O_RDONLY);
  fdTo = open(dLastName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  fstat(fdFrom, &fileStat);
  tsendfile(fdTo, fdFrom, NULL, fileStat.st_size);
  close(fdFrom);
  close(fdTo);

  // TODO : make it atomic
  vnodeRemoveFile(vnode, fileId);
  symlink(dHeadName, headName);
  symlink(dDataName, dataName);
  symlink(dLastName, lastName);

  __sync_fetch_and_sub(&(disk->numOfFiles), 1);

  dTrace("vid: %d file move between tier, fileId %d tierFrom %d diskFrom %d tierTo %d diskTo %d",
          vnode, fileId, disk->diskId.tid, disk->diskId.did, tierTo, diskTo->diskId.did);
}

void vnodeAdustVnodeFile(SVnodeObj *pVnode) {
  int fileId;
  int  file_removed = 0;
  if (pVnode->numOfFiles > pVnode->maxFiles) {
    fileId = pVnode->fileId - pVnode->numOfFiles + 1;
    vnodeRemoveFile(pVnode->vnode, fileId);
    pVnode->numOfFiles--;
    file_removed = 1;
  }

  if (diskTier.numOfTiers >= 3 && pVnode->numOfFiles > pVnode->maxFile2) {
    fileId = pVnode->fileId - pVnode->maxFile2;
    vnodeMoveFileBetweenTier(pVnode->vnode, fileId, 2);
    /* for (int i = 0; i < pVnode->numOfFiles - pVnode->maxFile2; i++) { */
    /*     fileId = pVnode->fileId - pVnode->maxFile2 + i; */
    /*     vnodeMoveFileBetweenTier(vnode, fileId, 2); */
    /* } */
  }

  if (diskTier.numOfTiers >= 2 && pVnode->numOfFiles > pVnode->maxFile1) {
    fileId = pVnode->fileId - pVnode->maxFile1;
    vnodeMoveFileBetweenTier(pVnode->vnode, fileId, 1);
    /* int numOfFiles = (pVnode->numOfFiles > pVnode->maxFile2) ?
     * (pVnode->maxFile2 - pVnode->maxFile1) : (pVnode->numOfFiles -
     * pVnode->maxFile1); */
    /* for (int i = 0; i < numOfFiles; i++) { */
    /*     fileId = pVnode->fileId - pVnode->maxFile1 + i; */
    /*     vnodeMoveFileBetweenTier(vnode, fileId, 1); */
    /* } */
  }

  if (!file_removed) {
    vnodeUpdateFileMagic(pVnode->vnode, pVnode->commitFileId);
  }
}

int vnodeSyncRetrieveFile(int vnode, int fd, uint32_t peerFid, uint64_t *fmagic) {
  SVnodeObj * pVnode;
  char        headName[TSDB_FILENAME_LEN];
  char        dataName[TSDB_FILENAME_LEN];
  char        lastName[TSDB_FILENAME_LEN];
  int32_t     fileId;
  int64_t     size;
  struct stat fstat;
  int         sfd;

  pVnode = vnodeList + vnode;

  if (pVnode->numOfFiles <= 0){
    pVnode->fileId = pVnode->firstKey / pVnode->cfg.daysPerFile / tsMsPerDay[(uint8_t)pVnode->cfg.precision];
  }

  dPrint("vid:%d, fd:%d, start sync retrieve, numOfFiles:%d", vnode, fd, pVnode->numOfFiles);

  if (peerFid > 0) {
    int minFId = pVnode->fileId - pVnode->maxFiles + 1;
    fileId = peerFid - peerFid % pVnode->maxFiles;

    for (int i = 0; i < pVnode->maxFiles; ++i, ++fileId) {
      if (fileId > peerFid) fileId -= pVnode->maxFiles;
      if (fileId < minFId) {
        dPrint("vid:%d, peer fileId:%d is too old, set magic to 0", vnode, fileId);
        fmagic[i] = 0;
      }
    }
  }

  fileId = pVnode->fileId - pVnode->fileId % pVnode->maxFiles;

  for (int i = 0; i < pVnode->maxFiles; ++i, ++fileId) {
    if (fileId > pVnode->fileId) fileId -= pVnode->maxFiles;

    // file is the same
    if (pVnode->fmagic[i] == fmagic[i]) {
      //dTrace("vid:%d, fileId:%d, skip retrieve, fmagic:%ld peer fmagic:%ld is same", vnode, fileId, pVnode->fmagic[i], fmagic[i]);
      continue;
    }

    dPrint("vid:%d, fileId:%d, start to retrieve, fmagic:%ld peer fmagic:%ld not equal", vnode, fileId, pVnode->fmagic[i], fmagic[i]);

    if (pVnode->fmagic[i] == 0 && fmagic[i] != 0) {
      // file not exist
      size = TSDB_SYNC_FILE_NEED_REMOVE;
      dTrace("vid:%d, fileId:%d, file not exist, send head file size:%ld to peer", vnode, fileId, size);
      if (taosWriteMsg(fd, &(fileId), sizeof(fileId)) < 0) {
        dError("vid:%d, fileId:%d, failed to send fileId to peer", vnode, fileId);
        return -1;
      }
      if (taosWriteMsg(fd, &size, sizeof(size)) < 0) {
        dError("vid:%d, fileId:%d, failed to send head file size:%ld to peer", vnode, fileId, size);
        return -1;
      }
    } else {
      // file different

      assert(fileId > 0);
      vnodeGetHeadDataLname(headName, dataName, lastName, vnode, fileId);

      // send head file first
      dPrint("vid:%d, fileId:%d, try to send head file:%s to peer", vnode, fileId, headName);
      if (taosWriteMsg(fd, &(fileId), sizeof(fileId)) < 0) {
        dError("vid:%d, fileId:%d, failed to send fileId to peer", vnode, fileId);
        return -1;
      }

      if (stat(headName, &fstat) < 0) {
        if (errno == ENOENT) {
          dPrint("vid:%d, fileId:%d, failed to stat head file:%s, reason:%s", vnode, fileId, headName, strerror(errno));
          size = TSDB_SYNC_FILE_NOT_EXIST;
        } else {
          dError("vid:%d, fileId:%d, failed to stat head file:%s, reason:%s", vnode, fileId, headName, strerror(errno));
          return -1;
        }
      } else {
        size = fstat.st_size;
      }

      if (taosWriteMsg(fd, &size, sizeof(size)) < 0) {
        dError("vid:%d, fileId:%d, failed to send head file:%s size:%ld to peer", vnode, fileId, headName, size);
        return -1;
      }

      if (size > 0) {
        pthread_mutex_lock(&(pVnode->vmutex));
        sfd = open(headName, O_RDONLY);
        pthread_mutex_unlock(&(pVnode->vmutex));
        if (sfd < 0) {
          dError("vid:%d, fileId:%d, failed to send head file:%s size:%d to peer for sfd:% invalid", vnode, fileId, headName, size, sfd);
          return -1;
        }

        if (tsendfile(fd, sfd, NULL, size) < 0) {
          dError("vid:%d, fileId:%d, failed to send head file:%s size:%d to peer, reason:%s", vnode, fileId, headName, size, strerror(errno));
          close(sfd);
          return -1;
        }

        dPrint("vid:%d, fileId:%d, head file:%s is sent to peer, size:%ld", vnode, fileId, headName, size);
        close(sfd);
      } else {
        dPrint("vid:%d, fileId:%d, head file:%s size:%ld is empty, not sent to peer", vnode, fileId, headName, size);
      }

      // send data file
      dPrint("vid:%d, fileId:%d, try to send data file:%s to peer", vnode, fileId, dataName);
      if (stat(dataName, &fstat) < 0) {
        if (errno == ENOENT) {
          dPrint("vid:%d, fileId:%d, failed to stat data file:%s, reason:%s", vnode, fileId, dataName, strerror(errno));
          size = TSDB_SYNC_FILE_NOT_EXIST;
        } else {
          dError("vid:%d, fileId:%d, failed to stat data file:%s, reason:%s", vnode, fileId, dataName, strerror(errno));
          return -1;
        }
      } else {
        size = fstat.st_size;
      }

      if (taosWriteMsg(fd, &size, sizeof(size)) < 0) {
        dError("vid:%d, fileId:%d, failed to send data file:%s size:%ld to peer", vnode, fileId, dataName, size);
        return -1;
      }

      if (size > 0) {
        sfd = open(dataName, O_RDONLY);
        if (sfd < 0) {
          dError("vid:%d, fileId:%d, failed to send data file:%s size:%ld to peer for sfd:% invalid", vnode, fileId,
                 dataName, size, sfd);
          return -1;
        }

        if (tsendfile(fd, sfd, NULL, size) < 0) {
          dError("vid:%d, fileId:%d, failed to send data file:%s size:%ld to peer, reason:%s", vnode, fileId, dataName,
                 size, strerror(errno));
          close(sfd);
          return -1;
        }

        dPrint("vid:%d, fileId:%d, data file:%s is sent to peer, size:%ld", vnode, fileId, dataName, size);
        close(sfd);
      }
      else {
        dPrint("vid:%d, fileId:%d, data file:%s size:%ld is empty, not sent to peer", vnode, fileId, dataName, size);
      }

      // send last file
      dPrint("vid:%d, fileId:%d, try to send last file:%s to peer", vnode, fileId, lastName);
      if (stat(lastName, &fstat) < 0) {
        if (errno == ENOENT) {
          dPrint("vid:%d, fileId:%d, failed to stat last file:%s, reason:%s", vnode, fileId, lastName, strerror(errno));
          size = TSDB_SYNC_FILE_NOT_EXIST;
        } else {
          dError("vid:%d, fileId:%d, failed to stat last file:%s, reason:%s", vnode, fileId, lastName, strerror(errno));
          return -1;
        }
      } else {
        size = fstat.st_size;
      }

      if (taosWriteMsg(fd, &size, sizeof(size)) < 0) {
        dError("vid:%d, fileId:%d, failed to send last file:%s size:%ld to peer", vnode, fileId, lastName, size);
        return -1;
      }

      if (size > 0) {
        sfd = open(lastName, O_RDONLY);
        if (sfd < 0) {
          dError("vid:%d, fileId:%d, failed to send last file:%s size:%d to peer for sfd:% invalid", vnode, fileId,
                 lastName, size, sfd);
          return -1;
        }

        if (tsendfile(fd, sfd, NULL, size) < 0) {
          dError("vid:%d, fileId:%d, failed to send last file:%s size:%d to peer, reason:%s", vnode, fileId, lastName,
                 size, strerror(errno));
          close(sfd);
          return -1;
        }

        dPrint("vid:%d, fileId:%d, last file:%s is sent to peer, size:%ld", vnode, fileId, lastName, size);
        close(sfd);
      }
      else {
        dPrint("vid:%d, fileId:%d, last file:%s size:%ld is empty, not sent to peer", vnode, fileId, lastName, size);
      }
    }
  }

  fileId = 0;
  size = TSDB_SYNC_FILE_FINISHED;
  if (taosWriteMsg(fd, &(fileId), sizeof(fileId)) < 0) {
    dPrint("vid:%d, failed to send stop fileId:%d to peer", vnode, fileId);
    return -1;
  }

  if (taosWriteMsg(fd, &size, sizeof(size)) < 0) {
    dPrint("vid:%d, failed to send stop file size:%ld to peer", vnode, size);
    return -1;
  }

  fileId = pVnode->fileId;
  size = pVnode->numOfFiles;
  if (taosWriteMsg(fd, &(fileId), sizeof(fileId)) < 0) {
    dPrint("vid:%d, failed to send vnode fileId:%d to peer", vnode, fileId);
    return -1;
  }

  if (taosWriteMsg(fd, &size, sizeof(size)) < 0) {
    dPrint("vid:%d, failed to send vnode numOfFiles:%ld to peer", vnode, size);
    return -1;
  }

  dPrint("vid:%d, sync retrieve finished", vnode);
  return 0;
}

void vnodeAdjustFileTier(int vnode) {
  SVnodeObj *pVnode = vnodeList + vnode;
  int        fileId, minFileId;

  if (diskTier.numOfTiers >= 3 && pVnode->numOfFiles > pVnode->maxFile2) {
    minFileId = pVnode->fileId - pVnode->numOfFiles;
    for (fileId = pVnode->fileId - pVnode->maxFile2; fileId > minFileId; fileId--) {
      vnodeMoveFileBetweenTier(vnode, fileId, 2);
    }
  }

  if (diskTier.numOfTiers >= 2 && pVnode->numOfFiles > pVnode->maxFile1) {
    minFileId = pVnode->fileId - MIN(pVnode->numOfFiles, pVnode->maxFile2);
    for (fileId = pVnode->fileId - pVnode->maxFile1; fileId > minFileId; fileId--) {
      vnodeMoveFileBetweenTier(vnode, fileId, 1);
    }
  }
}

int vnodeSyncRestoreFile(int vnode, int sfd) {
  // TODO : make sure file states are correct when sync failed, when recover from peer, the numOfFiles are not correct.
  SVnodeObj *pVnode;
  char       headName[TSDB_FILENAME_LEN];
  char       dataName[TSDB_FILENAME_LEN];
  char       lastName[TSDB_FILENAME_LEN];
  int32_t    fileId;
  int64_t    size;
  int        dfd;

  dPrint("vid:%d, sfd:%d, start sync restore", vnode, sfd);

  pVnode = vnodeList + vnode;
  SVnodeCfg *pCfg = &pVnode->cfg;

  while (1) {
    if (taosReadMsg(sfd, &fileId, sizeof(fileId)) < 0) {
      dError("vid:%d, failed to receive fileId from peer, reason:%s", vnode, strerror(errno));
      return -1;
    }

    if (fileId == 0) {
      dPrint("vid:%d, stop fileId:%d received from peer", vnode, fileId);
    } else {
      dPrint("vid:%d, start to receive file from peer, fileId:%d", vnode, fileId);
    }

    if (taosReadMsg(sfd, &size, sizeof(size)) < 0) {
      dError("vid:%d, failed to receive head file size from peer, reason:%s", vnode, strerror(errno));
      return -1;
    }

    if (size == TSDB_SYNC_FILE_FINISHED) {
      dPrint("vid:%d, stop head file size:%d received from peer", vnode, size);
      break;
    }

    vnodeRemoveFile(vnode, fileId);
    if (vnodeCreateHeadDataFile(vnode, fileId, headName, dataName, lastName) < 0) {
      dError("vid:%d, fileId:%d, failed to create head file:%s", vnode, fileId, headName);
      return -1;
    }

    if (size > TSDB_SYNC_FILE_FINISHED) {
      // read head file

      dPrint("vid:%d, fileId:%d, start to receive head file:%s from peer, size:%ld", vnode, fileId, headName, size);

      if (size >= TSDB_SYNC_FILE_EMPTY) {
        dfd = open(headName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
        if (dfd < 0) {
          dError("vid:%d, fileId:%d, failed to create head file:%s, reason:%s", vnode, fileId, headName, strerror(errno));
          return -1;
        }

        if (size > 0) {
          if (taosCopyFds(sfd, dfd, size) < 0) {
            dError("vid:%d, fileId:%d, failed to receive head file:%s content from peer, size:%ld reason:%s",
                   vnode, fileId, headName, size, strerror(errno));
            close(dfd);
            vnodeRemoveFile(vnode, fileId);
            return -1;
          }
          dPrint("vid:%d, fileId:%d, head file:%s is received from peer, size:%ld", vnode, fileId, headName, size);
        } else {
          dPrint("vid:%d, fileId:%d, head file:%s is empty in peer", vnode, fileId, headName);
        }
        close(dfd);
      } else {
        dPrint("vid:%d, fileId:%d, head file:%s not exist in peer", vnode, fileId, headName);
      }

      // read data file
      if (taosReadMsg(sfd, &size, sizeof(size)) < 0) {
        dError("vid:%d, fileId:%d, failed to receive data file:%s size from peer, reason:%s",
                vnode, fileId, dataName, strerror(errno));
        return -1;
      }

      if (size <= TSDB_SYNC_FILE_FINISHED) {
        dError("vid:%d, fileId:%d, data file:%s size:%ld is not right", vnode, fileId, dataName, size);
        vnodeRemoveFile(vnode, fileId);
        return -1;
      }

      dPrint("vid:%d, fileId:%d, start to receive data file:%s from peer, size:%ld", vnode, fileId, dataName, size);
      if (size >= TSDB_SYNC_FILE_EMPTY) {
        dfd = open(dataName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
        if (dfd < 0) {
          dError("vid:%d, fileId:%d, failed to create data file:%s reason:%s", vnode, fileId, dataName, strerror(errno));
          vnodeRemoveFile(vnode, fileId);
          return -1;
        }

        if (size > 0) {
          if (taosCopyFds(sfd, dfd, size) < 0) {
            dError("vid:%d, fileId:%d, failed to receive data file:%s content from peer, size:%ld reason:%s",
                    vnode, fileId, dataName, size, strerror(errno));
            close(dfd);
            vnodeRemoveFile(vnode, fileId);
            return -1;
          }

          dPrint("vid:%d, fileId:%d, data file:%s is received from peer, size:%ld", vnode, fileId, dataName, size);
        } else {
          dPrint("vid:%d, fileId:%d, data file:%s is empty in peer", vnode, fileId, dataName);
        }
        close(dfd);
      } else {
        dPrint("vid:%d, fileId:%d, data file:%s not exist in peer", vnode, fileId, dataName);
      }

        // read last file
      if (taosReadMsg(sfd, &size, sizeof(size)) < 0) {
        dError("vid:%d, fileId:%d, failed to receive last file:%s size from peer, reason:%s",
                vnode, fileId, lastName, strerror(errno));
        return -1;
      }

      if (size <= TSDB_SYNC_FILE_FINISHED) {
        dError("vid:%d, fileId:%d, last file:%s size:%ld is not right", vnode, fileId, lastName, size);
        vnodeRemoveFile(vnode, fileId);
        return -1;
      }

      dPrint("vid:%d, fileId:%d, start to receieve last file:%s content from peer, size:%ld", vnode, fileId, lastName, size);

      if (size >= TSDB_SYNC_FILE_EMPTY) {
        dfd = open(lastName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
        if (dfd < 0) {
          dError("vid:%d, fileId:%d, failed to create last file:%s, reason:%s", vnode, fileId, lastName, strerror(errno));
          vnodeRemoveFile(vnode, fileId);
          return -1;
        }

        if (size > 0) {
          if (taosCopyFds(sfd, dfd, size) < 0) {
            dError("vid:%d, fileId:%d, failed to receive last file:%s from peer, size:%ld reason:%s",
                   vnode, fileId, lastName, size, strerror(errno));
            close(dfd);
            vnodeRemoveFile(vnode, fileId);
            return -1;
          }
          dPrint("vid:%d, fileId:%d, last file:%s is received from peer, size:%ld", vnode, fileId, lastName, size);
        } else {
          dPrint("vid:%d, fileId:%d, last file:%s is empty in peer", vnode, fileId, lastName);
        }
        close(dfd);
      } else {
        dPrint("vid:%d, fileId:%d, last file:%s not exist in peer", vnode, fileId, lastName);
      }

      vnodeUpdateFileMagic(vnode, fileId);
    } else if (size == TSDB_SYNC_FILE_NEED_REMOVE) {
      dPrint("vid:%d, fileId:%d, head file:%s is removed since peer does not have it", vnode, fileId, headName);
      vnodeRemoveFile(vnode, fileId);
      pVnode->fmagic[fileId % pVnode->maxFiles] = 0;
    } else {
      dError("vid:%d, fileId:%d, head file:%s invalid size:%d", vnode, fileId, headName, size);
    }
  }

  if (taosReadMsg(sfd, &fileId, sizeof(fileId)) < 0) {
    dError("vid:%d, failed to receive vnode fileId:%d from peer, reason:%s", vnode, fileId, strerror(errno));
    return -1;
  }

  if (taosReadMsg(sfd, &size, sizeof(size)) < 0) {
    dPrint("vid:%d, failed to receive vnode numOfFiles:%ld from peer, reason:%s", vnode, size, strerror(errno));
    return -1;
  }

  int oldFirstFileId = pVnode->fileId - pVnode->numOfFiles + 1;
  int newFirstFileId = fileId - size + 1;

  if (pVnode->numOfFiles > 0) {
    while (oldFirstFileId < newFirstFileId) {
      vnodeRemoveFile(vnode, oldFirstFileId);
      dPrint("vid:%d, fileId:%d is removed since they are too old", vnode, oldFirstFileId);
      oldFirstFileId++;
    }
  }

  vnodeAdjustFileTier(vnode);

  pVnode->badFileId = 0;  // no corrupted files
  pVnode->fileId = fileId;
  pVnode->numOfFiles = size;
  pVnode->lastKeyOnFile = pVnode->numOfFiles == 0
                              ? 0
                              : (int64_t)(pVnode->fileId + 1) * pCfg->daysPerFile * tsMsPerDay[(uint8_t)pVnode->cfg.precision] - 1;
  vnodeSaveVnodeInfo(vnode);

  dPrint("vid:%d, sync restore finished", vnode);

  return 0;
}

int vnodeCheckNewHeaderFile(int fd, SVnodeObj *pVnode) {
  return 0;
}