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
#include "vnode.h"
#include "vnodeFile.h"

char* vnodeGetDiskFromHeadFile(char *headName) { return tsDirectory; }

char* vnodeGetDataDir(int vnode, int fileId) { return dataDir; }

void vnodeAdustVnodeFile(SVnodeObj *pVnode) {
  // Retention policy here
  int fileId = pVnode->fileId - pVnode->numOfFiles + 1;
  int cfile = taosGetTimestamp(pVnode->cfg.precision)/pVnode->cfg.daysPerFile/tsMsPerDay[(uint8_t)pVnode->cfg.precision];
  while (fileId <= cfile - pVnode->maxFiles) {
    vnodeRemoveFile(pVnode->vnode, fileId);
    pVnode->numOfFiles--;
    fileId++;
  }
}

int vnodeCheckNewHeaderFile(int fd, SVnodeObj *pVnode) {
  SCompHeader *pHeader = NULL;
  SCompBlock  *pBlocks = NULL;
  int          blockSize = 0;
  SCompInfo    compInfo;
  int          tmsize = 0;

  tmsize = sizeof(SCompHeader) * pVnode->cfg.maxSessions + sizeof(TSCKSUM);

  pHeader = (SCompHeader *)malloc(tmsize);
  if (pHeader == NULL) return 0;

  lseek(fd, TSDB_FILE_HEADER_LEN, SEEK_SET);
  if (read(fd, (void *)pHeader, tmsize) != tmsize) {
    goto _broken_exit;
  }

  if (!taosCheckChecksumWhole((uint8_t *)pHeader, tmsize)) {
    goto _broken_exit;
  }

  for (int sid = 0; sid < pVnode->cfg.maxSessions; sid++) {
    if (pVnode->meterList == NULL) goto _correct_exit;
    if (pVnode->meterList[sid] == NULL || pHeader[sid].compInfoOffset == 0) continue;
    lseek(fd, pHeader[sid].compInfoOffset, SEEK_SET);

    if (read(fd, (void *)(&compInfo), sizeof(SCompInfo)) != sizeof(SCompInfo)) {
      goto _broken_exit;
    }

    if (!taosCheckChecksumWhole((uint8_t *)(&compInfo), sizeof(SCompInfo))) {
      goto _broken_exit;
    }

    if (compInfo.uid != ((SMeterObj *)pVnode->meterList[sid])->uid) continue;

    int expectedSize = sizeof(SCompBlock) * compInfo.numOfBlocks + sizeof(TSCKSUM);
    if (blockSize < expectedSize) {
      pBlocks = (SCompBlock *)realloc(pBlocks, expectedSize);
      if (pBlocks == NULL) {
        tfree(pHeader);
        return 0;
      }

      blockSize = expectedSize;
    }

    if (read(fd, (void *)pBlocks, expectedSize) != expectedSize) {
      dError("failed to read block part");
      goto _broken_exit;
    }
    if (!taosCheckChecksumWhole((uint8_t *)pBlocks, expectedSize)) {
      dError("block part is broken");
      goto _broken_exit;
    }

    for (int i = 0; i < compInfo.numOfBlocks; i++) {
      if (pBlocks[i].last && i != compInfo.numOfBlocks-1) {
        dError("last block in middle, block:%d", i);
        goto  _broken_exit;
      }
    }
  }

    _correct_exit:
  dPrint("vid: %d new header file %s is correct", pVnode->vnode, pVnode->nfn);
  tfree(pBlocks);
  tfree(pHeader);
  return 0;

    _broken_exit:
  dError("vid: %d new header file %s is broken", pVnode->vnode, pVnode->nfn);
  tfree(pBlocks);
  tfree(pHeader);
  return -1;
}