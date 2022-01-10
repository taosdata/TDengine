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

#include "tDiskMgr.h"

struct SDiskMgr {
  const char *fname;
  uint16_t    pgsize;
  FileFd      fd;
  int32_t     npgid;
};

#define PAGE_OFFSET(PGID, PGSIZE) ((PGID) * (PGSIZE))

int tdmOpen(SDiskMgr **ppDiskMgr, const char *fname, uint16_t pgsize) {
  SDiskMgr *pDiskMgr;

  pDiskMgr = malloc(sizeof(*pDiskMgr));
  if (pDiskMgr == NULL) {
    return -1;
  }

  pDiskMgr->fname = strdup(fname);
  if (pDiskMgr->fname == NULL) {
    free(pDiskMgr);
    return -1;
  }
  pDiskMgr->pgsize = pgsize;
  pDiskMgr->fd = open(fname, O_CREAT | O_RDWR, 0755);
  if (pDiskMgr->fd < 0) {
    free(pDiskMgr->fname);
    free(pDiskMgr);
    return -1;
  }

  *ppDiskMgr = pDiskMgr;

  return 0;
}

int tdmClose(SDiskMgr *pDiskMgr) {
  close(pDiskMgr->fd);
  free(pDiskMgr->fname);
  free(pDiskMgr);
  return 0;
}

int tdmReadPage(SDiskMgr *pDiskMgr, pgid_t pgid, void *pData) {
  taosLSeekFile(pDiskMgr->fd, PAGE_OFFSET(pgid, pDiskMgr->pgsize), SEEK_SET);
  taosReadFile(pDiskMgr->fd, pData, pDiskMgr->pgsize);
  return 0;
}

int tdmWritePage(SDiskMgr *pDiskMgr, pgid_t pgid, const void *pData) {
  taosLSeekFile(pDiskMgr->fd, PAGE_OFFSET(pgid, pDiskMgr->pgsize), SEEK_SET);
  taosWriteFile(pDiskMgr->fd, pData, pDiskMgr->pgsize);
  return 0;
}

int32_t tdmAllocPage(SDiskMgr *pDiskMgr) { return pDiskMgr->npgid++; }