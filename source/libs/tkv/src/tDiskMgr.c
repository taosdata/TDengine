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

#include "tkvDiskMgr.h"

struct STkvDiskMgr {
  char *   fname;
  uint16_t pgsize;
  FileFd   fd;
  pgid_t   npgid;
};

#define PAGE_OFFSET(PGID, PGSIZE) ((PGID) * (PGSIZE))

int tdmOpen(STkvDiskMgr **ppDiskMgr, const char *fname, uint16_t pgsize) {
  STkvDiskMgr *pDiskMgr;

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

int tdmClose(STkvDiskMgr *pDiskMgr) {
  close(pDiskMgr->fd);
  free(pDiskMgr->fname);
  free(pDiskMgr);
  return 0;
}

int tdmReadPage(STkvDiskMgr *pDiskMgr, pgid_t pgid, void *pData) {
  taosLSeekFile(pDiskMgr->fd, PAGE_OFFSET(pgid, pDiskMgr->pgsize), SEEK_SET);
  taosReadFile(pDiskMgr->fd, pData, pDiskMgr->pgsize);
  return 0;
}

int tdmWritePage(STkvDiskMgr *pDiskMgr, pgid_t pgid, const void *pData) {
  taosLSeekFile(pDiskMgr->fd, PAGE_OFFSET(pgid, pDiskMgr->pgsize), SEEK_SET);
  taosWriteFile(pDiskMgr->fd, pData, pDiskMgr->pgsize);
  return 0;
}

int tdmFlush(STkvDiskMgr *pDiskMgr) { return taosFsyncFile(pDiskMgr->fd); }

int32_t tdmAllocPage(STkvDiskMgr *pDiskMgr) { return pDiskMgr->npgid++; }