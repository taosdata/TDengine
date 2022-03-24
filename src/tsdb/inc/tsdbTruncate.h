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
#ifndef _TD_TSDB_TRUNCATE_H_
#define _TD_TSDB_TRUNCATE_H_

#ifdef __cplusplus
extern "C" {
#endif

// SControlData addition information
typedef struct {
  SControlData ctlData;
  // addition info
  uint64_t     uid;        // table unique id
  int32_t      tid;        // table id
  tsem_t*      pSem;
  bool         memNull; // pRepo->mem is NULL, this is true
  uint64_t*    uids;  // delete table
  int32_t      uidCount;
  SShellSubmitRspMsg *pRsp;
} SControlDataInfo;

// -------- interface ---------

// delete
int tsdbControlDelete(STsdbRepo* pRepo, SControlDataInfo* pCtlDataInfo);

#ifdef __cplusplus
}
#endif

#endif /* _TD_TSDB_TRUNCATE_H_ */