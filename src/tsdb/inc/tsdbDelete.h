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
#ifndef _TD_TSDB_DELETE_H_
#define _TD_TSDB_DELETE_H_

#ifdef __cplusplus
extern "C" {
#endif

// SControlData addition information
#define GET_CTLINFO_SIZE(p) (sizeof(SControlDataInfo) + p->tnum * sizeof(int32_t))
typedef struct {
  // addition info
  tsem_t*      pSem;
  bool         memNull; // pRepo->mem is NULL, this is true
  int32_t      affectedRows;
  SShellSubmitRspMsg *pRsp;
  
  // base ControlData
  STimeWindow win;      // come from SControlData.win
  uint32_t    command;  // come from SControlData.command
  int32_t     tnum;     // num of tids array
  int32_t     tids[];
} SControlDataInfo;

// -------- interface ---------

// delete
int tsdbControlDelete(STsdbRepo* pRepo, SControlDataInfo* pCtlDataInfo);

#ifdef __cplusplus
}
#endif

#endif /* _TD_TSDB_DELETE_H_ */