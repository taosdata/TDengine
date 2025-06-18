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

#ifndef _TD_VNODE_TSDB_INT_H_
#define _TD_VNODE_TSDB_INT_H_

typedef struct {
  STsdb  *tsdb;
  int32_t szPage;
  int32_t nodeId; // node id of leader vnode in s3 migration
  int64_t now;
  int64_t cid;

  STFileSet   *fset;
  TFileOpArray fopArr;
} SRTNer;


int32_t tsdbDoSsMigrate(SRTNer *rtner);
int32_t tsdbSsFidLevel(int32_t fid, STsdbKeepCfg *pKeepCfg, int32_t ssKeepLocal, int64_t nowSec);


#ifdef __cplusplus
}
#endif

#endif /*_TSDB_VNODE_TSDB_INT_H_*/
