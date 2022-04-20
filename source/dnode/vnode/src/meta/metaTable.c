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

#include "vnodeInt.h"

int metaCreateSTable(SMeta *pMeta, SVCreateStbReq *pReq, SVCreateStbRsp *pRsp) {
  // TODO
  return 0;
}

int metaCreateTable(SMeta *pMeta, STbCfg *pTbCfg) {
#ifdef META_REFACT
#else
  if (metaSaveTableToDB(pMeta, pTbCfg) < 0) {
    // TODO: handle error
    return -1;
  }
#endif

  if (metaSaveTableToIdx(pMeta, pTbCfg) < 0) {
    // TODO: handle error
    return -1;
  }

  return 0;
}

int metaDropTable(SMeta *pMeta, tb_uid_t uid) {
  if (metaRemoveTableFromIdx(pMeta, uid) < 0) {
    // TODO: handle error
    return -1;
  }

  if (metaRemoveTableFromIdx(pMeta, uid) < 0) {
    // TODO
    return -1;
  }

  return 0;
}
