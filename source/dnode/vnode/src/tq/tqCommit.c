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

#include "tq.h"

int tqCommit(STQ* pTq) {
#if 0
  // stream meta commit does not be aligned to the vnode commit
  if (streamMetaCommit(pTq->pStreamMeta) < 0) {
    tqError("vgId:%d, failed to commit stream meta since %s", TD_VID(pTq->pVnode), terrstr());
    return -1;
  }
#endif

  return tqOffsetCommitFile(pTq->pOffsetStore);
}
