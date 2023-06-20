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

#include "vnd.h"

extern int32_t tsdbSyncRetention(STsdb *tsdb, int64_t now);

int32_t vnodeSyncRetention(SVnode *pVnode, int64_t now) {
  int32_t code;
  if (pVnode->config.sttTrigger == 1) {
    tsem_wait(&pVnode->canCommit);
    code = tsdbSyncRetention(pVnode->pTsdb, now);
    tsem_post(&pVnode->canCommit);
  } else {
    code = tsdbSyncRetention(pVnode->pTsdb, now);
  }
  return code;
}
