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
#include "os.h"
#include "mndInt.h"

int32_t mndInitDb(SMnode *pMnode) { return 0; }
void    mndCleanupDb(SMnode *pMnode) {}


// static int32_t mnodeProcessUseMsg(SMnodeMsg *pMsg) {
//   SUseDbMsg *pUseDbMsg = pMsg->rpcMsg.pCont;

//   int32_t code = TSDB_CODE_SUCCESS;
//   if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDb(pUseDbMsg->db);
//   if (pMsg->pDb == NULL) {
//     return TSDB_CODE_MND_INVALID_DB;
//   }
  
//   if (pMsg->pDb->status != TSDB_DB_STATUS_READY) {
//     mError("db:%s, status:%d, in dropping", pMsg->pDb->name, pMsg->pDb->status);
//     return TSDB_CODE_MND_DB_IN_DROPPING;
//   }

//   return code;
// }
