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

int32_t mndInitCrypt(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CRYPT, mndRetrieveCompact);

  SSdbTable table = {
      .sdbType = SDB_COMPACT,
      .keyType = SDB_KEY_INT32,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupCrypt(SMnode *pMnode) {
  mDebug("mnd crypt cleanup");
}

int32_t mndRetrieveCompact(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows){
    mDebug("mnd retrieve crypt");
    return 0;
}