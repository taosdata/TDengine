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

#include "mndCrypt.h"
#include "mndShow.h"
#include "tmisce.h"
#include "mndDnode.h"

int32_t mndInitCrypt(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CRYPT, mndRetrieveCrypt);

  SSdbTable table = {
      .sdbType = SDB_CRYPT,
      .keyType = SDB_KEY_INT32,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupCrypt(SMnode *pMnode) {
  mDebug("mnd crypt cleanup");
}

int32_t mndRetrieveCrypt(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows){
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SDnodeObj  *pDnode = NULL;
  int64_t     curMs = taosGetTimestampMs();
  ESdbStatus  objStatus = 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetchAll(pSdb, SDB_DNODE, pShow->pIter, (void **)&pDnode, &objStatus, true);

    if (pShow->pIter == NULL) break;

    bool online = mndIsDnodeOnline(pDnode, curMs);
    if(!online) continue;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;
    
    SEpSet epSet = {0};
    addEpIntoEpSet(&epSet, pDnode->fqdn, pDnode->port);

    SCryptReq req = {0};
    
    int32_t contLen = tSerializeSCryptReq(NULL, 0, &req);
    void *  pHead = rpcMallocCont(contLen);
    tSerializeSCryptReq(pHead, contLen, &req);

    SRpcMsg rpcMsg = {.pCont = pHead,
                    .contLen = contLen,
                    .msgType = TDMT_DND_QUERY_CRYPT,
                    .info.ahandle = (void *)0x9527,
                    .info.refId = 0,
                    .info.noResp = 0};
    SRpcMsg rpcRsp = {0};

    int8_t epUpdated = 0;

    rpcSendRecvWithTimeout(pMnode->msgCb.clientRpc, &epSet, &rpcMsg, &rpcRsp, &epUpdated, 5 * 1000);
    if (rpcRsp.code != 0) {
      mError("failed to get crypt info from dnode:%d, %s, code:%d", pDnode->id, pDnode->fqdn, rpcRsp.code);
      continue;
    }

    SCryptRsp cryptRsp = {0};

    if(tDeserializeSCryptRsp(rpcRsp.pCont, rpcRsp.contLen, &cryptRsp) != 0){
      mError("failed to deserialize crypt rsp from dnode:%d, %s", pDnode->id, pDnode->fqdn);
      continue;
    }

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)&cryptRsp.dnodeid, false);

/*
    char algorStr[10] = {0};    
    if(cryptRsp.cryptAlgorithm == DND_CA_SM4){
      sprintf(algorStr, "%s", "sm4");
    }
    if(cryptRsp.cryptAlgorithm == 0){
      sprintf(algorStr, "%s", "na");
    }
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    strncpy(varDataVal(tmpBuf), algorStr, TSDB_SHOW_SQL_LEN);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);

    char scopeStr[95] = {0};
    if((cryptRsp.cryptScope & DND_CS_TSDB) == DND_CS_TSDB){
      if(strlen(scopeStr) > 0){
        strcat(scopeStr, ",tsdb");
      }
      else{
        strcpy(scopeStr, "tsdb");
      }
    }
    if((cryptRsp.cryptScope & DND_CS_VNODE_WAL) == DND_CS_VNODE_WAL){
      if(strlen(scopeStr) > 0){
        strcat(scopeStr, ",vnode_wal");
      }
      else{
        strcpy(scopeStr, "vnode_wal");
      }
    }
    if((cryptRsp.cryptScope & DND_CS_SDB) == DND_CS_SDB){
      if(strlen(scopeStr) > 0){
        strcat(scopeStr, ",sdb");
      }
      else{
        strcpy(scopeStr, "sdb");
      }
    }
    if((cryptRsp.cryptScope & 4) == DND_CS_MNODE_WAL){
      if(strlen(scopeStr) > 0){
        strcat(scopeStr, ",mnode_wal");
      }
      else{
        strcpy(scopeStr, "mnode_wal");
      }
    }
    char scopeFinialStr[100] = {0};
    if((cryptRsp.cryptScope & DND_CS_TSDB) == DND_CS_TSDB && 
       (cryptRsp.cryptScope & DND_CS_VNODE_WAL) == DND_CS_VNODE_WAL &&
       (cryptRsp.cryptScope & DND_CS_SDB) == DND_CS_SDB &&
       (cryptRsp.cryptScope & DND_CS_VNODE_WAL) == DND_CS_VNODE_WAL){
      sprintf(scopeFinialStr, "all(%s)", scopeStr);
    }
    else{
      strcpy(scopeFinialStr, scopeStr);
    }
    if(cryptRsp.cryptScope == 0){
      strcpy(scopeFinialStr, "na");
    }
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    strncpy(varDataVal(tmpBuf), scopeFinialStr, TSDB_SHOW_SQL_LEN);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);
*/
    char *status = "key not setted";
    if(cryptRsp.keyFileExisted){
      status = "key setted";
    }
    if(cryptRsp.keyLoaded){
      status = "key loaded";
    }
    char statusVstr[24] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(statusVstr, status, 24);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)statusVstr, false);

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;    
  return numOfRows;
}