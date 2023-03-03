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
#include "syncTest.h"

cJSON *snapshotSender2Json(SSyncSnapshotSender *pSender) {
  char   u64buf[128];
  cJSON *pRoot = cJSON_CreateObject();

  if (pSender != NULL) {
    cJSON_AddNumberToObject(pRoot, "start", pSender->start);
    cJSON_AddNumberToObject(pRoot, "seq", pSender->seq);
    cJSON_AddNumberToObject(pRoot, "ack", pSender->ack);

    snprintf(u64buf, sizeof(u64buf), "%p", pSender->pReader);
    cJSON_AddStringToObject(pRoot, "pReader", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%p", pSender->pCurrentBlock);
    cJSON_AddStringToObject(pRoot, "pCurrentBlock", u64buf);
    cJSON_AddNumberToObject(pRoot, "blockLen", pSender->blockLen);

    if (pSender->pCurrentBlock != NULL) {
      char *s;
      s = syncUtilPrintBin((char *)(pSender->pCurrentBlock), pSender->blockLen);
      cJSON_AddStringToObject(pRoot, "pCurrentBlock", s);
      taosMemoryFree(s);
      s = syncUtilPrintBin2((char *)(pSender->pCurrentBlock), pSender->blockLen);
      cJSON_AddStringToObject(pRoot, "pCurrentBlock2", s);
      taosMemoryFree(s);
    }

    cJSON *pSnapshot = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSender->snapshot.lastApplyIndex);
    cJSON_AddStringToObject(pSnapshot, "lastApplyIndex", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSender->snapshot.lastApplyTerm);
    cJSON_AddStringToObject(pSnapshot, "lastApplyTerm", u64buf);
    cJSON_AddItemToObject(pRoot, "snapshot", pSnapshot);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSender->sendingMS);
    cJSON_AddStringToObject(pRoot, "sendingMS", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSender->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
    cJSON_AddNumberToObject(pRoot, "replicaIndex", pSender->replicaIndex);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSender->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    //  snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSender->privateTerm);
    //  cJSON_AddStringToObject(pRoot, "privateTerm", u64buf);

    cJSON_AddNumberToObject(pRoot, "finish", pSender->finish);
  }

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncSnapshotSender", pRoot);
  return pJson;
}

char *snapshotSender2Str(SSyncSnapshotSender *pSender) {
  cJSON *pJson = snapshotSender2Json(pSender);
  char  *serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

cJSON *snapshotReceiver2Json(SSyncSnapshotReceiver *pReceiver) {
  char   u64buf[128];
  cJSON *pRoot = cJSON_CreateObject();

  if (pReceiver != NULL) {
    cJSON_AddNumberToObject(pRoot, "start", pReceiver->start);
    cJSON_AddNumberToObject(pRoot, "ack", pReceiver->ack);

    snprintf(u64buf, sizeof(u64buf), "%p", pReceiver->pWriter);
    cJSON_AddStringToObject(pRoot, "pWriter", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%p", pReceiver->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);

    cJSON *pFromId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pReceiver->fromId.addr);
    cJSON_AddStringToObject(pFromId, "addr", u64buf);
    {
      uint64_t u64 = pReceiver->fromId.addr;
      cJSON   *pTmp = pFromId;
      char     host[128] = {0};
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pFromId, "vgId", pReceiver->fromId.vgId);
    cJSON_AddItemToObject(pRoot, "fromId", pFromId);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pReceiver->snapshot.lastApplyIndex);
    cJSON_AddStringToObject(pRoot, "snapshot.lastApplyIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pReceiver->snapshot.lastApplyTerm);
    cJSON_AddStringToObject(pRoot, "snapshot.lastApplyTerm", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pReceiver->snapshot.lastConfigIndex);
    cJSON_AddStringToObject(pRoot, "snapshot.lastConfigIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pReceiver->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pReceiver->startTime);
    cJSON_AddStringToObject(pRoot, "startTime", u64buf);
  }

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncSnapshotReceiver", pRoot);
  return pJson;
}

char *snapshotReceiver2Str(SSyncSnapshotReceiver *pReceiver) {
  cJSON *pJson = snapshotReceiver2Json(pReceiver);
  char  *serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

int32_t syncNodeOnPreSnapshot(SSyncNode *ths, SyncPreSnapshot *pMsg) {
  // syncLogRecvSyncPreSnapshot(ths, pMsg, "");

  SyncPreSnapshotReply *pMsgReply = syncPreSnapshotReplyBuild(ths->vgId);
  pMsgReply->srcId = ths->myRaftId;
  pMsgReply->destId = pMsg->srcId;
  pMsgReply->term = raftStoreGetTerm(ths);

  SSyncLogStoreData *pData = ths->pLogStore->data;
  SWal              *pWal = pData->pWal;

  if (syncNodeIsMnode(ths)) {
    pMsgReply->snapStart = SYNC_INDEX_BEGIN;

  } else {
    bool    isEmpty = ths->pLogStore->syncLogIsEmpty(ths->pLogStore);
    int64_t walCommitVer = walGetCommittedVer(pWal);

    if (!isEmpty && ths->commitIndex != walCommitVer) {
      sNError(ths, "commit not same, wal-commit:%" PRId64 ", commit:%" PRId64 ", ignore", walCommitVer,
              ths->commitIndex);
      goto _IGNORE;
    }

    pMsgReply->snapStart = ths->commitIndex + 1;

    // make local log clean
    int32_t code = ths->pLogStore->syncLogTruncate(ths->pLogStore, pMsgReply->snapStart);
    if (code != 0) {
      sNError(ths, "truncate wal error");
      goto _IGNORE;
    }
  }

  // can not write behind _RESPONSE
  SRpcMsg rpcMsg;

_RESPONSE:
  syncPreSnapshotReply2RpcMsg(pMsgReply, &rpcMsg);
  syncNodeSendMsgById(&pMsgReply->destId, ths, &rpcMsg);

  syncPreSnapshotReplyDestroy(pMsgReply);
  return 0;

_IGNORE:
  syncPreSnapshotReplyDestroy(pMsgReply);
  return 0;
}

int32_t syncNodeOnPreSnapshotReply(SSyncNode *ths, SyncPreSnapshotReply *pMsg) {
  // syncLogRecvSyncPreSnapshotReply(ths, pMsg, "");

  // start snapshot

  return 0;
}