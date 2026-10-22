/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

// Compile the implementation in this test-only translation unit to use its private state without duplicating layouts.
#ifndef TD_ASTRA_RPC
#include "../src/transCli.c"
typedef struct {
  STrans   instance;
  SCliThrd thread;
  SCliConn conn;
  int32_t  callbacks;
  int32_t  lastCode;
} SHeartbeatTestPeer;

static void heartbeatTestCallback(void* arg, SRpcMsg* msg, SEpSet* eps) {
  SHeartbeatTestPeer* peer = arg;
  ++peer->callbacks;
  peer->lastCode = msg->code;
  rpcFreeCont(msg->pCont);
}

static int32_t heartbeatTestPacket(SHeartbeatTestPeer* peer, int64_t seq, int64_t qid, int32_t msgType, int32_t code) {
  STransMsgHead head = {0};
  head.version = TRANS_VER;
  head.msgLen = htonl(sizeof(head));
  head.seqNum = taosHton64(seq);
  head.qid = taosHton64(qid);
  head.msgType = msgType;
  head.code = htonl(code);
  int32_t result = transDoCrc((char*)&head, sizeof(head));
  if (result != 0) return result;
  result = transConnBufferAppend(&peer->conn.readBuf, (char*)&head, sizeof(head));
  if (result != 0) return result;
  if (!transReadComplete(&peer->conn.readBuf)) return TSDB_CODE_INVALID_MSG;
  cliHandleResp(&peer->conn);
  return 0;
}

int32_t runNativeHeartbeatScenario(int32_t scenario, int32_t* callbacks, int32_t* pending, int32_t* lastCode,
                                   int64_t* contextTime) {
  SHeartbeatTestPeer peer = {0};
  peer.instance.cfp = heartbeatTestCallback;
  peer.instance.parent = &peer;
  peer.thread.pInst = &peer.instance;
  peer.conn.hostThrd = &peer.thread;
  transQueueInit(&peer.conn.reqsToSend, NULL);
  transQueueInit(&peer.conn.reqsSentOut, NULL);
  peer.conn.pQTable = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (peer.conn.pQTable == NULL) return terrno;
  int32_t code = transInitBuffer(&peer.conn.readBuf);
  if (code != 0) goto _exit;
  int64_t   qid = 7;
  STransCtx context = {.st = 12345};
  code = taosHashPut(peer.conn.pQTable, &qid, sizeof(qid), &context, sizeof(context));
  if (code != 0) goto _exit;
  if (scenario >= 2 && scenario <= 6) {
    SCliReq* req = taosMemoryCalloc(1, sizeof(*req));
    if (req == NULL) {
      code = terrno;
      goto _exit;
    }
    req->ctx = taosMemoryCalloc(1, sizeof(*req->ctx));
    if (req->ctx == NULL) {
      taosMemoryFree(req);
      code = terrno;
      goto _exit;
    }
    req->seq = 100;
    req->msg.msgType = scenario == 3 ? TDMT_MND_STATUS : TDMT_MND_STREAM_HEARTBEAT;
    req->msg.info.qId = qid;
    req->ctx->msgType = req->msg.msgType;
    transQueuePush(&peer.conn.reqsSentOut, &req->q);
  }
  int32_t responseType = scenario == 7 ? TDMT_MND_STATUS_RSP : TDMT_MND_STREAM_HEARTBEAT_RSP;
  code = heartbeatTestPacket(&peer, scenario == 4 ? 101 : 100, scenario == 0 ? 0 : qid, responseType,
                             scenario == 6 ? TSDB_CODE_RPC_BROKEN_LINK : 0);
  if (code == 0 && scenario == 2) code = heartbeatTestPacket(&peer, 100, qid, responseType, 0);
  *callbacks = peer.callbacks;
  *pending = transQueueSize(&peer.conn.reqsSentOut);
  *lastCode = peer.lastCode;
  STransCtx* saved = taosHashGet(peer.conn.pQTable, &qid, sizeof(qid));
  *contextTime = saved == NULL ? 0 : saved->st;
_exit:
  while (transQueueSize(&peer.conn.reqsSentOut) > 0) {
    queue* item = transQueuePop(&peer.conn.reqsSentOut);
    destroyReq(QUEUE_DATA(item, SCliReq, q));
  }
  transDestroyBuffer(&peer.conn.readBuf);
  taosHashCleanup(peer.conn.pQTable);
  return code;
}

// Devon scan item 195: a hasEpSet response whose body is shorter than the canonical serialized
// SEpSet must be rejected instead of driving a negative-length rpcMallocCont/memcpy.
// bodyLen < canonical size  -> return 1 when the response is rejected with INVALID_MSG
// bodyLen == canonical size -> return 2 when the response is accepted (legit traffic unaffected)
// negative                  -> test setup failure
int32_t testCliTryUpdateEpsetShort(int32_t bodyLen, int canonicalLen) {
  SEpSet epset = {0};
  epset.inUse = 0;
  epset.numOfEps = 1;
  epset.eps[0].port = htons(6030);
  (void)snprintf(epset.eps[0].fqdn, sizeof(epset.eps[0].fqdn), "h1");

  int32_t selfLen = tSerializeSEpSet(NULL, 0, &epset);
  if (selfLen <= 0 || selfLen != canonicalLen) return -1;
  if (bodyLen > 4 * 1024) return -1;

  char* body = rpcMallocCont(bodyLen);
  if (body == NULL) return -2;

  int32_t code = 0;
  if (bodyLen < canonicalLen) {
    // attacker shape: a minimal wire form the decoder accepts (variable-length fqdn entries
    // instead of the canonical fixed-width encoding), followed by zero padding
    SEncoder enc = {0};
    tEncoderInit(&enc, body, bodyLen);
    code = tStartEncode(&enc);
    if (code == 0) code = tEncodeI8(&enc, epset.inUse);
    if (code == 0) code = tEncodeI8(&enc, epset.numOfEps);
    for (int32_t i = 0; code == 0 && i < TSDB_MAX_REPLICA; i++) {
      code = tEncodeU16(&enc, epset.eps[i].port);
      if (code == 0) code = tEncodeCStr(&enc, epset.eps[i].fqdn);
    }
    if (code == 0) tEndEncode(&enc);
    tEncoderClear(&enc);
  } else {
    code = tSerializeSEpSet(body, bodyLen, &epset);
  }
  if (code < 0) {
    rpcFreeCont(body);
    return -3;
  }

  SReqCtx   ctx = {0};
  SCliReq   req = {0};
  STransMsg resp = {0};
  req.ctx = &ctx;
  resp.info.hasEpSet = 1;
  resp.pCont = body;
  resp.contLen = bodyLen;

  bool accepted = cliTryUpdateEpset(&req, &resp);
  rpcFreeCont(resp.pCont);
  taosMemoryFree(ctx.epSet);

  if (bodyLen < canonicalLen) {
    return (!accepted && resp.code == TSDB_CODE_INVALID_MSG) ? 1 : 0;
  }
  return accepted ? 2 : 0;
}

// Devon scan item 351: a TDMT_SCH_TASK_RELEASE frame carrying a qId that is not registered on
// the receiving connection must be ignored. Registers registeredQid, releases releaseQid and
// returns 1 while registeredQid is still present; 0 otherwise; negative on setup failure.
int32_t testCliReleaseRespGate(int64_t releaseQid, int64_t registeredQid) {
  STrans   instance = {0};
  SCliThrd thread = {0};
  SCliConn conn = {0};

  thread.pInst = &instance;
  conn.hostThrd = &thread;
  transQueueInit(&conn.reqsToSend, NULL);
  transQueueInit(&conn.reqsSentOut, NULL);
  conn.pQTable = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  thread.pIdConnTable = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (conn.pQTable == NULL || thread.pIdConnTable == NULL) {
    return -1;
  }

  STransCtx ctx = {.st = 4242};
  if (taosHashPut(conn.pQTable, &registeredQid, sizeof(registeredQid), &ctx, sizeof(ctx)) != 0) {
    return -2;
  }

  STransMsgHead head = {0};
  head.version = TRANS_VER;
  head.msgLen = htonl(sizeof(head));
  head.msgType = TDMT_SCH_TASK_RELEASE;
  head.qid = taosHton64(releaseQid);

  (void)cliHandleState_mayHandleReleaseResp(&conn, &head);

  int32_t present = taosHashGet(conn.pQTable, &registeredQid, sizeof(registeredQid)) != NULL;

  while (transQueueSize(&conn.reqsSentOut) > 0) {
    queue* item = transQueuePop(&conn.reqsSentOut);
    destroyReq(QUEUE_DATA(item, SCliReq, q));
  }
  while (transQueueSize(&conn.reqsToSend) > 0) {
    queue* item = transQueuePop(&conn.reqsToSend);
    destroyReq(QUEUE_DATA(item, SCliReq, q));
  }
  taosHashCleanup(conn.pQTable);
  taosHashCleanup(thread.pIdConnTable);
  return present;
}
#else
#include "../src/transComm.c"

static void*   heartbeatBodies[2];
static int32_t heartbeatBodyFrees;
static int32_t heartbeatCallbacks;
static int32_t heartbeatLastCode;

void __real_rpcFreeCont(void* body);
void __wrap_rpcFreeCont(void* body) {
  for (int32_t i = 0; i < 2; ++i) {
    if (body != NULL && body == heartbeatBodies[i]) {
      ++heartbeatBodyFrees;
      heartbeatBodies[i] = NULL;
    }
  }
  __real_rpcFreeCont(body);
}

static void heartbeatTestCallback(void* arg, SRpcMsg* msg, SEpSet* eps) {
  ++heartbeatCallbacks;
  heartbeatLastCode = msg->code;
  rpcFreeCont(msg->pCont);
}

int32_t runAstraHeartbeatScenario(int32_t scenario, int32_t* callbacks, int32_t* pending, int32_t* lastCode,
                                  int32_t* bodyFrees) {
  STrans             peer = {0};
  STaosQueue*        queue = NULL;
  STaosQset*         qset = NULL;
  MultiThreadQhandle queues = {.numOfThread = 1, .qhandle = &queue, .qset = &qset};
  TThread            thread = {.idx = 0};
  int32_t            code = 0;
  int64_t            seq = 100;
  int64_t            qid = 7;
  int32_t            requestType = scenario == 3 ? TDMT_MND_STATUS : TDMT_MND_STREAM_HEARTBEAT;
  STransCtx          context = {.st = 12345};
  heartbeatCallbacks = heartbeatBodyFrees = heartbeatLastCode = 0;
  peer.cfp = heartbeatTestCallback;
  taosThreadMutexInit(&peer.seqMutex, NULL);
  taosThreadMutexInit(&tableMutex, NULL);
  taosThreadMutexInit(&mutex[1], NULL);
  peer.seqTable = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  peer.sidTable = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  hashTable = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UINT), false, HASH_NO_LOCK);
  if (peer.seqTable == NULL || peer.sidTable == NULL || hashTable == NULL) {
    code = terrno;
    goto _exit;
  }
  code = taosHashPut(peer.sidTable, &qid, sizeof(qid), &context, sizeof(context));
  if (code != 0) goto _exit;
  if (scenario >= 2 && scenario <= 6) {
    code = taosHashPut(peer.seqTable, &seq, sizeof(seq), &requestType, sizeof(requestType));
    if (code != 0) goto _exit;
  }
  code = transUpdateCb(TD_ASTRA_DSVR_CLIENT, &peer);
  if (code != 0) goto _exit;
  code = taosOpenQueue(&queue);
  if (code != 0) goto _exit;
  code = taosOpenQset(&qset);
  if (code != 0) goto _exit;
  code = taosAddIntoQset(qset, queue, NULL);
  if (code != 0) goto _exit;
  multiQ[1] = &queues;
  for (int32_t i = 0; i < (scenario == 2 ? 2 : 1); ++i) {
    SRpcMsg msg = {0};
    msg.type = TD_ASTRA_DSVR_CLIENT;
    msg.msgType = scenario == 7 ? TDMT_MND_STATUS_RSP : TDMT_MND_STREAM_HEARTBEAT_RSP;
    msg.info.seq = scenario == 4 ? 101 : seq;
    msg.info.qId = scenario == 0 ? 0 : qid;
    if (scenario == 6) {
      msg.code = TSDB_CODE_RPC_BROKEN_LINK;
    } else {
      msg.contLen = 1;
      msg.pCont = rpcMallocCont(msg.contLen);
      if (msg.pCont == NULL) {
        code = terrno;
        goto _exit;
      }
      heartbeatBodies[i] = msg.pCont;
    }
    code = transSendResp(&msg);
    if (code != 0) goto _exit;
  }
  // The extra wakeup lets the real queue loop exit after consuming its local messages.
  taosQsetThreadResume(qset);
  processSvrMsg(&thread);
  *callbacks = heartbeatCallbacks;
  *bodyFrees = heartbeatBodyFrees;
  *pending = taosHashGetSize(peer.seqTable);
  *lastCode = heartbeatLastCode;
_exit:
  taosCloseQueue(queue);
  if (qset != NULL) taosCloseQset(qset);
  multiQ[1] = NULL;
  for (int32_t i = 0; i < 2; ++i) {
    rpcFreeCont(heartbeatBodies[i]);
    heartbeatBodies[i] = NULL;
  }
  taosHashCleanup(peer.seqTable);
  taosHashCleanup(peer.sidTable);
  taosHashCleanup(hashTable);
  hashTable = NULL;
  taosThreadMutexDestroy(&peer.seqMutex);
  taosThreadMutexDestroy(&tableMutex);
  taosThreadMutexDestroy(&mutex[1]);
  return code;
}
#endif
