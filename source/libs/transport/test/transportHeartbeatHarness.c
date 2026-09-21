/** Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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
