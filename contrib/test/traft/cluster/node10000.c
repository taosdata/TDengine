#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <uv.h>

#include "raft.h"

SRaftEnv raftEnv;

typedef struct Tsdb {
  uint64_t lastApplyIndex;
  void    *mem;
  void    *imm;
  void    *store;
} Tsdb;

void tsdbWrite(Tsdb *t, char *msg) {}

void *startFunc(void *param) {
  SRaftEnv *pSRaftEnv = (SRaftEnv *)param;
  int32_t   r = raftEnvStart(pSRaftEnv);
  assert(r == 0);
  return NULL;
}

int fsmApplyCb(struct raft_fsm *pFsm, const struct raft_buffer *buf, void **result, raft_index index) {
  // get commit value
  char *msg = (char *)buf->base;
  printf("fsm apply: index:%llu value:%s \n", index, msg);

  Tsdb *t = pFsm->data;
  if (index > t->lastApplyIndex) {
    // apply value into tsdb
    tsdbWrite(t, msg);

    // update lastApplyIndex
    t->lastApplyIndex = index;
  }

  return 0;
}

void putValueCb(struct raft_apply *req, int status, void *result) {
  void *ptr = req->data;
  if (status != 0) {
    printf("putValueCb error \n");
  } else {
    printf("putValueCb ok \n");
  }
  free(ptr);
  free(req);
}

void submitValue() {
  // prepare value
  struct raft_buffer buf;
  buf.len = 32;
  void *ptr = malloc(buf.len);
  buf.base = ptr;
  snprintf(buf.base, buf.len, "%ld", time(NULL));

  // get raft
  struct raft *r = getRaft(&raftEnv, 100);
  assert(r != NULL);
  // printRaftState(r);

  // submit value
  struct raft_apply *req = malloc(sizeof(*req));
  req->data = ptr;
  int ret = raft_apply(r, req, &buf, 1, putValueCb);
  if (ret == 0) {
    printf("put %s \n", (char *)buf.base);
  } else {
    printf("put error: %s \n", raft_errmsg(r));
  }
}

int main(int argc, char **argv) {
  // init raft env
  int r = raftEnvInit(&raftEnv, "127.0.0.1", 10000, "./data");
  assert(r == 0);

  // start raft env
  pthread_t tid;
  pthread_create(&tid, NULL, startFunc, &raftEnv);

  // wait for start, just for simple
  while (raftEnv.isStart != 1) {
    sleep(1);
  }

  // init fsm
  struct raft_fsm *pFsm = malloc(sizeof(*pFsm));
  pFsm->apply = fsmApplyCb;
  Tsdb *tsdb = malloc(sizeof(*tsdb));
  pFsm->data = tsdb;

  // add vgroup, id = 100, only has 3 replica.
  // array <peers, peersCount> gives the peer replica infomation.
  char peers[MAX_PEERS_COUNT][ADDRESS_LEN];
  memset(peers, 0, sizeof(peers));
  snprintf(peers[0], ADDRESS_LEN, "%s", "127.0.0.1:10001");
  snprintf(peers[1], ADDRESS_LEN, "%s", "127.0.0.1:10002");
  uint32_t peersCount = 2;
  r = addRaftVoter(&raftEnv, peers, peersCount, 100, pFsm);
  assert(r == 0);

  // for test: submit a value every second
  while (1) {
    sleep(1);
    submitValue();
  }

  return 0;
}
