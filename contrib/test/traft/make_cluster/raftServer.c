#include "raftServer.h"
#include <stdlib.h>
#include <unistd.h>
#include "common.h"
#include "simpleHash.h"
#include "util.h"

void *startServerFunc(void *param) {
  RaftServer *pRaftServer = (RaftServer *)param;
  int32_t     r = raftServerStart(pRaftServer);
  assert(r == 0);
  return NULL;
}

void raftChangeAssignCb(struct raft_change *req, int status) {
  struct raft *r = req->data;
  if (status != 0) {
    printf("raftChangeAssignCb error: %s \n", raft_errmsg(r));
  } else {
    printf("raftChangeAssignCb ok \n");
  }
  raft_free(req);
}

void raftChangeAddCb(struct raft_change *req, int status) {
  RaftJoin *pRaftJoin = req->data;
  if (status != 0) {
    printf("raftChangeAddCb error: %s \n", raft_errmsg(pRaftJoin->r));
  } else {
    struct raft_change *req2 = raft_malloc(sizeof(*req2));
    req2->data = pRaftJoin->r;
    int ret = raft_assign(pRaftJoin->r, req2, pRaftJoin->joinId, RAFT_VOTER, raftChangeAssignCb);
    if (ret != 0) {
      printf("raftChangeAddCb error: %s \n", raft_errmsg(pRaftJoin->r));
    }
  }
  raft_free(req->data);
  raft_free(req);
}

int fsmApplyCb(struct raft_fsm *pFsm, const struct raft_buffer *buf, void **result) {
  // get fsm data
  SimpleHash *sh = pFsm->data;

  // get commit value
  char *msg = (char *)buf->base;
  printf("fsm apply: [%s] \n", msg);
  char arr[2][TOKEN_LEN];
  int  r = splitString(msg, "--", arr, 2);
  assert(r == 2);

  // do the value on fsm
  sh->insert_cstr(sh, arr[0], arr[1]);

  raft_free(buf->base);
  return 0;
}

void putValueCb(struct raft_apply *req, int status, void *result) {
  struct raft *r = req->data;
  if (status != 0) {
    printf("putValueCb error: %s \n", raft_errmsg(r));
  } else {
    printf("putValueCb: %s \n", "ok");
  }
  raft_free(req);
}

void putValue(struct raft *r, const char *value) {
  struct raft_buffer buf;

  buf.len = strlen(value) + 1;
  buf.base = raft_malloc(buf.len);
  snprintf(buf.base, buf.len, "%s", value);

  struct raft_apply *req = raft_malloc(sizeof(*req));
  req->data = r;
  int ret = raft_apply(r, req, &buf, 1, putValueCb);
  if (ret == 0) {
    printf("put %s \n", (char *)buf.base);
  } else {
    printf("put error: %s \n", raft_errmsg(r));
  }
}

const char *state2String(unsigned short state) {
  if (state == RAFT_UNAVAILABLE) {
    return "RAFT_UNAVAILABLE";

  } else if (state == RAFT_FOLLOWER) {
    return "RAFT_FOLLOWER";

  } else if (state == RAFT_CANDIDATE) {
    return "RAFT_CANDIDATE";

  } else if (state == RAFT_LEADER) {
    return "RAFT_LEADER";
  }
  return "UNKNOWN_RAFT_STATE";
}

void printRaftConfiguration(struct raft_configuration *c) {
  printf("configuration: \n");
  for (int i = 0; i < c->n; ++i) {
    printf("%llu -- %d -- %s\n", c->servers[i].id, c->servers[i].role, c->servers[i].address);
  }
}

void printRaftState(struct raft *r) {
  printf("----Raft State: -----------\n");
  printf("mem_addr: %p \n", r);
  printf("my_id: %llu \n", r->id);
  printf("address: %s \n", r->address);
  printf("current_term: %llu \n", r->current_term);
  printf("voted_for: %llu \n", r->voted_for);
  printf("role: %s \n", state2String(r->state));
  printf("commit_index: %llu \n", r->commit_index);
  printf("last_applied: %llu \n", r->last_applied);
  printf("last_stored: %llu \n", r->last_stored);

  printf("configuration_index: %llu \n", r->configuration_index);
  printf("configuration_uncommitted_index: %llu \n", r->configuration_uncommitted_index);
  printRaftConfiguration(&r->configuration);

  printf("----------------------------\n");
}

int32_t addRaftVoter(RaftServer *pRaftServer, char peers[][ADDRESS_LEN], uint32_t peersCount, uint16_t vid) {
  int ret;

  RaftInstance *pRaftInstance = malloc(sizeof(*pRaftInstance));
  assert(pRaftInstance != NULL);

  // init raftId
  pRaftInstance->raftId = encodeRaftId(pRaftServer->host, pRaftServer->port, vid);

  // init dir
  snprintf(pRaftInstance->dir, sizeof(pRaftInstance->dir), "%s/%s_%hu_%hu_%llu", pRaftServer->baseDir,
           pRaftServer->host, pRaftServer->port, vid, pRaftInstance->raftId);

  if (!dirOK(pRaftInstance->dir)) {
    ret = mkdir(pRaftInstance->dir, 0775);
    if (ret != 0) {
      fprintf(stderr, "mkdir error, %s \n", pRaftInstance->dir);
      assert(0);
    }
  }

  // init fsm
  pRaftInstance->fsm.data = newSimpleHash(2);
  pRaftInstance->fsm.apply = fsmApplyCb;

  // init io
  ret = raft_uv_init(&pRaftInstance->io, &pRaftServer->loop, pRaftInstance->dir, &pRaftServer->transport);
  if (ret != 0) {
    fprintf(stderr, "raft_uv_init error, %s \n", raft_errmsg(&pRaftInstance->raft));
    assert(0);
  }

  // init raft
  ret = raft_init(&pRaftInstance->raft, &pRaftInstance->io, &pRaftInstance->fsm, pRaftInstance->raftId,
                  pRaftServer->address);
  if (ret != 0) {
    fprintf(stderr, "raft_init error, %s \n", raft_errmsg(&pRaftInstance->raft));
    assert(0);
  }

  // init raft_configuration
  struct raft_configuration conf;
  raft_configuration_init(&conf);
  raft_configuration_add(&conf, pRaftInstance->raftId, pRaftServer->address, RAFT_VOTER);
  for (int i = 0; i < peersCount; ++i) {
    char *   peerAddress = peers[i];
    char     host[64];
    uint16_t port;
    parseAddr(peerAddress, host, sizeof(host), &port);
    uint64_t raftId = encodeRaftId(host, port, vid);
    raft_configuration_add(&conf, raftId, peers[i], RAFT_VOTER);
  }
  raft_bootstrap(&pRaftInstance->raft, &conf);

  // start raft
  ret = raft_start(&pRaftInstance->raft);
  if (ret != 0) {
    fprintf(stderr, "raft_start error, %s \n", raft_errmsg(&pRaftInstance->raft));
    assert(0);
  }

  // add raft instance into raft server
  pRaftServer->raftInstances.insert(&pRaftServer->raftInstances, vid, pRaftInstance);

  return 0;
}

int32_t addRaftSpare(RaftServer *pRaftServer, uint16_t vid) {
  int ret;

  RaftInstance *pRaftInstance = malloc(sizeof(*pRaftInstance));
  assert(pRaftInstance != NULL);

  // init raftId
  pRaftInstance->raftId = encodeRaftId(pRaftServer->host, pRaftServer->port, vid);

  // init dir
  snprintf(pRaftInstance->dir, sizeof(pRaftInstance->dir), "%s/%s_%hu_%hu_%llu", pRaftServer->baseDir,
           pRaftServer->host, pRaftServer->port, vid, pRaftInstance->raftId);
  ret = mkdir(pRaftInstance->dir, 0775);
  if (ret != 0) {
    fprintf(stderr, "mkdir error, %s \n", pRaftInstance->dir);
    assert(0);
  }

  // init fsm
  pRaftInstance->fsm.data = newSimpleHash(2);
  pRaftInstance->fsm.apply = fsmApplyCb;

  // init io
  ret = raft_uv_init(&pRaftInstance->io, &pRaftServer->loop, pRaftInstance->dir, &pRaftServer->transport);
  if (ret != 0) {
    fprintf(stderr, "raft_uv_init error, %s \n", raft_errmsg(&pRaftInstance->raft));
    assert(0);
  }

  // init raft
  ret = raft_init(&pRaftInstance->raft, &pRaftInstance->io, &pRaftInstance->fsm, pRaftInstance->raftId,
                  pRaftServer->address);
  if (ret != 0) {
    fprintf(stderr, "raft_init error, %s \n", raft_errmsg(&pRaftInstance->raft));
    assert(0);
  }

  // init raft_configuration
  struct raft_configuration conf;
  raft_configuration_init(&conf);
  raft_configuration_add(&conf, pRaftInstance->raftId, pRaftServer->address, RAFT_SPARE);
  raft_bootstrap(&pRaftInstance->raft, &conf);

  // start raft
  ret = raft_start(&pRaftInstance->raft);
  if (ret != 0) {
    fprintf(stderr, "raft_start error, %s \n", raft_errmsg(&pRaftInstance->raft));
    assert(0);
  }

  // add raft instance into raft server
  pRaftServer->raftInstances.insert(&pRaftServer->raftInstances, vid, pRaftInstance);

  return 0;
}

int32_t raftServerInit(RaftServer *pRaftServer, const RaftServerConfig *pConf) {
  int ret;

  // init host, port, address, dir
  snprintf(pRaftServer->host, sizeof(pRaftServer->host), "%s", pConf->me.host);
  pRaftServer->port = pConf->me.port;
  snprintf(pRaftServer->address, sizeof(pRaftServer->address), "%s:%u", pRaftServer->host, pRaftServer->port);
  snprintf(pRaftServer->baseDir, sizeof(pRaftServer->baseDir), "%s", pConf->baseDir);

  // init loop
  ret = uv_loop_init(&pRaftServer->loop);
  if (ret != 0) {
    fprintf(stderr, "uv_loop_init error: %s \n", uv_strerror(ret));
    assert(0);
  }

  // init network
  ret = raft_uv_tcp_init(&pRaftServer->transport, &pRaftServer->loop);
  if (ret != 0) {
    fprintf(stderr, "raft_uv_tcp_init: error %d \n", ret);
    assert(0);
  }

  // init raft instance container
  initIdHash(&pRaftServer->raftInstances, 2);

  return 0;
}

int32_t raftServerStart(RaftServer *pRaftServer) {
  // start loop
  uv_run(&pRaftServer->loop, UV_RUN_DEFAULT);
  return 0;
}

void raftServerStop(RaftServer *pRaftServer) {}
