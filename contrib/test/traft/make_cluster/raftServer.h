#ifndef TDENGINE_RAFT_SERVER_H
#define TDENGINE_RAFT_SERVER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <arpa/inet.h>
#include <assert.h>
#include <netinet/in.h>
#include <string.h>
#include "common.h"
#include "config.h"
#include "raft.h"
#include "raft/uv.h"
#include "simpleHash.h"

typedef struct RaftJoin {
  struct raft *r;
  raft_id      joinId;
} RaftJoin;

typedef struct {
  raft_id         raftId;
  char            dir[BASE_DIR_LEN * 2];
  struct raft_fsm fsm;
  struct raft_io  io;
  struct raft     raft;
} RaftInstance;

typedef struct {
  char     host[HOST_LEN];
  uint16_t port;
  char     address[ADDRESS_LEN];  /* Raft instance address */
  char     baseDir[BASE_DIR_LEN]; /* Raft instance address */

  struct uv_loop_s         loop;      /* UV loop */
  struct raft_uv_transport transport; /* UV I/O backend transport */

  IdHash raftInstances; /* multi raft instances. traft use IdHash to manager multi vgroup inside, here we can use IdHash
                           too. */
} RaftServer;

void *  startServerFunc(void *param);
int32_t addRaftVoter(RaftServer *pRaftServer, char peers[][ADDRESS_LEN], uint32_t peersCount, uint16_t vid);
int32_t addRaftSpare(RaftServer *pRaftServer, uint16_t vid);

int32_t raftServerInit(RaftServer *pRaftServer, const RaftServerConfig *pConf);
int32_t raftServerStart(RaftServer *pRaftServer);
void    raftServerStop(RaftServer *pRaftServer);

int  fsmApplyCb(struct raft_fsm *pFsm, const struct raft_buffer *buf, void **result);
void putValueCb(struct raft_apply *req, int status, void *result);
void putValue(struct raft *r, const char *value);

void raftChangeAddCb(struct raft_change *req, int status);

const char *state2String(unsigned short state);
void        printRaftConfiguration(struct raft_configuration *c);
void        printRaftState(struct raft *r);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_RAFT_SERVER_H
