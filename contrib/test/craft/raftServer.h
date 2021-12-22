#ifndef TDENGINE_RAFT_SERVER_H
#define TDENGINE_RAFT_SERVER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <string.h>
#include "raft.h"
#include "raft/uv.h"

#define DIR_LEN 128
#define HOST_LEN 128
#define ADDRESS_LEN (HOST_LEN + 16)
typedef struct {
	char dir[DIR_LEN];                 /* Data dir of UV I/O backend */
	char host[HOST_LEN];
    uint32_t port;
	char address[ADDRESS_LEN];         /* Raft instance address */
	raft_id raftId;                    /* For vote */
	struct raft_fsm *fsm;              /* Sample application FSM */

	struct raft raft;                   /* Raft instance */
	struct raft_io io;                  /* UV I/O backend */
	struct uv_loop_s loop;              /* UV loop */
	struct raft_uv_transport transport; /* UV I/O backend transport */
} SRaftServer;

int32_t raftServerInit(SRaftServer *pRaftServer, const char *host, uint32_t port, const char *dir, struct raft_fsm *pFsm);
int32_t raftServerStart(SRaftServer *pRaftServer);
void raftServerClose(SRaftServer *pRaftServer);


int initFsm(struct raft_fsm *fsm);




#ifdef __cplusplus
}
#endif

#endif // TDENGINE_RAFT_SERVER_H
