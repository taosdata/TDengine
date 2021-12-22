#ifndef TDENGINE_RAFT_SERVER_H
#define TDENGINE_RAFT_SERVER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "raft.h"
#include "raft/uv.h"



#define DIR_LEN 128
#define ADDRESS_LEN 128
typedef struct {
	char dir[DIR_LEN];                 /* Data dir of UV I/O backend */
	char address[ADDRESS_LEN];         /* Raft instance address */
	raft_id raftId;                    /* For vote */
	struct raft_fsm *fsm;              /* Sample application FSM */

	struct raft raft;                   /* Raft instance */
	struct raft_io io;                  /* UV I/O backend */
	struct uv_loop_s loop;              /* UV loop */
	struct raft_uv_transport transport; /* UV I/O backend transport */
} SRaftServer;

struct SRaftServerConfig;
int32_t raftServerInit(SRaftServer *pRaftServer, struct SRaftServerConfig *pConf, struct raft_fsm *pFsm);
int32_t raftServerStart(SRaftServer *pRaftServer);
void raftServerClose(SRaftServer *pRaftServer);


int initFsm(struct raft_fsm *fsm);




#ifdef __cplusplus
}
#endif

#endif // TDENGINE_RAFT_SERVER_H
